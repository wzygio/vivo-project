"""Daily update logic for an existing fabricated critical-parts snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.equipment_domain.core.parts_identity import build_fabricated_param_name
from src.equipment_domain.infrastructure.fake_data import (
    FabricationPolicy,
    SNAPSHOT_COLUMNS,
    build_fabricated_snapshot_path,
    materialize_param_name,
)


@dataclass(frozen=True)
class FabricatedUpdateResult:
    """Updated fabricated snapshot and an auditable summary."""

    snapshot_df: pd.DataFrame
    summary: dict[str, Any]


@dataclass(frozen=True)
class FabricatedFileUpdateOutcome:
    """Result of applying the 24-hour update policy to one snapshot file."""

    path: Path
    updated: bool
    summary: dict[str, Any]


def _build_lifetime_by_key(spec_df: pd.DataFrame) -> dict[tuple[str, str, str], float]:
    required = {"站点", "机台号-腔室", "参数名称", "寿命规格"}
    missing = sorted(required.difference(spec_df.columns))
    if missing:
        raise ValueError(f"missing update specification columns: {missing}")

    lifetime_by_key: dict[tuple[str, str, str], float] = {}
    for _, spec_row in spec_df.iterrows():
        station = str(spec_row.get("站点", "")).strip()
        machine = str(spec_row.get("机台号-腔室", "")).strip()
        lifetime = pd.to_numeric(spec_row.get("寿命规格"), errors="coerce")
        if not station or not machine or pd.isna(lifetime) or float(lifetime) <= 0:
            continue
        raw_param = spec_row.get("参数名称")
        param_pattern = "" if raw_param is None or bool(pd.isna(raw_param)) else str(raw_param).strip()
        param_name = (
            materialize_param_name(param_pattern, machine)
            if param_pattern
            else build_fabricated_param_name(spec_row)
        )
        key = (station, machine, param_name)
        previous = lifetime_by_key.get(key)
        if previous is not None and not np.isclose(previous, float(lifetime)):
            raise ValueError(f"conflicting life specifications for fabricated key: {key}")
        lifetime_by_key[key] = float(lifetime)
    return lifetime_by_key


def update_fabricated_snapshot(
    snapshot_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    policy: FabricationPolicy,
) -> FabricatedUpdateResult:
    """Advance an existing fabricated snapshot by one business day."""
    missing = [column for column in SNAPSHOT_COLUMNS if column not in snapshot_df.columns]
    if missing:
        raise ValueError(f"missing fabricated snapshot columns: {missing}")
    if snapshot_df.empty:
        raise ValueError("fabricated snapshot is empty")

    updated = snapshot_df.loc[:, SNAPSHOT_COLUMNS].copy()
    keys = list(
        updated[["step_id", "sub_equip_id", "param_name"]].itertuples(
            index=False,
            name=None,
        )
    )
    if len(keys) != len(set(keys)):
        raise ValueError("fabricated snapshot contains duplicate keys")

    lifetime_by_key = _build_lifetime_by_key(spec_df)
    missing_keys = [key for key in keys if key not in lifetime_by_key]
    if missing_keys:
        raise ValueError(f"fabricated snapshot keys missing from specifications: {missing_keys[:5]}")

    values = pd.to_numeric(updated["value"], errors="coerce")
    times = pd.to_datetime(updated["glass_start_time"], errors="coerce")
    if values.isna().any() or not np.isfinite(values).all():
        raise ValueError("fabricated snapshot contains invalid values")
    if times.isna().any():
        raise ValueError("fabricated snapshot contains invalid measurement times")

    rng = np.random.default_rng(policy.random_seed)
    new_values: list[float] = []
    reset_rows = 0
    for key, current_value in zip(keys, values, strict=True):
        lifetime = lifetime_by_key[key]
        candidate = float(current_value) + lifetime * policy.update_increment_ratio
        if candidate > lifetime:
            low, high = policy.reset_ratio_range
            candidate = lifetime * float(rng.uniform(low, high))
            reset_rows += 1
        new_values.append(candidate)

    updated["value"] = pd.Series(new_values, index=updated.index, dtype=float)
    updated["glass_start_time"] = times + pd.Timedelta(days=1)
    return FabricatedUpdateResult(
        snapshot_df=updated,
        summary={
            "updated_rows": int(len(updated)),
            "reset_rows": int(reset_rows),
            "random_seed": int(policy.random_seed),
        },
    )


def update_fabricated_snapshot_file(
    spec_df: pd.DataFrame,
    policy: FabricationPolicy,
    *,
    output_dir: str | Path,
    now: pd.Timestamp,
    force: bool = False,
) -> FabricatedFileUpdateOutcome:
    """Update an expired fabricated snapshot; never create an initial dataset."""
    snapshot_path = build_fabricated_snapshot_path(spec_df, output_dir)
    if not snapshot_path.exists():
        raise FileNotFoundError(f"fabricated snapshot not found: {snapshot_path}")

    current_time = pd.Timestamp(now)
    if pd.isna(current_time):
        raise ValueError("now must be a valid timestamp")
    if current_time.tzinfo is not None:
        current_time = current_time.tz_localize(None)
    modified_time = datetime.fromtimestamp(snapshot_path.stat().st_mtime)
    age_hours = (
        current_time.to_pydatetime() - modified_time
    ).total_seconds() / 3600
    if not force and age_hours < policy.snapshot_ttl_hours:
        return FabricatedFileUpdateOutcome(
            path=snapshot_path,
            updated=False,
            summary={
                "updated_rows": 0,
                "reset_rows": 0,
                "age_hours": float(age_hours),
                "ttl_hours": int(policy.snapshot_ttl_hours),
                "reason": "snapshot-valid",
            },
        )

    existing = pd.read_parquet(snapshot_path)
    result = update_fabricated_snapshot(existing, spec_df, policy)
    result.snapshot_df.to_parquet(snapshot_path, index=False)
    return FabricatedFileUpdateOutcome(
        path=snapshot_path,
        updated=True,
        summary={
            **result.summary,
            "age_hours": float(age_hours),
            "ttl_hours": int(policy.snapshot_ttl_hours),
            "reason": "forced" if force else "snapshot-expired",
        },
    )
