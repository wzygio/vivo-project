"""TTL-driven maintenance for fabricated critical-parts snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import tempfile
from typing import Any

import numpy as np
import pandas as pd

from src.equipment_domain.core.parts_identity import build_fabricated_param_name
from src.equipment_domain.infrastructure.fake_data import (
    FabricationPolicy,
    SNAPSHOT_COLUMNS,
    build_fabricated_snapshot_path,
    generate_fabricated_snapshot,
    materialize_param_name,
    stable_unit_fraction,
)


@dataclass(frozen=True)
class FabricatedUpdateResult:
    """Updated fabricated snapshot and an auditable summary."""

    snapshot_df: pd.DataFrame
    summary: dict[str, Any]


@dataclass(frozen=True)
class FabricatedFileUpdateOutcome:
    """Result of applying the TTL maintenance policy to one snapshot file."""

    path: Path
    updated: bool
    summary: dict[str, Any]
    created: bool = False


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
    *,
    periods: int = 1,
) -> FabricatedUpdateResult:
    """Advance a snapshot through deterministic, near-arithmetic daily steps."""
    if isinstance(periods, bool) or not isinstance(periods, int) or periods <= 0:
        raise ValueError("update periods must be a positive integer")
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

    reset_rows = 0
    current_values = values.astype(float).to_numpy(copy=True)
    current_times = pd.Series(times, index=updated.index)
    for _ in range(periods):
        next_times = current_times + pd.Timedelta(days=1)
        next_values: list[float] = []
        for key, current_value, next_time in zip(
            keys,
            current_values,
            next_times,
            strict=True,
        ):
            lifetime = lifetime_by_key[key]
            variation = 0.95 + 0.10 * stable_unit_fraction(
                "daily-increment",
                *key,
                pd.Timestamp(next_time).isoformat(),
                seed=policy.random_seed,
            )
            increment = lifetime * policy.update_increment_ratio * variation
            candidate = float(current_value) + increment
            if candidate >= lifetime:
                reset_low, reset_high = policy.reset_ratio_range
                candidate = candidate % lifetime
                candidate = float(
                    np.clip(candidate, lifetime * reset_low, lifetime * reset_high)
                )
                reset_rows += 1
            next_values.append(candidate)
        current_values = np.asarray(next_values, dtype=float)
        current_times = next_times

    updated["value"] = pd.Series(current_values, index=updated.index, dtype=float)
    updated["glass_start_time"] = current_times
    return FabricatedUpdateResult(
        snapshot_df=updated,
        summary={
            "updated_rows": int(len(updated)),
            "reset_rows": int(reset_rows),
            "random_seed": int(policy.random_seed),
            "update_periods": periods,
            "update_mode": "deterministic-near-arithmetic",
        },
    )


def _normalize_timestamp(value: pd.Timestamp, *, name: str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError(f"{name} must be a valid timestamp")
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_localize(None)
    return timestamp


def _write_snapshot_atomically(snapshot_df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f".{path.stem}-",
        suffix=path.suffix,
        dir=path.parent,
        delete=False,
    ) as temporary_file:
        temporary_path = Path(temporary_file.name)
    try:
        snapshot_df.loc[:, SNAPSHOT_COLUMNS].to_parquet(temporary_path, index=False)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


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

    current_time = _normalize_timestamp(now, name="now")
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
                "update_periods": 0,
            },
        )

    existing = pd.read_parquet(snapshot_path)
    update_periods = (
        1
        if force
        else max(1, int(age_hours // policy.snapshot_ttl_hours))
    )
    result = update_fabricated_snapshot(
        existing,
        spec_df,
        policy,
        periods=update_periods,
    )
    _write_snapshot_atomically(result.snapshot_df, snapshot_path)
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


def ensure_fabricated_snapshot_file(
    spec_df: pd.DataFrame,
    policy: FabricationPolicy,
    *,
    output_dir: str | Path,
    now: pd.Timestamp,
) -> FabricatedFileUpdateOutcome:
    """Create a missing snapshot or advance an expired snapshot automatically."""
    current_time = _normalize_timestamp(now, name="now")
    snapshot_path = build_fabricated_snapshot_path(spec_df, output_dir)
    if snapshot_path.exists():
        return update_fabricated_snapshot_file(
            spec_df,
            policy,
            output_dir=output_dir,
            now=current_time,
        )

    generated = generate_fabricated_snapshot(spec_df, policy, as_of=current_time)
    _write_snapshot_atomically(generated.snapshot_df, snapshot_path)
    return FabricatedFileUpdateOutcome(
        path=snapshot_path,
        created=True,
        updated=False,
        summary={
            **generated.summary,
            "updated_rows": 0,
            "reset_rows": 0,
            "update_periods": 0,
            "age_hours": 0.0,
            "ttl_hours": int(policy.snapshot_ttl_hours),
            "reason": "snapshot-created",
        },
    )
