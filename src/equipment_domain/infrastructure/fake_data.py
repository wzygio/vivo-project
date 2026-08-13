"""Deterministic current-value dataset fabrication for critical parts."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Any

import numpy as np
import pandas as pd

from src.equipment_domain.core.parts_matcher import _compile_like_pattern
from src.equipment_domain.core.parts_identity import build_fabricated_param_name


SNAPSHOT_COLUMNS = [
    "step_id",
    "sub_equip_id",
    "param_name",
    "value",
    "glass_start_time",
]


@dataclass(frozen=True)
class FabricationPolicy:
    """Validated generation and update rules for fabricated measurements."""

    random_seed: int
    initial_value_ratio_range: tuple[float, float]
    initial_lookback_days: int
    update_increment_ratio: float
    reset_ratio_range: tuple[float, float]
    snapshot_ttl_hours: int

    def __post_init__(self) -> None:
        ranges = (self.initial_value_ratio_range, self.reset_ratio_range)
        if any(len(value_range) != 2 for value_range in ranges):
            raise ValueError("fabrication ratio ranges must contain two values")
        if any(low < 0 or low > high or high > 1 for low, high in ranges):
            raise ValueError("fabrication ratio ranges must be ordered within [0, 1]")
        if self.initial_lookback_days <= 0:
            raise ValueError("initial lookback days must be positive")
        if self.update_increment_ratio <= 0:
            raise ValueError("update increment ratio must be positive")
        if self.snapshot_ttl_hours <= 0:
            raise ValueError("fabricated snapshot TTL must be positive")


@dataclass(frozen=True)
class FabricationResult:
    """Fabricated snapshot and its auditable generation summary."""

    snapshot_df: pd.DataFrame
    summary: dict[str, Any]


def materialize_param_name(like_pattern: str, machine_chamber: str) -> str:
    """Materialize one of the baseline's known SQL LIKE parameter patterns."""
    pattern = str(like_pattern).strip()
    chamber_match = re.search(r"PM(\d+)", str(machine_chamber), flags=re.IGNORECASE)
    if chamber_match is None:
        raise ValueError(f"machine chamber has no PM identifier: {machine_chamber!r}")
    chamber_number = chamber_match.group(1)

    upper_pattern = pattern.upper()
    if "TRGTLIFE" in upper_pattern:
        param_name = f"P{chamber_number}_TRGTLIFE_G_MAX"
    elif "MASKLIFE" in upper_pattern:
        param_name = f"P{chamber_number}_MASKLIFE_G_MAX"
    elif "PRE_SPRT_KWH" in upper_pattern:
        param_name = f"PM{chamber_number}_1_PRE_SPRT_KWH"
    else:
        raise ValueError(f"unsupported parameter LIKE pattern: {like_pattern!r}")

    if _compile_like_pattern(pattern).fullmatch(param_name) is None:
        raise ValueError(
            f"materialized parameter {param_name!r} does not match {like_pattern!r}"
        )
    return param_name


def stable_unit_fraction(*parts: object, seed: int) -> float:
    """Map stable business inputs to a reproducible fraction in ``[0, 1)``."""
    payload = "\x1f".join([str(seed), *(str(part) for part in parts)])
    digest = hashlib.sha256(payload.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big") / 2**64


def generate_fabricated_snapshot(
    spec_df: pd.DataFrame,
    policy: FabricationPolicy,
    *,
    as_of: pd.Timestamp,
) -> FabricationResult:
    """Generate an initial record for every unique monitorable specification key."""
    required = {"站点", "机台号-腔室", "参数名称", "寿命规格"}
    missing = sorted(required.difference(spec_df.columns))
    if missing:
        raise ValueError(f"missing fabrication specification columns: {missing}")

    timestamp = pd.Timestamp(as_of)
    if pd.isna(timestamp):
        raise ValueError("as_of must be a valid timestamp")
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_localize(None)

    work = spec_df.copy()
    work["参数名称"] = work["参数名称"].fillna("").astype(str).str.strip()
    work["寿命规格"] = pd.to_numeric(work["寿命规格"], errors="coerce")
    blank_param_mask = work["参数名称"].eq("")
    valid_spec_mask = work["寿命规格"].gt(0) & np.isfinite(work["寿命规格"])
    valid_identity_mask = (
        work["站点"].fillna("").astype(str).str.strip().ne("")
        & work["机台号-腔室"].fillna("").astype(str).str.strip().ne("")
    )
    monitorable_mask = valid_spec_mask & valid_identity_mask
    monitorable = work[monitorable_mask].copy()
    monitorable["_resolved_param_name"] = monitorable.apply(
        lambda row: (
            build_fabricated_param_name(row)
            if not row["参数名称"]
            else materialize_param_name(row["参数名称"], row["机台号-腔室"])
        ),
        axis=1,
    )

    key_columns = ["站点", "机台号-腔室", "_resolved_param_name"]
    conflict_counts = monitorable.groupby(key_columns, dropna=False)["寿命规格"].nunique()
    conflicting_keys = conflict_counts[conflict_counts > 1]
    if not conflicting_keys.empty:
        raise ValueError(
            "conflicting life specifications for one bottom key: "
            f"{list(conflicting_keys.index[:5])}"
        )

    unique_specs = monitorable.drop_duplicates(key_columns, keep="first")
    unique_specs = unique_specs.sort_values(key_columns, kind="mergesort").reset_index(drop=True)
    records: list[dict[str, Any]] = []
    lookback_seconds = policy.initial_lookback_days * 24 * 60 * 60
    for _, spec_row in unique_specs.iterrows():
        key = tuple(str(spec_row[column]).strip() for column in key_columns)
        low, high = policy.initial_value_ratio_range
        value_ratio = low + (high - low) * stable_unit_fraction(
            "initial-value",
            *key,
            seed=policy.random_seed,
        )
        value = float(spec_row["寿命规格"]) * value_ratio
        age_seconds = int(
            stable_unit_fraction(
                "initial-time",
                *key,
                seed=policy.random_seed,
            )
            * (lookback_seconds + 1)
        )
        records.append(
            {
                "step_id": str(spec_row["站点"]).strip(),
                "sub_equip_id": str(spec_row["机台号-腔室"]).strip(),
                "param_name": str(spec_row["_resolved_param_name"]),
                "value": value,
                "glass_start_time": timestamp - pd.Timedelta(seconds=age_seconds),
            }
        )

    snapshot_df = pd.DataFrame.from_records(records, columns=SNAPSHOT_COLUMNS)
    if snapshot_df.empty:
        snapshot_df = pd.DataFrame(
            {
                "step_id": pd.Series(dtype=object),
                "sub_equip_id": pd.Series(dtype=object),
                "param_name": pd.Series(dtype=object),
                "value": pd.Series(dtype=float),
                "glass_start_time": pd.Series(dtype="datetime64[ns]"),
            }
        )
    else:
        snapshot_df["value"] = snapshot_df["value"].astype(float)
        snapshot_df["glass_start_time"] = pd.to_datetime(snapshot_df["glass_start_time"])

    summary: dict[str, Any] = {
        "source_rows": int(len(spec_df)),
        "monitorable_rows": int(monitorable_mask.sum()),
        "generated_rows": int(len(snapshot_df)),
        "synthetic_param_rows": int((blank_param_mask & monitorable_mask).sum()),
        "skipped_blank_param_rows": 0,
        "skipped_invalid_spec_rows": int((~valid_spec_mask).sum()),
        "skipped_invalid_identity_rows": int((valid_spec_mask & ~valid_identity_mask).sum()),
        "random_seed": int(policy.random_seed),
        "generation_mode": "stable-key-phase",
        "as_of": str(timestamp),
    }
    return FabricationResult(snapshot_df=snapshot_df, summary=summary)


def fabricate_current_snapshot(
    spec_df: pd.DataFrame,
    policy: FabricationPolicy,
    *,
    as_of: pd.Timestamp,
) -> FabricationResult:
    """Backward-compatible alias for initial fabricated snapshot generation."""
    return generate_fabricated_snapshot(spec_df, policy, as_of=as_of)


def calculate_spec_signature(spec_df: pd.DataFrame) -> str:
    """Return the same content signature used by the production snapshot loader."""
    return hashlib.md5(
        pd.util.hash_pandas_object(spec_df, index=True).values.tobytes()
    ).hexdigest()[:12]


def build_fabricated_snapshot_path(
    spec_df: pd.DataFrame,
    output_dir: str | Path,
) -> Path:
    """Return the isolated path for one specification's fabricated snapshot."""
    return Path(output_dir) / (
        f"part_life_fabricated_{calculate_spec_signature(spec_df)}.parquet"
    )


def write_fabricated_snapshot(
    snapshot_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    *,
    output_dir: str | Path,
    overwrite: bool = False,
) -> Path:
    """Write a production-named Parquet snapshot without silent replacement."""
    missing = [column for column in SNAPSHOT_COLUMNS if column not in snapshot_df.columns]
    if missing:
        raise ValueError(f"missing fabricated snapshot columns: {missing}")
    output_path = build_fabricated_snapshot_path(spec_df, output_dir)
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"fabricated snapshot already exists: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_df.loc[:, SNAPSHOT_COLUMNS].to_parquet(output_path, index=False)
    return output_path
