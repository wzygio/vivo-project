"""Load and validate AOI_TT station-level Particle Size ratio specifications."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.shared_kernel.utils.excel_tools import read_workbook_sheet

RATIO_SPEC_SHEET_NAME = "比例规格表"
RATIO_SPEC_COLUMN = "分配比例"
RATIO_SPEC_COLUMNS = ["step_id", "particle_size", "ratio"]
PARTICLE_SIZES = ("S", "M", "L", "H")


def _normalize_step_id(value: object) -> str:
    text = str(value).strip()
    try:
        number = float(text)
    except ValueError:
        return text
    return str(int(number)) if number.is_integer() else text


def load_particle_size_ratios(path: Path) -> pd.DataFrame:
    """Read the configured ratio sheet and return one valid distribution per station."""
    frame = read_workbook_sheet(Path(path), RATIO_SPEC_SHEET_NAME)
    required = {"step_id", "particle_size", RATIO_SPEC_COLUMN}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"AOI_TT 比例规格表缺少字段: {sorted(missing)}")

    ratios = frame[["step_id", "particle_size", RATIO_SPEC_COLUMN]].copy()
    ratios["step_id"] = ratios["step_id"].map(_normalize_step_id)
    ratios["particle_size"] = ratios["particle_size"].astype(str).str.strip().str.upper()
    ratios["ratio"] = pd.to_numeric(ratios[RATIO_SPEC_COLUMN], errors="coerce")
    ratios = ratios.drop(columns=[RATIO_SPEC_COLUMN]).dropna(subset=["ratio"])
    ratios = ratios[ratios["particle_size"].isin(PARTICLE_SIZES)]
    if ratios.duplicated(["step_id", "particle_size"]).any():
        raise ValueError("AOI_TT 比例规格表存在重复的 step_id + particle_size")

    for step_id, group in ratios.groupby("step_id", sort=False):
        if set(group["particle_size"]) != set(PARTICLE_SIZES):
            raise ValueError(f"站点 {step_id} 必须完整配置 S/M/L/H")
        if not group["ratio"].gt(0).all() or abs(float(group["ratio"].sum()) - 1.0) > 1e-6:
            raise ValueError(f"站点 {step_id} 的 S/M/L/H 分配比例必须为正且合计为 1")

    order = {size: index for index, size in enumerate(PARTICLE_SIZES)}
    ratios["_size_order"] = ratios["particle_size"].map(order)
    return (
        ratios.sort_values(["step_id", "_size_order"], kind="stable")
        .drop(columns=["_size_order"])
        .reset_index(drop=True)
        .reindex(columns=RATIO_SPEC_COLUMNS)
    )
