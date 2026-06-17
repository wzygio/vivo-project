import logging
import math
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def derive_lot_id(sheet_id: object) -> str:
    """Derive 9-character Lot ID from Sheet/Glass/Panel-like identifiers."""
    sheet_id_str = str(sheet_id).strip() if sheet_id is not None else ""
    if len(sheet_id_str) < 9:
        return ""
    return sheet_id_str[:9]


def calculate_cpm(
    mean_value: float,
    std_value: float,
    usl: float,
    lsl: float,
    target: Optional[float] = None,
) -> float:
    """Calculate Taguchi CPM for a two-sided specification."""
    values = [mean_value, std_value, usl, lsl]
    if any(pd.isna(value) for value in values):
        return float("nan")

    if usl <= lsl:
        return float("nan")

    resolved_target = target
    if resolved_target is None or pd.isna(resolved_target):
        resolved_target = (usl + lsl) / 2.0

    denominator = 6.0 * math.sqrt(float(std_value) ** 2 + (float(mean_value) - float(resolved_target)) ** 2)
    if denominator == 0:
        return float("inf")
    return float((usl - lsl) / denominator)


def calculate_cpk(mean_value: float, std_value: float, usl: float, lsl: float) -> float:
    """Calculate CPK from the nearest specification distance."""
    values = [mean_value, std_value, usl, lsl]
    if any(pd.isna(value) for value in values):
        return float("nan")

    if usl <= lsl or std_value < 0:
        return float("nan")

    nearest_distance = min(float(usl) - float(mean_value), float(mean_value) - float(lsl))
    denominator = 3.0 * float(std_value)
    if denominator == 0:
        if nearest_distance > 0:
            return float("inf")
        if nearest_distance == 0:
            return 0.0
        return float("-inf")
    return float(nearest_distance / denominator)


def build_lot_cpm_report(sheet_features: pd.DataFrame, min_sheet_count: int = 2) -> pd.DataFrame:
    """Aggregate Sheet-level SPC features into Lot-level CPM by monitoring indicator."""
    required_cols = {
        "prod_code",
        "factory",
        "sheet_id",
        "step_id",
        "param_name",
        "sheet_mean",
        "usl",
        "lsl",
    }
    missing = required_cols - set(sheet_features.columns)
    if missing:
        logger.warning("[CPM] sheet_features missing required columns: %s", sorted(missing))
        return pd.DataFrame()

    if sheet_features.empty:
        return pd.DataFrame()

    df = sheet_features.copy()
    df["lot_id"] = df["sheet_id"].apply(derive_lot_id)
    df = df[df["lot_id"] != ""].copy()
    if df.empty:
        return pd.DataFrame()

    if "target" not in df.columns:
        df["target"] = np.nan

    group_cols = ["prod_code", "factory", "lot_id", "step_id", "param_name"]
    records: list[dict[str, object]] = []

    for keys, group in df.groupby(group_cols, dropna=False, sort=True):
        valid = group.dropna(subset=["sheet_mean", "usl", "lsl"])
        if len(valid) < min_sheet_count:
            continue

        prod_code, factory, lot_id, step_id, param_name = keys
        lot_mean = float(valid["sheet_mean"].mean())
        lot_std = float(valid["sheet_mean"].std(ddof=1))
        usl = float(valid["usl"].iloc[0])
        lsl = float(valid["lsl"].iloc[0])
        target_value = valid["target"].dropna().iloc[0] if valid["target"].notna().any() else np.nan
        cpm = calculate_cpm(
            mean_value=lot_mean,
            std_value=lot_std,
            usl=usl,
            lsl=lsl,
            target=float(target_value) if pd.notna(target_value) else None,
        )
        cpk = calculate_cpk(
            mean_value=lot_mean,
            std_value=lot_std,
            usl=usl,
            lsl=lsl,
        )

        records.append(
            {
                "prod_code": prod_code,
                "factory": factory,
                "lot_id": lot_id,
                "step_id": step_id,
                "param_name": param_name,
                "sheet_count": int(valid["sheet_id"].nunique()),
                "lot_mean": lot_mean,
                "lot_std": lot_std,
                "usl": usl,
                "lsl": lsl,
                "target": float(target_value) if pd.notna(target_value) else (usl + lsl) / 2.0,
                "cpm": cpm,
                "cpk": cpk,
                "first_sheet_time": pd.to_datetime(valid["sheet_start_time"], errors="coerce").min()
                if "sheet_start_time" in valid.columns
                else pd.NaT,
                "last_sheet_time": pd.to_datetime(valid["sheet_start_time"], errors="coerce").max()
                if "sheet_start_time" in valid.columns
                else pd.NaT,
            }
        )

    result = pd.DataFrame(records)
    if result.empty:
        return result
    return result.sort_values(["step_id", "param_name", "last_sheet_time", "lot_id"]).reset_index(drop=True)
