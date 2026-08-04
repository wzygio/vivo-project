"""Integer allocation and count reconciliation for MWD trend data."""

from __future__ import annotations

import numpy as np
import pandas as pd


def allocate_integer_counts(
    weights: np.ndarray,
    capacities: np.ndarray,
    target_total: int,
) -> np.ndarray:
    """Allocate an integer total by weight without exceeding row capacities."""
    safe_capacities = np.floor(
        np.nan_to_num(capacities, nan=0.0, posinf=0.0, neginf=0.0)
    ).astype(int)
    safe_capacities = np.clip(safe_capacities, 0, None)
    effective_target = min(max(0, int(target_total)), int(safe_capacities.sum()))
    if effective_target == 0:
        return np.zeros(len(safe_capacities), dtype=int)

    safe_weights = np.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    safe_weights = np.clip(safe_weights.astype(float), 0.0, None)
    exact_allocations = np.zeros(len(safe_capacities), dtype=float)
    remaining_target = float(effective_target)
    active = safe_capacities > 0

    while remaining_target > 0 and active.any():
        active_weights = np.where(active, safe_weights, 0.0)
        if active_weights.sum() <= 0:
            active_weights = np.where(active, safe_capacities - exact_allocations, 0.0)

        shares = remaining_target * active_weights / active_weights.sum()
        remaining_capacity = safe_capacities - exact_allocations
        saturated = active & (shares >= remaining_capacity)
        if saturated.any():
            exact_allocations[saturated] += remaining_capacity[saturated]
            remaining_target -= float(remaining_capacity[saturated].sum())
            active[saturated] = False
            continue

        exact_allocations[active] += shares[active]
        remaining_target = 0.0

    allocated = np.floor(exact_allocations).astype(int)
    remainder = effective_target - int(allocated.sum())
    if remainder > 0:
        fractional = exact_allocations - allocated
        eligible = allocated < safe_capacities
        order = np.argsort(-fractional, kind="stable")
        for index in order:
            if remainder == 0:
                break
            if eligible[index]:
                allocated[index] += 1
                remainder -= 1

    return allocated


def reconcile_code_daily_counts(
    daily_df: pd.DataFrame,
    raw_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    """Reconcile Code daily integers to post-multiplier raw monthly totals."""
    if daily_df.empty:
        return daily_df.copy()

    result = daily_df.copy()
    result["warehousing_time"] = pd.to_datetime(result["warehousing_time"])
    result["_month"] = result["warehousing_time"].dt.to_period("M")

    raw = raw_daily_df.copy()
    raw["warehousing_time"] = pd.to_datetime(raw["warehousing_time"])
    raw["_month"] = raw["warehousing_time"].dt.to_period("M")
    targets = raw.groupby(
        ["defect_group", "defect_desc", "_month"],
        dropna=False,
    )["defect_panel_count"].sum()

    group_columns = ["defect_group", "defect_desc", "_month"]
    for group_key, group_rows in result.groupby(group_columns, sort=False):
        row_indices = group_rows.index
        target_total = max(0, int(targets.get(group_key, 0)))
        current_counts = pd.to_numeric(
            group_rows["defect_panel_count"], errors="coerce"
        ).fillna(0).clip(lower=0)
        current_total = float(current_counts.sum())
        capacities = pd.to_numeric(
            group_rows["total_panels"], errors="coerce"
        ).fillna(0).clip(lower=0).to_numpy(dtype=float)

        allocated = allocate_integer_counts(
            current_counts.to_numpy(dtype=float) if current_total > 0 else capacities,
            capacities,
            target_total,
        )
        result.loc[row_indices, "defect_panel_count"] = allocated

    result["defect_panel_count"] = result["defect_panel_count"].astype(int)
    return result.drop(columns=["_month"])
