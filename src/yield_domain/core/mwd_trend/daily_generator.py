"""指定良损驱动的日度不良数生成器。

算法（逐不良类型、逐月闭环）：

1. 月度目标量 ``target = round(目标良损 × 当月投入总数)``；
2. 跨月平滑基线：各月目标良损锚定在月中（15 日），相邻锚点线性插值，
   两端按最近锚点平延 —— 跨月过渡无阶梯；
3. 确定性扰动：``noise = 1 + volatility × (2u − 1)``，其中
   ``u = blake2b("{product}|{defect}|{date}") / 2^64`` —— 无周期性震荡，
   且同输入多次运行结果完全一致（不依赖内置 ``hash()``）；
4. 日度权重 ``w = base × noise × total_panels``，月内按权重把目标量分配到日，
   单日上限为当日投入。

所有 Code、所有分析月份都使用月度目标。修饰表未提供目标时，回退到从 Panel 明细
按月汇总得到的原始月度良损；不回落原始日度不良数。
"""

from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

MID_MONTH_DAY = 15


def allocate_integer_counts(
    weights: np.ndarray,
    capacities: np.ndarray,
    target_total: int,
) -> np.ndarray:
    """按权重分配整数目标，且不超过各行容量。"""
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
            active_weights = np.where(
                active,
                safe_capacities - exact_allocations,
                0.0,
            )

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


def _hash_unit_interval(product_code: str, defect: str, date: pd.Timestamp) -> float:
    """稳定的 [0,1) 哈希值：同输入永远同输出，跨进程一致。"""
    token = f"{product_code}|{defect}|{date.strftime('%Y-%m-%d')}".encode("utf-8")
    digest = hashlib.blake2b(token, digest_size=8).digest()
    return int.from_bytes(digest, "big") / 2**64


def interpolated_base_rates(
    dates: pd.Series,
    month_rates: dict[str, float],
) -> np.ndarray:
    """把"月 → 目标良损"按月中锚点线性插值为逐日基线率。"""
    anchors = []
    for month in sorted(month_rates):
        year, mon = int(month[:4]), int(month[5:7])
        anchors.append((pd.Timestamp(year, mon, MID_MONTH_DAY), month_rates[month]))
    if not anchors:
        return np.zeros(len(dates), dtype=float)

    anchor_times = np.array([a[0].value for a in anchors], dtype=float)
    anchor_rates = np.array([a[1] for a in anchors], dtype=float)
    date_values = pd.to_datetime(dates).astype("int64").to_numpy(dtype=float)
    # np.interp 两端自动按最近锚点平延
    return np.interp(date_values, anchor_times, anchor_rates)


def _generate_defect_daily(
    defect_df: pd.DataFrame,
    month_rates: dict[str, float],
    product_code: str,
    defect: str,
    volatility: float,
) -> pd.DataFrame:
    """对单个缺陷生成日度不良数（逐月闭环整数分配）。"""
    result = defect_df.copy()
    result["defect_panel_count"] = 0
    result["warehousing_time"] = pd.to_datetime(result["warehousing_time"])
    result["_month"] = result["warehousing_time"].dt.strftime("%Y-%m")

    base_rates = interpolated_base_rates(result["warehousing_time"], month_rates)
    noises = np.array(
        [
            1.0
            + volatility
            * (2.0 * _hash_unit_interval(product_code, defect, date) - 1.0)
            for date in result["warehousing_time"]
        ]
    )
    result["_weight"] = base_rates * noises * result["total_panels"].to_numpy(float)

    for month, month_rows in result.groupby("_month", sort=False):
        rate = month_rates[month]
        month_panels = float(month_rows["total_panels"].sum())
        target_total = int(round(rate * month_panels))
        allocated = allocate_integer_counts(
            month_rows["_weight"].to_numpy(dtype=float),
            month_rows["total_panels"].to_numpy(dtype=float),
            target_total,
        )
        result.loc[month_rows.index, "defect_panel_count"] = allocated

    result["defect_panel_count"] = result["defect_panel_count"].astype(int)
    return result.drop(columns=["_month", "_weight"])


def generate_daily_counts(
    padded_daily: pd.DataFrame,
    monthly_targets: dict[str, dict[str, float]],
    product_code: str,
    volatility: float = 0.3,
    raw_monthly_targets: dict[str, dict[str, float]] | None = None,
) -> pd.DataFrame:
    """按指定月度良损生成 Code 级日度不良数。

    Args:
        padded_daily: `data_preparation.pad_daily_data_to_end` 输出的容量长表
            （warehousing_time/total_panels/defect_group/defect_desc）。
        monthly_targets: {defect_desc: {月份(YYYY-MM): 目标良损}}，
            通常由 `modifier_table.resolve_monthly_targets` 产出。
        product_code: 产品型号（参与哈希，保证产品间序列独立）。
        volatility: 日度形状波动幅度（仅影响日度分布，不影响月度合计）。
        raw_monthly_targets: 从 Panel 明细按月汇总的原始 Code 良损；用于补齐
            `monthly_targets` 未覆盖的 Code/月。

    Returns:
        带有生成后 `defect_panel_count` 的日度表。

    Raises:
        ValueError: 修饰目标与原始月度良损均无法覆盖某个 Code/月。
    """
    if padded_daily.empty:
        return padded_daily.copy()

    effective_targets = _merge_monthly_targets(
        monthly_targets,
        raw_monthly_targets or {},
    )
    working = padded_daily.copy()
    working["warehousing_time"] = pd.to_datetime(working["warehousing_time"])
    required_targets = (
        working.assign(_month=working["warehousing_time"].dt.strftime("%Y-%m"))[
            ["defect_desc", "_month"]
        ]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    missing = sorted(
        (str(defect), str(month))
        for defect, month in required_targets
        if month not in effective_targets.get(defect, {})
    )
    if missing:
        missing_text = ", ".join(
            f"{defect}/{month}" for defect, month in missing
        )
        raise ValueError(f"月度良损目标未覆盖全部 Code/月: {missing_text}")

    pieces = []
    for (group, defect), defect_df in working.groupby(
        ["defect_group", "defect_desc"], sort=False
    ):
        month_rates = effective_targets[defect]
        pieces.append(
            _generate_defect_daily(
                defect_df, month_rates, product_code, defect, volatility
            )
        )
    return pd.concat(pieces).sort_index()


def _merge_monthly_targets(
    monthly_targets: dict[str, dict[str, float]],
    raw_monthly_targets: dict[str, dict[str, float]],
) -> dict[str, dict[str, float]]:
    """Fill missing modifier targets from raw monthly rates."""
    merged = {
        defect: dict(month_rates)
        for defect, month_rates in raw_monthly_targets.items()
    }
    for defect, month_rates in (monthly_targets or {}).items():
        merged.setdefault(defect, {}).update(month_rates)
    return merged
