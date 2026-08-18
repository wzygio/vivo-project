"""指定良损驱动的日度不良数生成器。

算法（逐不良类型、逐月闭环）：

1. 月度目标量 ``target = round(目标良损 × 当月投入总数)``；
2. 跨月平滑基线：各月目标良损锚定在月中（15 日），相邻锚点线性插值，
   两端按最近锚点平延 —— 跨月过渡无阶梯；
3. 确定性扰动：``noise = 1 + volatility × (2u − 1)``，其中
   ``u = blake2b("{product}|{defect}|{date}") / 2^64`` —— 无周期性震荡，
   且同输入多次运行结果完全一致（不依赖内置 ``hash()``）；
4. 日度权重 ``w = base × noise × total_panels``，月内按权重把目标量分配到日
   （复用 `allocation.allocate_integer_counts`，单日上限 = 当日投入）。

未出现在目标表中的缺陷保持原始日度不良数不变（回落原始数据语义）。
"""

from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

from yield_domain.core.mwd_trend.allocation import allocate_integer_counts

MID_MONTH_DAY = 15


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
        rate = month_rates.get(month)
        if rate is None:
            # 目标表未覆盖的月份：保持原始
            continue
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
) -> pd.DataFrame:
    """按指定月度良损生成 Code 级日度不良数。

    Args:
        padded_daily: `data_preparation.pad_daily_data_to_end` 输出的长表
            （warehousing_time/total_panels/defect_group/defect_desc/defect_panel_count）。
        monthly_targets: {defect_desc: {月份(YYYY-MM): 目标良损}}，
            通常由 `modifier_table.resolve_monthly_targets` 产出。
        product_code: 产品型号（参与哈希，保证产品间序列独立）。
        volatility: 日度形状波动幅度（仅影响日度分布，不影响月度合计）。

    Returns:
        与输入同构的日度表；未指定的缺陷保持原始不良数。
    """
    if padded_daily.empty:
        return padded_daily.copy()

    pieces = []
    for (group, defect), defect_df in padded_daily.groupby(
        ["defect_group", "defect_desc"], sort=False
    ):
        month_rates = (monthly_targets or {}).get(defect)
        if not month_rates:
            pieces.append(defect_df)
            continue
        pieces.append(
            _generate_defect_daily(
                defect_df, month_rates, product_code, defect, volatility
            )
        )
    return pd.concat(pieces).sort_index()
