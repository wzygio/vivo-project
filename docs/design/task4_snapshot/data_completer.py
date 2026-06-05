# src/equipment_domain/core/data_completer.py
"""
[核心业务逻辑] 数据补全与超规钳制。

两遍扫描策略:
1. 第一遍: 标记所有真实数据（测量值非空）
2. 第二遍: 仅从真实对侧数据补全缺失值（不级联模拟数据）
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

SIMULATED_CAP_RATIO = 0.95
PERTURBATION_MIN = 0.97
PERTURBATION_MAX = 1.03
OVER_SPEC_CLAMP_RATIO = 0.95

SOURCE_REAL = "真实"
SOURCE_SIMULATED = "模拟"
SOURCE_CLAMPED = "钳制"


def fill_missing_by_pairing(
    report_df: pd.DataFrame,
    seed: int = 42,
) -> pd.DataFrame:
    """
    两遍扫描补全缺失测量值（仅从真实对侧数据模拟，不级联）。

    规则:
    1. 第一遍: 标记所有已有测量值的行为"真实"
    2. 第二遍: 仅当对侧数据来源为"真实"时才模拟
    3. 模拟值 = 对侧值 * (本侧规格 / 对侧规格) * random(0.97, 1.03)
    4. 钳制上限: min(模拟值, 0.95 * 本侧规格)

    Args:
        report_df: 报表明细 DataFrame
        seed: 随机种子

    Returns:
        pd.DataFrame: 补全后的 DataFrame（原地修改）
    """
    if "数据来源" not in report_df.columns:
        report_df["数据来源"] = ""

    # 第一遍: 标记真实数据
    report_df.loc[report_df["测量值"].notna(), "数据来源"] = SOURCE_REAL

    # 第二遍: 仅从真实对侧模拟
    rng = np.random.RandomState(seed)
    filled_count = 0

    for idx, row in report_df.iterrows():
        if pd.notna(row.get("测量值")):
            continue

        station = row["站点"]
        machine = row["机台号-腔室"]
        part_type = row["备件类型"]
        this_spec = row["寿命规格"]

        if pd.isna(this_spec) or this_spec <= 0:
            continue

        # 只找数据来源为"真实"的对侧行
        opposite = report_df[
            (report_df["站点"] == station)
            & (report_df["机台号-腔室"] == machine)
            & (report_df["备件类型"] != part_type)
            & (report_df["数据来源"] == SOURCE_REAL)
        ]
        if opposite.empty:
            continue

        opposite_row = opposite.iloc[0]
        opposite_value = opposite_row["测量值"]
        opposite_spec = opposite_row["寿命规格"]

        if pd.isna(opposite_spec) or opposite_spec <= 0:
            continue

        scale = this_spec / opposite_spec
        perturbation = rng.uniform(PERTURBATION_MIN, PERTURBATION_MAX)
        simulated = opposite_value * scale * perturbation

        cap = this_spec * SIMULATED_CAP_RATIO
        simulated = min(simulated, cap)

        report_df.at[idx, "测量值"] = round(simulated, 1)
        report_df.at[idx, "数据来源"] = SOURCE_SIMULATED
        report_df.at[idx, "匹配参数名"] = f"(simulated from {opposite_row['备件类型']})"
        report_df.at[idx, "测量时间"] = opposite_row.get("测量时间")
        filled_count += 1

    logger.info(f"Filled {filled_count} missing values (from real paired data only)")
    return report_df


def clamp_over_spec(report_df: pd.DataFrame) -> pd.DataFrame:
    """超规值钳制: 测量值 = 0.95 * 寿命规格。"""
    if "数据来源" not in report_df.columns:
        report_df["数据来源"] = SOURCE_REAL

    over_mask = (
        report_df["测量值"].notna()
        & (report_df["测量值"] > report_df["寿命规格"])
        & (report_df["寿命规格"] > 0)
        & (report_df.get("数据来源", "").str.contains("模拟", na=False))
    )
    over_count = over_mask.sum()

    if over_count > 0:
        report_df.loc[over_mask, "测量值"] = (
            report_df.loc[over_mask, "寿命规格"] * OVER_SPEC_CLAMP_RATIO
        )
        report_df.loc[over_mask, "数据来源"] = (
            report_df.loc[over_mask, "数据来源"].astype(str) + "+" + SOURCE_CLAMPED
        )
        logger.info(f"Clamped {over_count} over-spec values")

    return report_df