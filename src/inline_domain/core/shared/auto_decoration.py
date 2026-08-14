"""超规项自动修饰：把越出规格线的值截断为线内的确定性伪随机值。

仿照 SPC Sheet OOS 修饰的截断语义
（`core/spc/spc_sheet_oos_decoration.py::_clip_inside_spec`）：

- 上限越规 → 截断到上限以下 5%~15% span 处；下限越规对称处理；
- 单边规格（无下限）时 span 以 0 为下界（即截断到上限的 85%~95%）；
- 伪随机由稳定哈希驱动，同一数据行重跑结果一致（报表可复现）。

供无工作簿机制的业务模块（如 aoi_tt / aoi_rs）复用的最简自动修饰。
"""

from __future__ import annotations

import logging

import pandas as pd

from src.inline_domain.core.spc.spc_sheet_oos_decoration import _stable_fraction

logger = logging.getLogger(__name__)

_LOWER_MARGIN = 0.05
_MARGIN_SPAN = 0.1


def _clip_value(value: float, upper: float, lower: float | None, seed_parts: list[object]) -> float:
    """把越规值截断到线内；未越规原样返回。"""
    if lower is not None and not pd.isna(lower) and upper <= lower:
        return value  # 非法规格（上限<=下限）不修饰
    if lower is not None and pd.isna(lower):
        lower = None
    span = upper - lower if lower is not None else upper - 0.0
    if span <= 0:
        return value
    if value > upper:
        margin = (_LOWER_MARGIN + _stable_fraction([*seed_parts, "upper"]) * _MARGIN_SPAN) * span
        return upper - margin
    if lower is not None and value < lower:
        margin = (_LOWER_MARGIN + _stable_fraction([*seed_parts, "lower"]) * _MARGIN_SPAN) * span
        return lower + margin
    return value


def auto_clip_over_spec(
    df: pd.DataFrame,
    spec_df: pd.DataFrame,
    *,
    value_col: str,
    join_keys: list[str],
    upper_col: str,
    lower_col: str | None = None,
) -> pd.DataFrame:
    """按规格线自动截断 df 中越规的 value_col 值（无工作簿的最简修饰）。

    :param df: 明细数据，须含 join_keys 与 value_col。
    :param spec_df: 规格数据，须含 join_keys 与 upper_col（可选 lower_col）。
    :param join_keys: 明细与规格的匹配键（须同名）；同一键组合取第一条规格。
    :param upper_col: 规格上限列名；单边规格时 lower_col 传 None。
    :param lower_col: 规格下限列名（双边规格时使用）。
    :return: 截断后的新 DataFrame；无匹配规格的行保持原值。
    """
    if df.empty or spec_df.empty or value_col not in df.columns:
        return df.copy()
    if upper_col not in spec_df.columns or not set(join_keys).issubset(df.columns):
        return df.copy()
    if not set(join_keys).issubset(spec_df.columns):
        return df.copy()

    spec_cols = [*join_keys, upper_col] + ([lower_col] if lower_col else [])
    specs = spec_df[spec_cols].copy()
    specs[upper_col] = pd.to_numeric(specs[upper_col], errors="coerce")
    specs = specs.dropna(subset=[upper_col]).drop_duplicates(subset=join_keys, keep="first")
    if specs.empty:
        return df.copy()
    specs = specs.rename(columns={upper_col: "_auto_upper"} | ({lower_col: "_auto_lower"} if lower_col else {}))

    result = df.copy()
    result[value_col] = pd.to_numeric(result[value_col], errors="coerce")
    result = result.merge(specs, on=join_keys, how="left")

    over_mask = result["_auto_upper"].notna() & result[value_col].notna() & (
        result[value_col] > result["_auto_upper"]
    )
    if lower_col is not None:
        over_mask = over_mask | (
            result["_auto_lower"].notna()
            & result[value_col].notna()
            & (result[value_col] < result["_auto_lower"])
        )
    if over_mask.any():
        seed_cols = [c for c in ["prod_code", "factory", "step_id", "sheet_id", "lot_id"] if c in result.columns]
        clipped = result.loc[over_mask].apply(
            lambda row: _clip_value(
                row[value_col],
                float(row["_auto_upper"]),
                float(row["_auto_lower"])
                if lower_col is not None and pd.notna(row["_auto_lower"])
                else None,
                [row[c] for c in seed_cols] + [row[value_col]],
            ),
            axis=1,
        )
        logger.info("[AutoDecoration] 自动截断 %d 个超规点（%s）", int(over_mask.sum()), value_col)
        result.loc[over_mask, value_col] = clipped

    return result.drop(columns=["_auto_upper"] + (["_auto_lower"] if lower_col else []))
