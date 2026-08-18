"""超规项自动修饰：把越出规格线的值截断为线内的确定性伪随机值。

仿照 SPC Sheet OOS 修饰的截断语义
（`core/shared/sheet_oos_decoration.py::_clip_inside_spec`）：

- 上限越规 → 截断到上限以下 5%~15% span 处；下限越规对称处理；
- 单边规格（无下限）时 span 以 0 为下界（即截断到上限的 85%~95%）；
- 伪随机由稳定哈希驱动，同一数据行重跑结果一致（报表可复现）；
- 配置命中的豁免参数保留真实值，Delete 动作仍优先删除对应明细。

供无工作簿机制的业务模块（如 aoi_tt / aoi_rs）复用的最简自动修饰。
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

import pandas as pd

from src.inline_domain.core.shared.sheet_oos_decoration import (
    _exclude_delete_flagged_measurements,
    _is_delete_action,
    _normalize_key_columns,
    _parse_flag,
    _stable_fraction,
)

logger = logging.getLogger(__name__)

_LOWER_MARGIN = 0.05
_MARGIN_SPAN = 0.1

_SEED_CANDIDATE_COLUMNS = ["prod_code", "factory", "step_id", "rs_code", "tt_name", "sheet_id", "lot_id"]


def _parameter_exemption_mask(
    df: pd.DataFrame,
    parameter_col: str | None,
    exempt_param_name_contains: Iterable[str] | None,
) -> pd.Series:
    """Return rows whose parameter name matches a configured literal token."""
    mask = pd.Series(False, index=df.index)
    if not parameter_col or parameter_col not in df.columns:
        return mask

    needles = [
        str(value).strip()
        for value in exempt_param_name_contains or []
        if value is not None and str(value).strip()
    ]
    if not needles:
        return mask

    parameter_names = df[parameter_col].fillna("").astype(str)
    for needle in needles:
        mask |= parameter_names.str.contains(needle, case=False, regex=False)
    return mask


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


def _clip_in_place(
    df: pd.DataFrame,
    value_col: str,
    upper_col: str,
    lower_col: str | None = None,
    parameter_col: str | None = None,
    exempt_param_name_contains: Iterable[str] | None = None,
) -> pd.DataFrame:
    """对同帧内的规格列执行截断（upper_col/lower_col 为 df 自身列）。"""
    result = df.copy()
    result[value_col] = pd.to_numeric(result[value_col], errors="coerce")
    result[upper_col] = pd.to_numeric(result[upper_col], errors="coerce")
    if lower_col is not None:
        result[lower_col] = pd.to_numeric(result[lower_col], errors="coerce")

    over_mask = result[upper_col].notna() & result[value_col].notna() & (
        result[value_col] > result[upper_col]
    )
    if lower_col is not None:
        over_mask = over_mask | (
            result[lower_col].notna()
            & result[value_col].notna()
            & (result[value_col] < result[lower_col])
        )
    exemption_mask = _parameter_exemption_mask(
        result,
        parameter_col,
        exempt_param_name_contains,
    )
    over_mask &= ~exemption_mask
    if not over_mask.any():
        return result

    seed_cols = [c for c in _SEED_CANDIDATE_COLUMNS if c in result.columns]
    clipped = result.loc[over_mask].apply(
        lambda row: _clip_value(
            row[value_col],
            float(row[upper_col]),
            float(row[lower_col])
            if lower_col is not None and pd.notna(row[lower_col])
            else None,
            [row[c] for c in seed_cols] + [row[value_col]],
        ),
        axis=1,
    )
    logger.info("[AutoDecoration] 自动截断 %d 个超规点（%s）", int(over_mask.sum()), value_col)
    result.loc[over_mask, value_col] = clipped
    return result


def clip_over_spec_column(
    df: pd.DataFrame,
    *,
    value_col: str,
    spec_col: str,
    lower_spec_col: str | None = None,
    parameter_col: str | None = None,
    exempt_param_name_contains: Iterable[str] | None = None,
) -> pd.DataFrame:
    """同帧截断：df 已带规格列（如 attach_spec_values 之后）时使用。

    :param value_col: 被修饰的值列。
    :param spec_col: 上限规格列（NaN = 无规格，不修饰）。
    :param lower_spec_col: 下限规格列（双边规格时使用）。
    :param parameter_col: 参数名称列；提供后可按名称应用豁免。
    :param exempt_param_name_contains: 大小写不敏感的参数名包含规则。
    """
    if df.empty or value_col not in df.columns or spec_col not in df.columns:
        return df.copy()
    return _clip_in_place(
        df,
        value_col,
        spec_col,
        lower_spec_col,
        parameter_col,
        exempt_param_name_contains,
    )


def auto_clip_over_spec(
    df: pd.DataFrame,
    spec_df: pd.DataFrame,
    *,
    value_col: str,
    join_keys: list[str],
    upper_col: str,
    lower_col: str | None = None,
    parameter_col: str | None = None,
    exempt_param_name_contains: Iterable[str] | None = None,
) -> pd.DataFrame:
    """按规格线自动截断 df 中越规的 value_col 值（无工作簿的最简修饰）。

    :param df: 明细数据，须含 join_keys 与 value_col。
    :param spec_df: 规格数据，须含 join_keys 与 upper_col（可选 lower_col）。
    :param join_keys: 明细与规格的匹配键（须同名）；同一键组合取第一条规格。
    :param upper_col: 规格上限列名；单边规格时 lower_col 传 None。
    :param lower_col: 规格下限列名（双边规格时使用）。
    :param parameter_col: 参数名称列；提供后可按名称应用豁免。
    :param exempt_param_name_contains: 大小写不敏感的参数名包含规则。
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

    merged = df.merge(specs, on=join_keys, how="left")
    clipped = _clip_in_place(
        merged,
        value_col,
        "_auto_upper",
        "_auto_lower" if lower_col else None,
        parameter_col,
        exempt_param_name_contains,
    )
    return clipped.drop(columns=["_auto_upper"] + (["_auto_lower"] if lower_col else []))


def apply_tri_state_decoration(
    df: pd.DataFrame,
    decoration_df: pd.DataFrame,
    *,
    key_columns: list[str],
    value_col: str,
    spec_col: str,
    parameter_col: str | None = None,
    exempt_param_name_contains: Iterable[str] | None = None,
) -> pd.DataFrame:
    """按工作簿三态 flag 修饰 df（df 须已带 spec_col 规格列）。

    - flag=Delete：剔除匹配 key_columns 的行；
    - flag=False：释放真实值（不截断）；
    - 参数豁免：保留真实值，优先级低于 Delete、高于 True；
    - flag=True（默认）：非豁免参数按 spec_col 截断。

    不在 decoration_df 中的行按规格正常截断（与默认 flag=True 一致）。
    """
    if df.empty or value_col not in df.columns or spec_col not in df.columns:
        return df.copy()
    if decoration_df is None or decoration_df.empty or "flag" not in decoration_df.columns:
        return _clip_in_place(
            df,
            value_col,
            spec_col,
            parameter_col=parameter_col,
            exempt_param_name_contains=exempt_param_name_contains,
        )

    result = _exclude_delete_flagged_measurements(df, decoration_df, key_columns)
    if result.empty:
        return result

    released = decoration_df[
        ~decoration_df["flag"].apply(_is_delete_action)
        & ~decoration_df["flag"].apply(_parse_flag)
    ]
    if released.empty:
        return _clip_in_place(
            result,
            value_col,
            spec_col,
            parameter_col=parameter_col,
            exempt_param_name_contains=exempt_param_name_contains,
        )

    release_keys = _normalize_key_columns(
        released[key_columns].drop_duplicates(), key_columns
    ).assign(_released=True)
    merged = _normalize_key_columns(result, key_columns).merge(
        release_keys, on=key_columns, how="left", validate="many_to_one"
    )
    merged.loc[merged["_released"].eq(True), spec_col] = pd.NA
    clipped = _clip_in_place(
        merged,
        value_col,
        spec_col,
        parameter_col=parameter_col,
        exempt_param_name_contains=exempt_param_name_contains,
    )
    return clipped.drop(columns=["_released"])
