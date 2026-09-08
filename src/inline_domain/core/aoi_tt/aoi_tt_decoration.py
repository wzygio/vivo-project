"""AOI_TT 超规片修饰：工作簿三态 flag + 默认自动截断。

与 SPC/CTQ 对齐（见 docs/dev_docs/generated/Inline_domain/decoration-unify-proposal.md）：

- 工作簿 `resources/inline_domain/aoi_tt_sheet_oos_decoration.xlsx`，每产品一个 sheet，
  复用共享引擎的三态语义：flag=Delete 删除该行、False 释放真实值、True（默认）自动截断；
- 无工作簿 / 缺产品 sheet 时按空修饰语义处理，超过 UCL 的行默认截断到 UCL 内；
- OOS 工作簿明细仍按 USL 判定；自动截断使用 UCL，缺失时不回退到 USL；
- 配置命中的参数豁免自动截断并保留真实值，Delete 仍优先；
- 截断算法与 flag 机制均来自 core/shared（单一算法来源）。
"""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.inline_domain.core.shared.auto_decoration import apply_tri_state_decoration

AOI_TT_OOS_DECORATION_FILE_NAME = "aoi_tt_sheet_oos_decoration.xlsx"
AOI_TT_OOS_KEY_COLUMNS = ["prod_code", "step_id", "tt_name", "sheet_id"]
AOI_TT_OOS_DETAIL_COLUMNS = [
    "factory",
    "prod_code",
    "step_id",
    "tt_name",
    "sheet_id",
    "lot_id",
    "start_time",
    "tt_qty",
    "usl",
]


def _spec_map(spec_df: pd.DataFrame, upper_col: str) -> pd.DataFrame:
    if spec_df.empty or upper_col not in spec_df.columns:
        return pd.DataFrame(columns=["step_id", "tt_name", upper_col])
    specs = spec_df[["step_id", "tt_name", upper_col]].copy()
    specs[upper_col] = pd.to_numeric(specs[upper_col], errors="coerce")
    return specs.dropna(subset=[upper_col]).drop_duplicates(["step_id", "tt_name"], keep="first")


def build_aoi_tt_oos_detail(tt_details_df: pd.DataFrame, spec_df: pd.DataFrame) -> pd.DataFrame:
    """列出 tt_qty 超过 USL 的超规片行（工作簿明细）。"""
    if tt_details_df.empty:
        return pd.DataFrame(columns=AOI_TT_OOS_DETAIL_COLUMNS)
    specs = _spec_map(spec_df, "usl")
    if specs.empty:
        return pd.DataFrame(columns=AOI_TT_OOS_DETAIL_COLUMNS)

    merged = tt_details_df.merge(specs, on=["step_id", "tt_name"], how="inner")
    detail = merged[pd.to_numeric(merged["tt_qty"], errors="coerce") > merged["usl"]]
    if detail.empty:
        return pd.DataFrame(columns=AOI_TT_OOS_DETAIL_COLUMNS)
    return (
        detail.reindex(columns=AOI_TT_OOS_DETAIL_COLUMNS)
        .sort_values(["factory", "step_id", "tt_name", "start_time", "sheet_id"], kind="stable")
        .reset_index(drop=True)
    )


def apply_aoi_tt_decoration(
    tt_details_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    decoration_df: pd.DataFrame,
    exempt_param_name_contains: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Apply tri-state decisions, clipping AOI-TT quantities against UCL."""
    decorated_df = tt_details_df.copy()
    if not decorated_df.empty:
        specs = _spec_map(spec_df, "ucl")
        if not specs.empty:
            attached = decorated_df.merge(
                specs.rename(columns={"ucl": "_ooc_ucl"}),
                on=["step_id", "tt_name"],
                how="left",
            )
            decorated_df = apply_tri_state_decoration(
                attached,
                decoration_df,
                key_columns=AOI_TT_OOS_KEY_COLUMNS,
                value_col="tt_qty",
                spec_col="_ooc_ucl",
                parameter_col="tt_name",
                exempt_param_name_contains=exempt_param_name_contains,
            ).drop(columns=["_ooc_ucl"])

    return decorated_df
