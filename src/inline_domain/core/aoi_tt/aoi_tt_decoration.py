"""AOI_TT 超规片修饰：工作簿三态 flag + 默认自动截断。

与 SPC/CTQ 对齐（见 docs/dev_docs/generated/Inline_domain/decoration-unify-proposal.md）：

- 工作簿 `resources/inline_domain/aoi_tt_sheet_oos_decoration.xlsx`，每产品一个 sheet，
  复用共享引擎的三态语义：flag=Delete 删除该行、False 释放真实值、True（默认）自动截断；
- 无工作簿 / 缺产品 sheet 时按空修饰语义处理 —— 全部超规行默认截断，
  与引入工作簿前的 auto_clip_over_spec 行为完全一致（向后兼容）；
- 配置命中的参数豁免自动截断并保留真实值，Delete 仍优先；
- 截断算法与 flag 机制均来自 core/shared（单一算法来源）。
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.inline_domain.core.shared.auto_decoration import apply_tri_state_decoration
from src.inline_domain.core.shared.sheet_oos_decoration import (
    load_sheet_oos_decoration,
    merge_detail_with_decoration_flags,
    persist_sheet_oos_decoration,
)

logger = logging.getLogger(__name__)

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


@dataclass(frozen=True)
class AoiTtDecorationResult:
    """AOI_TT 明细经工作簿三态修饰后的结果。"""

    tt_details_df: pd.DataFrame
    decoration_df: pd.DataFrame
    decoration_path: Path
    decoration_sheet: str


def _oos_spec_map(spec_df: pd.DataFrame) -> pd.DataFrame:
    if spec_df.empty or "usl" not in spec_df.columns:
        return pd.DataFrame(columns=["step_id", "tt_name", "usl"])
    specs = spec_df[["step_id", "tt_name", "usl"]].copy()
    specs["usl"] = pd.to_numeric(specs["usl"], errors="coerce")
    return specs.dropna(subset=["usl"]).drop_duplicates(["step_id", "tt_name"], keep="first")


def build_aoi_tt_oos_detail(tt_details_df: pd.DataFrame, spec_df: pd.DataFrame) -> pd.DataFrame:
    """列出 tt_qty 超过 USL 的超规片行（工作簿明细）。"""
    if tt_details_df.empty:
        return pd.DataFrame(columns=AOI_TT_OOS_DETAIL_COLUMNS)
    specs = _oos_spec_map(spec_df)
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


def prepare_aoi_tt_decoration(
    tt_details_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    product_dir: Path,
    prod_code: str,
    persist: bool = True,
    exempt_param_name_contains: Iterable[str] | None = None,
) -> AoiTtDecorationResult:
    """应用三态 flag 与参数豁免；Delete 的优先级最高。"""
    detail_df = build_aoi_tt_oos_detail(tt_details_df, spec_df)
    if persist:
        decoration_df = persist_sheet_oos_decoration(
            product_dir,
            detail_df,
            AOI_TT_OOS_DECORATION_FILE_NAME,
            prod_code,
            key_columns=AOI_TT_OOS_KEY_COLUMNS,
        )
    else:
        decoration_df = merge_detail_with_decoration_flags(
            detail_df,
            load_sheet_oos_decoration(
                product_dir,
                AOI_TT_OOS_DECORATION_FILE_NAME,
                prod_code,
                key_columns=AOI_TT_OOS_KEY_COLUMNS,
            ),
            key_columns=AOI_TT_OOS_KEY_COLUMNS,
        )

    decorated_df = tt_details_df.copy()
    if not decorated_df.empty:
        specs = _oos_spec_map(spec_df)
        if not specs.empty:
            attached = decorated_df.merge(
                specs.rename(columns={"usl": "_oos_usl"}),
                on=["step_id", "tt_name"],
                how="left",
            )
            decorated_df = apply_tri_state_decoration(
                attached,
                decoration_df,
                key_columns=AOI_TT_OOS_KEY_COLUMNS,
                value_col="tt_qty",
                spec_col="_oos_usl",
                parameter_col="tt_name",
                exempt_param_name_contains=exempt_param_name_contains,
            ).drop(columns=["_oos_usl"])

    logger.info(
        "[AOI_TT] Sheet OOS decoration prepared for %s: oos=%s, rows=%s",
        prod_code,
        len(decoration_df),
        len(decorated_df),
    )
    return AoiTtDecorationResult(
        tt_details_df=decorated_df,
        decoration_df=decoration_df,
        decoration_path=product_dir / AOI_TT_OOS_DECORATION_FILE_NAME,
        decoration_sheet=prod_code,
    )
