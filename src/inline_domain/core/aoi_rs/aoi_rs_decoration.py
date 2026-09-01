"""AOI_RS 超规修饰：工作簿三态 flag + 默认自动截断。

与 SPC/CTQ/AOI_TT 对齐（见 docs/dev_docs/generated/Inline_domain/decoration-unify-proposal.md）：

- 工作簿 `resources/inline_domain/aoi_rs_sheet_oos_decoration.xlsx`，每产品一个 sheet，
  复用共享引擎三态语义：flag=Delete 删除图点、False 释放真实值、True（默认）截断；
- By Lot 与 By Sheet 两张图规格来源不同（LOT_RATIO vs SHEET_ID/GLASS_ID），
  工作簿以 ``chart_kind`` 列区分图口径，``point_id`` 在 lot 图取 lot_id、
  sheet 图取 sheet_id；
- 无工作簿 / 缺产品 sheet 时按空修饰语义处理 —— 全部超规点默认截断，
  与引入工作簿前的 clip_over_spec_column 行为一致（向后兼容）。
- 配置命中的参数豁免自动截断并保留真实值，Delete 仍优先。
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.inline_domain.core.aoi_rs.aoi_rs_calculator import attach_spec_values
from src.inline_domain.core.shared.auto_decoration import apply_tri_state_decoration
from src.inline_domain.core.shared.sheet_oos_decoration import (
    load_sheet_oos_decisions,
    merge_detail_with_decoration_flags,
    persist_sheet_oos_decoration,
)

logger = logging.getLogger(__name__)

AOI_RS_OOS_DECORATION_FILE_NAME = "aoi_rs_sheet_oos_decoration.xlsx"
AOI_RS_OOS_KEY_COLUMNS = [
    "prod_code",
    "factory",
    "step_id",
    "rs_code",
    "chart_kind",
    "point_id",
]
AOI_RS_OOS_DETAIL_COLUMNS = [*AOI_RS_OOS_KEY_COLUMNS, "value", "spec", "sheet_start_time"]

# chart_kind -> (点帧 id 列, 值列)
_CHART_POINT_META = {
    "lot": ("lot_id", "value"),
    "sheet": ("sheet_id", "rs_qty"),
}


@dataclass(frozen=True)
class AoiRsDecorationResult:
    """AOI_RS 图表点帧经工作簿三态修饰后的结果。"""

    lot_points_df: pd.DataFrame
    sheet_points_df: pd.DataFrame
    decoration_df: pd.DataFrame
    decoration_path: Path
    decoration_sheet: str


def _normalized_points(
    attached_df: pd.DataFrame, chart_kind: str, prod_code: str
) -> pd.DataFrame:
    """把图表点帧归一化为 (key..., point_id, value, spec) 结构。

    图表点帧本身不带 prod_code（键含厂别/站点/Code 已够绘图），
    工作簿键含 prod_code，这里按查询产品补齐。
    """
    id_col, value_col = _CHART_POINT_META[chart_kind]
    if attached_df.empty:
        return pd.DataFrame(columns=AOI_RS_OOS_DETAIL_COLUMNS)
    result = attached_df.copy()
    result["prod_code"] = prod_code
    result["point_id"] = result[id_col].fillna("").astype(str)
    result["chart_kind"] = chart_kind
    result["value"] = pd.to_numeric(result[value_col], errors="coerce")
    # 预警需要按上一 ISO 周筛选：sheet/lot 的起始时间来自点帧聚合的 first_start_time
    result["sheet_start_time"] = pd.to_datetime(
        result.get("first_start_time"), errors="coerce"
    )
    return result


def build_aoi_rs_oos_detail(
    lot_points_df: pd.DataFrame,
    sheet_points_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    prod_code: str,
) -> pd.DataFrame:
    """列出两张图中超过规格的点（工作簿明细，含 chart_kind 维度）。"""
    frames: list[pd.DataFrame] = []
    for chart_kind, points_df in (("lot", lot_points_df), ("sheet", sheet_points_df)):
        attached = attach_spec_values(points_df, spec_df, chart_kind=chart_kind)
        normalized = _normalized_points(attached, chart_kind, prod_code)
        if normalized.empty:
            continue
        oos = normalized[
            normalized["spec"].notna() & (normalized["value"] > normalized["spec"])
        ]
        if not oos.empty:
            frames.append(oos)
    if not frames:
        return pd.DataFrame(columns=AOI_RS_OOS_DETAIL_COLUMNS)
    return (
        pd.concat(frames, ignore_index=True)
        .reindex(columns=AOI_RS_OOS_DETAIL_COLUMNS)
        .sort_values(["factory", "step_id", "rs_code", "chart_kind", "point_id"], kind="stable")
        .reset_index(drop=True)
    )


def _apply_chart_decoration(
    points_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    decoration_df: pd.DataFrame,
    chart_kind: str,
    prod_code: str,
    exempt_param_name_contains: Iterable[str] | None = None,
) -> pd.DataFrame:
    """对单张图的点帧应用三态 flag（Delete 剔除 / False 释放 / True 截断）。"""
    id_col, value_col = _CHART_POINT_META[chart_kind]
    attached = attach_spec_values(points_df, spec_df, chart_kind=chart_kind)
    if attached.empty:
        return attached.drop(columns=["spec"], errors="ignore")

    normalized = _normalized_points(attached, chart_kind, prod_code)
    chart_flags = (
        decoration_df[decoration_df["chart_kind"].astype(str) == chart_kind]
        if not decoration_df.empty and "chart_kind" in decoration_df.columns
        else pd.DataFrame()
    )
    decorated = apply_tri_state_decoration(
        normalized,
        chart_flags,
        key_columns=AOI_RS_OOS_KEY_COLUMNS,
        value_col="value",
        spec_col="spec",
        parameter_col="rs_code",
        exempt_param_name_contains=exempt_param_name_contains,
    )
    if value_col != "value":
        decorated[value_col] = decorated["value"]
        decorated = decorated.drop(columns=["value"])
    return decorated.drop(columns=["spec", "chart_kind", "point_id", "sheet_start_time"])


def prepare_aoi_rs_decoration(
    lot_points_df: pd.DataFrame,
    sheet_points_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    product_dir: Path,
    prod_code: str,
    persist: bool = True,
    exempt_param_name_contains: Iterable[str] | None = None,
    *,
    scope: str | None = None,
    product_revision: str = "",
    decision_signature: str = "",
    now: datetime | None = None,
) -> AoiRsDecorationResult:
    """对 By Lot / By Sheet 点帧应用三态修饰与参数豁免。

    ``persist=True`` 且传入 ``scope`` 时启用共享刷新门控（与 SPC/CTQ 一致）：
    产品明细 sheet / meta 缺失、``product_revision`` 或 ``decision_signature``
    变化、或距上次成功写入超过 TTL 才重写工作簿，否则只算不写；
    meta 行按 (scope, prod_code) 隔离记录在 ``__refresh_meta__``。
    不传 ``scope`` 保持旧语义（总是持久化、不维护 meta）。

    决策来源只有 ``<产品>__flags``：缺失即空台账（__flags 只记录人为决策，
    2026-09-01 起不再从旧产品 sheet 迁移）。
    """
    detail_df = build_aoi_rs_oos_detail(lot_points_df, sheet_points_df, spec_df, prod_code)
    if persist:
        decoration_df = persist_sheet_oos_decoration(
            product_dir,
            detail_df,
            AOI_RS_OOS_DECORATION_FILE_NAME,
            prod_code,
            key_columns=AOI_RS_OOS_KEY_COLUMNS,
            scope=scope,
            prod_code=prod_code,
            product_revision=product_revision,
            decision_signature=decision_signature,
            now=now,
        )
    else:
        decoration_df = merge_detail_with_decoration_flags(
            detail_df,
            load_sheet_oos_decisions(
                product_dir,
                AOI_RS_OOS_DECORATION_FILE_NAME,
                prod_code,
                key_columns=AOI_RS_OOS_KEY_COLUMNS,
            ),
            key_columns=AOI_RS_OOS_KEY_COLUMNS,
        )

    lot_decorated = _apply_chart_decoration(
        lot_points_df,
        spec_df,
        decoration_df,
        "lot",
        prod_code,
        exempt_param_name_contains,
    )
    sheet_decorated = _apply_chart_decoration(
        sheet_points_df,
        spec_df,
        decoration_df,
        "sheet",
        prod_code,
        exempt_param_name_contains,
    )
    logger.info(
        "[AOI_RS] Sheet OOS decoration prepared for %s: oos=%s, lot=%s, sheet=%s",
        prod_code,
        len(decoration_df),
        len(lot_decorated),
        len(sheet_decorated),
    )
    return AoiRsDecorationResult(
        lot_points_df=lot_decorated,
        sheet_points_df=sheet_decorated,
        decoration_df=decoration_df,
        decoration_path=product_dir / AOI_RS_OOS_DECORATION_FILE_NAME,
        decoration_sheet=prod_code,
    )
