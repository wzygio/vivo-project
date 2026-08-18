"""AOI_RS 报表应用服务：缓存 payload → ViewModel（缓存边界遵循 ADR-0001）。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.inline_domain.application.shared.decorated_data import resolve_product_resource_dir
from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.core.aoi_rs.aoi_rs_calculator import (
    build_lot_point_df,
    build_sheet_point_df,
)
from src.inline_domain.core.aoi_rs.aoi_rs_decoration import prepare_aoi_rs_decoration
from src.shared_kernel.config import ConfigLoader

if TYPE_CHECKING:
    from src.inline_domain.application.aoi_rs.ports import AoiRsDataPort

logger = logging.getLogger(__name__)


@dataclass
class AoiRsReportViewModel:
    """AOI_RS 报表视图模型：明细、分母、规格、图表就绪点帧与指标元数据。"""

    rs_details_df: pd.DataFrame
    pass_through_df: pd.DataFrame
    spec_df: pd.DataFrame
    indicators_df: pd.DataFrame
    lot_points_df: pd.DataFrame
    sheet_points_df: pd.DataFrame


def _build_indicators(
    rs_details_df: pd.DataFrame,
    spec_df: pd.DataFrame,
) -> pd.DataFrame:
    """指标粒度 = 厂别 + 站点 + RS Code；code_desc 由规格表左连接带出（允许为空）。"""
    if rs_details_df.empty:
        return pd.DataFrame(columns=["prod_code", "factory", "step_id", "rs_code", "code_desc"])

    indicators = (
        rs_details_df[["prod_code", "factory", "step_id", "rs_code"]]
        .drop_duplicates()
        .sort_values(["factory", "step_id", "rs_code"])
        .reset_index(drop=True)
    )
    if not spec_df.empty and {"rs_code", "code_desc"}.issubset(spec_df.columns):
        desc_map = (
            spec_df[["rs_code", "code_desc"]]
            .dropna(subset=["code_desc"])
            .drop_duplicates(subset=["rs_code"])
        )
        indicators = indicators.merge(desc_map, on="rs_code", how="left")
    else:
        indicators["code_desc"] = pd.NA
    return indicators


def _build_chart_points(
    rs_details_df: pd.DataFrame,
    pass_through_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    prod_code: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build chart-ready lot/sheet point frames after tri-state workbook decoration.

    修饰统一位于 service 层（D4：是否修饰由 application 层决定，前端只渲染）。
    By Lot 用 LOT_RATIO 规格、By Sheet 用 SHEET_ID/GLASS_ID 规格，两图分别修饰；
    工作簿 flag=Delete 删除图点、False 释放真实值、True（默认）截断。
    """
    result = prepare_aoi_rs_decoration(
        build_lot_point_df(rs_details_df, pass_through_df),
        build_sheet_point_df(rs_details_df),
        spec_df,
        product_dir=resolve_product_resource_dir(prod_code),
        prod_code=prod_code,
        exempt_param_name_contains=ConfigLoader.get_auto_decoration_param_exemptions(),
    )
    return result.lot_points_df, result.sheet_points_df


class AoiRsReportService:
    """AOI_RS 报表应用服务。"""

    @staticmethod
    def _empty_payload() -> dict[str, object]:
        return {
            "rs_details_df": pd.DataFrame(),
            "pass_through_df": pd.DataFrame(),
            "spec_df": pd.DataFrame(),
            "indicators_df": pd.DataFrame(),
            "lot_points_df": pd.DataFrame(),
            "sheet_points_df": pd.DataFrame(),
        }

    @staticmethod
    def _view_model_from_payload(payload: dict[str, object]) -> AoiRsReportViewModel:
        def _df(key: str) -> pd.DataFrame:
            value = payload.get(key)
            return value if isinstance(value, pd.DataFrame) else pd.DataFrame()

        return AoiRsReportViewModel(
            rs_details_df=_df("rs_details_df"),
            pass_through_df=_df("pass_through_df"),
            spec_df=_df("spec_df"),
            indicators_df=_df("indicators_df"),
            lot_points_df=_df("lot_points_df"),
            sheet_points_df=_df("sheet_points_df"),
        )

    @staticmethod
    @st.cache_data(show_spinner=False, max_entries=3)
    def fetch_aoi_rs_report_payload(
        _data_port: "AoiRsDataPort",
        query_config_json: str,
        snapshot_signature: str = "",
    ) -> dict[str, object]:
        """缓存仅含 DataFrame 的原生 payload；失败降级为空。"""
        try:
            query_config = AoiRsQueryConfig.model_validate_json(query_config_json)
        except Exception as exc:
            logger.error("[AOI_RS] query config parse failed: %s", exc, exc_info=True)
            return AoiRsReportService._empty_payload()

        try:
            rs_details_df = _data_port.get_rs_details(query_config)
            if rs_details_df.empty:
                return AoiRsReportService._empty_payload()
            pass_through_df = _data_port.get_pass_through(query_config)
            spec_df = _data_port.get_rs_spec_limits(query_config.prod_code)
            # 超规修饰在 service 层完成：By Lot 用 LOT_RATIO 规格、By Sheet 用
            # SHEET_ID/GLASS_ID 规格，分别产出图表就绪的修饰后点帧（D4）
            lot_points_df, sheet_points_df = _build_chart_points(
                rs_details_df, pass_through_df, spec_df, query_config.prod_code
            )
            indicators_df = _build_indicators(rs_details_df, spec_df)
            return {
                "rs_details_df": rs_details_df,
                "pass_through_df": pass_through_df,
                "spec_df": spec_df,
                "indicators_df": indicators_df,
                "lot_points_df": lot_points_df,
                "sheet_points_df": sheet_points_df,
            }
        except Exception as exc:
            logger.error("[AOI_RS] report generation failed: %s", exc, exc_info=True)
            return AoiRsReportService._empty_payload()

    @staticmethod
    def get_aoi_rs_report_data(
        _data_port: "AoiRsDataPort",
        query_config_json: str,
        snapshot_signature: str = "",
    ) -> AoiRsReportViewModel:
        """在 Streamlit pickle 缓存边界外构造 ViewModel。"""
        payload = AoiRsReportService.fetch_aoi_rs_report_payload(
            _data_port=_data_port,
            query_config_json=query_config_json,
            snapshot_signature=snapshot_signature,
        )
        return AoiRsReportService._view_model_from_payload(payload)
