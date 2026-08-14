"""AOI_RS 报表应用服务：缓存 payload → ViewModel（缓存边界遵循 ADR-0001）。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.inline_domain.infrastructure.aoi_rs.data_loader import (
    AoiRsQueryConfig,
    load_pass_through,
    load_rs_details,
    load_rs_spec_limits,
)

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class AoiRsReportViewModel:
    """AOI_RS 报表视图模型：明细、分母、规格与指标元数据。"""

    rs_details_df: pd.DataFrame
    pass_through_df: pd.DataFrame
    spec_df: pd.DataFrame
    indicators_df: pd.DataFrame


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


class AoiRsReportService:
    """AOI_RS 报表应用服务。"""

    @staticmethod
    def _empty_payload() -> dict[str, object]:
        return {
            "rs_details_df": pd.DataFrame(),
            "pass_through_df": pd.DataFrame(),
            "spec_df": pd.DataFrame(),
            "indicators_df": pd.DataFrame(),
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
        )

    @staticmethod
    @st.cache_data(show_spinner=False, max_entries=3)
    def fetch_aoi_rs_report_payload(
        _db_manager: "DatabaseManager",
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
            rs_details_df = load_rs_details(_db_manager, query_config)
            if rs_details_df.empty:
                return AoiRsReportService._empty_payload()
            pass_through_df = load_pass_through(_db_manager, query_config)
            spec_df = load_rs_spec_limits(_db_manager, query_config.prod_code)
            # 修饰不在 service 层做：lot/sheet 图各自使用不同 type_flag 的规格线，
            # 截断发生在图表组装层（见 sections/aoi_rs/aoi_rs_dashboard.py）
            indicators_df = _build_indicators(rs_details_df, spec_df)
            return {
                "rs_details_df": rs_details_df,
                "pass_through_df": pass_through_df,
                "spec_df": spec_df,
                "indicators_df": indicators_df,
            }
        except Exception as exc:
            logger.error("[AOI_RS] report generation failed: %s", exc, exc_info=True)
            return AoiRsReportService._empty_payload()

    @staticmethod
    def get_aoi_rs_report_data(
        _db_manager: "DatabaseManager",
        query_config_json: str,
        snapshot_signature: str = "",
    ) -> AoiRsReportViewModel:
        """在 Streamlit pickle 缓存边界外构造 ViewModel。"""
        payload = AoiRsReportService.fetch_aoi_rs_report_payload(
            _db_manager=_db_manager,
            query_config_json=query_config_json,
            snapshot_signature=snapshot_signature,
        )
        return AoiRsReportService._view_model_from_payload(payload)
