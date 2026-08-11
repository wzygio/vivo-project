"""AOI_TT 报表应用服务：缓存 payload → ViewModel（缓存边界遵循 ADR-0001）。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.inline_domain.infrastructure.aoi_tt.data_loader import (
    AoiTtQueryConfig,
    load_tt_details,
    load_tt_spec_limits,
)

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class AoiTtReportViewModel:
    """AOI_TT 报表视图模型：明细、规格与指标元数据。"""

    tt_details_df: pd.DataFrame
    spec_df: pd.DataFrame
    indicators_df: pd.DataFrame


def _build_indicators(tt_details_df: pd.DataFrame) -> pd.DataFrame:
    """指标粒度 = 厂别 + 站点 + TT 参数名。"""
    if tt_details_df.empty:
        return pd.DataFrame(columns=["prod_code", "factory", "step_id", "tt_name"])

    indicators = (
        tt_details_df[["prod_code", "factory", "step_id", "tt_name"]]
        .drop_duplicates()
        .sort_values(["factory", "step_id", "tt_name"])
        .reset_index(drop=True)
    )
    return indicators


class AoiTtReportService:
    """AOI_TT 报表应用服务。"""

    @staticmethod
    def _empty_payload() -> dict[str, object]:
        return {
            "tt_details_df": pd.DataFrame(),
            "spec_df": pd.DataFrame(),
            "indicators_df": pd.DataFrame(),
        }

    @staticmethod
    def _view_model_from_payload(payload: dict[str, object]) -> AoiTtReportViewModel:
        def _df(key: str) -> pd.DataFrame:
            value = payload.get(key)
            return value if isinstance(value, pd.DataFrame) else pd.DataFrame()

        return AoiTtReportViewModel(
            tt_details_df=_df("tt_details_df"),
            spec_df=_df("spec_df"),
            indicators_df=_df("indicators_df"),
        )

    @staticmethod
    @st.cache_data(show_spinner=False, max_entries=3)
    def fetch_aoi_tt_report_payload(
        _db_manager: "DatabaseManager",
        query_config_json: str,
        snapshot_signature: str = "",
    ) -> dict[str, object]:
        """缓存仅含 DataFrame 的原生 payload；失败降级为空。"""
        try:
            query_config = AoiTtQueryConfig.model_validate_json(query_config_json)
        except Exception as exc:
            logger.error("[AOI_TT] query config parse failed: %s", exc, exc_info=True)
            return AoiTtReportService._empty_payload()

        try:
            tt_details_df = load_tt_details(_db_manager, query_config)
            if tt_details_df.empty:
                return AoiTtReportService._empty_payload()
            spec_df = load_tt_spec_limits(_db_manager, query_config.prod_code)
            indicators_df = _build_indicators(tt_details_df)
            return {
                "tt_details_df": tt_details_df,
                "spec_df": spec_df,
                "indicators_df": indicators_df,
            }
        except Exception as exc:
            logger.error("[AOI_TT] report generation failed: %s", exc, exc_info=True)
            return AoiTtReportService._empty_payload()

    @staticmethod
    def get_aoi_tt_report_data(
        _db_manager: "DatabaseManager",
        query_config_json: str,
        snapshot_signature: str = "",
    ) -> AoiTtReportViewModel:
        """在 Streamlit pickle 缓存边界外构造 ViewModel。"""
        payload = AoiTtReportService.fetch_aoi_tt_report_payload(
            _db_manager=_db_manager,
            query_config_json=query_config_json,
            snapshot_signature=snapshot_signature,
        )
        return AoiTtReportService._view_model_from_payload(payload)
