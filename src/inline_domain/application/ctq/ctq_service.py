from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.inline_domain.application.shared.decorated_features import (
    fetch_decorated_features,
)
from src.inline_domain.core.ctq.indicator_chart import assign_ctq_indicator_chart_type
from src.inline_domain.core.shared.sheet_oos_decoration import SheetOosDecorationResult
from src.inline_domain.application.spc.dtos import SpcQueryConfig

if TYPE_CHECKING:
    from src.inline_domain.application.ctq.ports import CtqDataPort

logger = logging.getLogger(__name__)


@dataclass
class CtqReportViewModel:
    """Capability-free CTQ distribution report view model."""

    sheet_features_df: pd.DataFrame
    raw_measurements_df: pd.DataFrame
    indicators_df: pd.DataFrame
    sheet_oos_decoration_result: SheetOosDecorationResult | None = None


class CtqReportService:
    """Application service for CTQ Sheet and point distributions."""

    @staticmethod
    def _empty_payload() -> dict[str, object]:
        return {
            "sheet_features_df": pd.DataFrame(),
            "raw_measurements_df": pd.DataFrame(),
            "indicators_df": pd.DataFrame(),
            "sheet_oos_decoration": None,
        }

    @staticmethod
    def _view_model_from_payload(payload: dict[str, object]) -> CtqReportViewModel:
        sheet_features_df = payload.get("sheet_features_df")
        raw_measurements_df = payload.get("raw_measurements_df")
        indicators_df = payload.get("indicators_df")
        decoration_payload = payload.get("sheet_oos_decoration")

        sheet_features_df = (
            sheet_features_df if isinstance(sheet_features_df, pd.DataFrame) else pd.DataFrame()
        )
        raw_measurements_df = (
            raw_measurements_df if isinstance(raw_measurements_df, pd.DataFrame) else pd.DataFrame()
        )
        indicators_df = indicators_df if isinstance(indicators_df, pd.DataFrame) else pd.DataFrame()

        decoration_result = None
        if isinstance(decoration_payload, dict):
            decoration_df = decoration_payload.get("decoration_df")
            decision_df = decoration_payload.get("decision_df")
            decoration_result = SheetOosDecorationResult(
                raw_measurements_df=raw_measurements_df,
                decoration_df=(
                    decoration_df if isinstance(decoration_df, pd.DataFrame) else pd.DataFrame()
                ),
                decoration_path=Path(str(decoration_payload.get("decoration_path", ""))),
                decoration_sheet=str(decoration_payload.get("decoration_sheet", "Sheet1")),
                decision_sheet=str(decoration_payload.get("decision_sheet", "")),
                decision_df=decision_df if isinstance(decision_df, pd.DataFrame) else None,
                refresh_reason=str(decoration_payload.get("refresh_reason", "")),
            )

        return CtqReportViewModel(
            sheet_features_df=sheet_features_df,
            raw_measurements_df=raw_measurements_df,
            indicators_df=indicators_df,
            sheet_oos_decoration_result=decoration_result,
        )

    @staticmethod
    @st.cache_data(show_spinner=False, max_entries=1, ttl=4 * 60 * 60)
    def fetch_ctq_report_payload(
        _data_port: "CtqDataPort",
        query_config_json: str,
        snapshot_signature: str = "",
        product_revision: str = "",
        decision_signature: str = "",
    ) -> dict[str, object]:
        """Cache only reload-stable CTQ payload values.

        product_revision/decision_signature 进入缓存 key 并透传到共享管线门控。
        """
        try:
            query_config = SpcQueryConfig.model_validate_json(query_config_json)
            query_config.data_type_filter = "CTQ"
        except Exception as exc:
            logger.error("[CTQ] query config parse failed: %s", exc, exc_info=True)
            return CtqReportService._empty_payload()

        try:
            # 共享修饰+特征管线（scope='ctq'）：使用 ctq_sheet_oos_decoration.xlsx。
            features_payload = fetch_decorated_features(
                _features_source=_data_port,
                prod_code=query_config.prod_code,
                scope="ctq",
                start_date=query_config.start_date,
                end_date=query_config.end_date,
                snapshot_signature=snapshot_signature,
                product_revision=product_revision,
                decision_signature=decision_signature,
            )
            if features_payload["raw_measurements_df"].empty or features_payload["spec_empty"]:
                return CtqReportService._empty_payload()

            raw_measurements_df = assign_ctq_indicator_chart_type(
                features_payload["raw_measurements_df"]
            )
            sheet_features_df = assign_ctq_indicator_chart_type(features_payload["sheet_features_df"])
            if sheet_features_df.empty:
                return CtqReportService._empty_payload()

            indicators_df = (
                sheet_features_df[["prod_code", "factory", "step_id", "param_name"]]
                .drop_duplicates()
                .sort_values(["param_name", "step_id", "factory"])
                .reset_index(drop=True)
            )
            indicators_df = assign_ctq_indicator_chart_type(indicators_df)
            return {
                "sheet_features_df": sheet_features_df,
                "raw_measurements_df": raw_measurements_df,
                "indicators_df": indicators_df,
                "sheet_oos_decoration": features_payload["sheet_oos_decoration"],
            }
        except Exception as exc:
            logger.error("[CTQ] report generation failed: %s", exc, exc_info=True)
            return CtqReportService._empty_payload()

    @staticmethod
    def get_ctq_report_data(
        _data_port: "CtqDataPort",
        query_config_json: str,
        snapshot_signature: str = "",
        product_revision: str = "",
        decision_signature: str = "",
    ) -> CtqReportViewModel:
        """Build the CTQ ViewModel outside the Streamlit pickle boundary."""
        payload = CtqReportService.fetch_ctq_report_payload(
            _data_port=_data_port,
            query_config_json=query_config_json,
            snapshot_signature=snapshot_signature,
            product_revision=product_revision,
            decision_signature=decision_signature,
        )
        return CtqReportService._view_model_from_payload(payload)
