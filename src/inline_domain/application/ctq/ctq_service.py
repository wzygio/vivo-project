from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.inline_domain.application.ctq.ctq_data_decoration import prepare_decorated_ctq_data
from src.inline_domain.core.ctq.indicator_chart import assign_ctq_indicator_chart_type
from src.inline_domain.core.spc.spc_sheet_oos_decoration import SheetOosDecorationResult
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
            decoration_result = SheetOosDecorationResult(
                raw_measurements_df=raw_measurements_df,
                decoration_df=(
                    decoration_df if isinstance(decoration_df, pd.DataFrame) else pd.DataFrame()
                ),
                decoration_path=Path(str(decoration_payload.get("decoration_path", ""))),
                decoration_sheet=str(decoration_payload.get("decoration_sheet", "Sheet1")),
            )

        return CtqReportViewModel(
            sheet_features_df=sheet_features_df,
            raw_measurements_df=raw_measurements_df,
            indicators_df=indicators_df,
            sheet_oos_decoration_result=decoration_result,
        )

    @staticmethod
    @st.cache_data(show_spinner=False, max_entries=1)
    def fetch_ctq_report_payload(
        _data_port: "CtqDataPort",
        query_config_json: str,
        snapshot_signature: str = "",
    ) -> dict[str, object]:
        """Cache only reload-stable CTQ payload values."""
        try:
            query_config = SpcQueryConfig.model_validate_json(query_config_json)
            query_config.data_type_filter = "CTQ"
        except Exception as exc:
            logger.error("[CTQ] query config parse failed: %s", exc, exc_info=True)
            return CtqReportService._empty_payload()

        try:
            measurements_df = _data_port.get_spc_measurements(query_config)
            spec_df = _data_port.get_spc_spec_limits(query_config.prod_code)
            if measurements_df.empty or spec_df.empty:
                return CtqReportService._empty_payload()

            if "sheet_start_time" in measurements_df.columns:
                measurements_df = measurements_df.copy()
                measurements_df["sheet_start_time"] = pd.to_datetime(
                    measurements_df["sheet_start_time"],
                    errors="coerce",
                )
                start_dt = pd.to_datetime(query_config.start_date, errors="coerce")
                end_dt = pd.to_datetime(query_config.end_date, errors="coerce") + pd.Timedelta(days=1)
                measurements_df = measurements_df[
                    (measurements_df["sheet_start_time"] >= start_dt)
                    & (measurements_df["sheet_start_time"] < end_dt)
                ].copy()
                if measurements_df.empty:
                    return CtqReportService._empty_payload()

            decorated_data = prepare_decorated_ctq_data(
                raw_measurements_df=measurements_df,
                spec_df=spec_df,
                prod_code=query_config.prod_code,
            )
            raw_measurements_df = assign_ctq_indicator_chart_type(
                decorated_data.raw_measurements_df
            )
            sheet_features_df = assign_ctq_indicator_chart_type(decorated_data.sheet_features_df)
            if sheet_features_df.empty:
                return CtqReportService._empty_payload()

            indicators_df = (
                sheet_features_df[["prod_code", "factory", "step_id", "param_name"]]
                .drop_duplicates()
                .sort_values(["param_name", "step_id", "factory"])
                .reset_index(drop=True)
            )
            indicators_df = assign_ctq_indicator_chart_type(indicators_df)
            decoration_result = decorated_data.sheet_oos_decoration_result
            return {
                "sheet_features_df": sheet_features_df,
                "raw_measurements_df": raw_measurements_df,
                "indicators_df": indicators_df,
                "sheet_oos_decoration": {
                    "decoration_df": decoration_result.decoration_df,
                    "decoration_path": str(decoration_result.decoration_path),
                    "decoration_sheet": decoration_result.decoration_sheet,
                },
            }
        except Exception as exc:
            logger.error("[CTQ] report generation failed: %s", exc, exc_info=True)
            return CtqReportService._empty_payload()

    @staticmethod
    def get_ctq_report_data(
        _data_port: "CtqDataPort",
        query_config_json: str,
        snapshot_signature: str = "",
    ) -> CtqReportViewModel:
        """Build the CTQ ViewModel outside the Streamlit pickle boundary."""
        payload = CtqReportService.fetch_ctq_report_payload(
            _data_port=_data_port,
            query_config_json=query_config_json,
            snapshot_signature=snapshot_signature,
        )
        return CtqReportService._view_model_from_payload(payload)
