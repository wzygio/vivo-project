import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.spc_domain.core.cpm_calculator import build_period_capability_report
from src.spc_domain.core.spc_calculator import preprocess_sheet_features
from src.spc_domain.infrastructure.data_loader import SpcQueryConfig
from src.spc_domain.infrastructure.repositories.spc_repository import SpcRepository

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)


def resolve_period_capability_end_date(sheet_features_df: pd.DataFrame, query_end_date: object) -> date | None:
    """Resolve the M/W/D capability window end date from the latest available Sheet."""
    query_end_ts = pd.to_datetime(query_end_date, errors="coerce")
    if pd.isna(query_end_ts):
        return None

    if not sheet_features_df.empty and "sheet_start_time" in sheet_features_df.columns:
        latest_sheet_ts = pd.to_datetime(sheet_features_df["sheet_start_time"], errors="coerce").max()
        if pd.notna(latest_sheet_ts):
            return min(latest_sheet_ts.date(), query_end_ts.date())

    return query_end_ts.date()


@dataclass
class CpmReportViewModel:
    """CPM/CPK distribution report view model."""

    period_capability_df: pd.DataFrame
    sheet_features_df: pd.DataFrame
    raw_measurements_df: pd.DataFrame
    indicators_df: pd.DataFrame

    @property
    def lot_cpm_df(self) -> pd.DataFrame:
        """Backward-compatible alias for older page code."""
        return self.period_capability_df

    @property
    def sheet_measurements_df(self) -> pd.DataFrame:
        """Backward-compatible alias for older page code."""
        return self.sheet_features_df


class CpmReportService:
    """Application service for SPC-only Lot-level CPM reporting."""

    @staticmethod
    def _empty_view_model() -> CpmReportViewModel:
        return CpmReportViewModel(
            period_capability_df=pd.DataFrame(),
            sheet_features_df=pd.DataFrame(),
            raw_measurements_df=pd.DataFrame(),
            indicators_df=pd.DataFrame(),
        )

    @staticmethod
    @st.cache_data(show_spinner=False, max_entries=1)
    def get_cpm_report_data(
        _db_manager: "DatabaseManager",
        query_config_json: str,
        snapshot_signature: str = "",
    ) -> CpmReportViewModel:
        """Load SPC data and calculate M/W/D CPM/CPK distribution data."""
        try:
            query_config = SpcQueryConfig.model_validate_json(query_config_json)
            query_config.data_type_filter = "SPC"
        except Exception as e:
            logger.error("[CPM] query config parse failed: %s", e, exc_info=True)
            return CpmReportService._empty_view_model()

        try:
            snapshot_dir = Path("data") / query_config.prod_code
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            repo = SpcRepository(snapshot_dir=snapshot_dir, use_snapshot=True, db_manager=_db_manager)

            measurements_df = repo.get_spc_measurements(query_config)
            spec_df = repo.get_spc_spec_limits(query_config.prod_code)
            if measurements_df.empty or spec_df.empty:
                return CpmReportService._empty_view_model()

            if "sheet_start_time" in measurements_df.columns:
                measurements_df = measurements_df.copy()
                measurements_df["sheet_start_time"] = pd.to_datetime(
                    measurements_df["sheet_start_time"], errors="coerce"
                )
                start_dt = pd.to_datetime(query_config.start_date, errors="coerce")
                end_dt = pd.to_datetime(query_config.end_date, errors="coerce") + pd.Timedelta(days=1)
                measurements_df = measurements_df[
                    (measurements_df["sheet_start_time"] >= start_dt)
                    & (measurements_df["sheet_start_time"] < end_dt)
                ].copy()
                if measurements_df.empty:
                    return CpmReportService._empty_view_model()

            sheet_features_df = preprocess_sheet_features(measure_df=measurements_df, spec_df=spec_df)
            if sheet_features_df.empty:
                return CpmReportService._empty_view_model()

            capability_end_date = resolve_period_capability_end_date(sheet_features_df, query_config.end_date)
            if capability_end_date is None:
                return CpmReportService._empty_view_model()

            period_capability_df = build_period_capability_report(
                sheet_features=sheet_features_df,
                end_date=capability_end_date,
            )
            indicators_df = (
                sheet_features_df[["prod_code", "factory", "step_id", "param_name"]]
                .drop_duplicates()
                .sort_values(["param_name", "step_id", "factory"])
                .reset_index(drop=True)
                if not sheet_features_df.empty
                else pd.DataFrame(columns=["prod_code", "factory", "step_id", "param_name"])
            )

            return CpmReportViewModel(
                period_capability_df=period_capability_df,
                sheet_features_df=sheet_features_df,
                raw_measurements_df=measurements_df,
                indicators_df=indicators_df,
            )
        except Exception as e:
            logger.error("[CPM] report generation failed: %s", e, exc_info=True)
            return CpmReportService._empty_view_model()
