import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.inline_domain.core.spc.spc_calculator import (
    PERIOD_SIGMA_SOURCE_POINT_VALUE,
    build_period_capability_report,
    normalize_period_sigma_source,
)
from src.inline_domain.core.spc.spc_sheet_oos_decoration import SheetOosDecorationResult
from src.inline_domain.core.spc.cpk_decoration import CpkDecorationResult, prepare_cpk_decoration
from src.inline_domain.application.spc.spc_data_decoration import (
    prepare_decorated_spc_data,
    resolve_product_resource_dir,
)
from src.shared_kernel.config import ConfigLoader
from src.inline_domain.infrastructure.spc.data_loader import SpcQueryConfig
from src.inline_domain.infrastructure.spc.repositories.spc_repository import SpcRepository

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)

INDICATOR_CHART_TYPE_COLUMN = "chart_type"
INDICATOR_CHART_TYPE_BOX = "box"
INDICATOR_CHART_TYPE_LINE = "line"
CPM_CPK_EXCLUDED_PARAMETER_TOKEN = "PPA"


def assign_indicator_chart_type(indicator_df: pd.DataFrame) -> pd.DataFrame:
    """Attach the backend-owned chart type for each monitoring parameter."""
    result = indicator_df.copy()
    if "param_name" not in result.columns:
        result[INDICATOR_CHART_TYPE_COLUMN] = INDICATOR_CHART_TYPE_BOX
        return result

    is_uniformity_parameter = result["param_name"].astype(str).str.contains(
        "UNI", case=False, regex=False
    )
    result[INDICATOR_CHART_TYPE_COLUMN] = is_uniformity_parameter.map(
        {
            True: INDICATOR_CHART_TYPE_LINE,
            False: INDICATOR_CHART_TYPE_BOX,
        }
    )
    return result


def exclude_cpm_cpk_parameters(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Exclude business-rule parameters from CPM/CPK inputs without affecting chart data."""
    if dataframe.empty or "param_name" not in dataframe.columns:
        return dataframe.copy()

    is_excluded_parameter = dataframe["param_name"].astype(str).str.contains(
        CPM_CPK_EXCLUDED_PARAMETER_TOKEN,
        case=False,
        regex=False,
    )
    return dataframe.loc[~is_excluded_parameter].copy()


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
class SpcReportViewModel:
    """CPM/CPK distribution report view model."""

    period_capability_df: pd.DataFrame
    sheet_features_df: pd.DataFrame
    raw_measurements_df: pd.DataFrame
    indicators_df: pd.DataFrame
    sheet_oos_decoration_result: SheetOosDecorationResult | None = None
    cpk_decoration_result: CpkDecorationResult | None = None

    @property
    def lot_cpm_df(self) -> pd.DataFrame:
        """Backward-compatible alias for older page code."""
        return self.period_capability_df

    @property
    def sheet_measurements_df(self) -> pd.DataFrame:
        """Backward-compatible alias for older page code."""
        return self.sheet_features_df


class SpcReportService:
    """Application service for SPC-only Lot-level CPM reporting."""

    @staticmethod
    def _empty_payload() -> dict[str, object]:
        return {
            "period_capability_df": pd.DataFrame(),
            "sheet_features_df": pd.DataFrame(),
            "raw_measurements_df": pd.DataFrame(),
            "indicators_df": pd.DataFrame(),
            "sheet_oos_decoration": None,
            "cpk_decoration": None,
        }

    @staticmethod
    def _view_model_from_payload(payload: dict[str, object]) -> SpcReportViewModel:
        period_capability_df = payload.get("period_capability_df")
        sheet_features_df = payload.get("sheet_features_df")
        raw_measurements_df = payload.get("raw_measurements_df")
        indicators_df = payload.get("indicators_df")
        decoration_payload = payload.get("sheet_oos_decoration")
        cpk_decoration_payload = payload.get("cpk_decoration")

        period_capability_df = (
            period_capability_df if isinstance(period_capability_df, pd.DataFrame) else pd.DataFrame()
        )
        sheet_features_df = sheet_features_df if isinstance(sheet_features_df, pd.DataFrame) else pd.DataFrame()
        raw_measurements_df = (
            raw_measurements_df if isinstance(raw_measurements_df, pd.DataFrame) else pd.DataFrame()
        )
        indicators_df = indicators_df if isinstance(indicators_df, pd.DataFrame) else pd.DataFrame()

        decoration_result = None
        if isinstance(decoration_payload, dict):
            detail_df = decoration_payload.get("detail_df")
            decoration_df = decoration_payload.get("decoration_df")
            decoration_result = SheetOosDecorationResult(
                raw_measurements_df=raw_measurements_df,
                detail_df=detail_df if isinstance(detail_df, pd.DataFrame) else pd.DataFrame(),
                decoration_df=decoration_df if isinstance(decoration_df, pd.DataFrame) else pd.DataFrame(),
                detail_path=Path(str(decoration_payload.get("detail_path", ""))),
                decoration_path=Path(str(decoration_payload.get("decoration_path", ""))),
            )

        cpk_decoration_result = None
        if isinstance(cpk_decoration_payload, dict):
            detail_df = cpk_decoration_payload.get("detail_df")
            decoration_df = cpk_decoration_payload.get("decoration_df")
            cpk_decoration_result = CpkDecorationResult(
                period_capability_df=period_capability_df,
                detail_df=detail_df if isinstance(detail_df, pd.DataFrame) else pd.DataFrame(),
                decoration_df=decoration_df if isinstance(decoration_df, pd.DataFrame) else pd.DataFrame(),
                detail_path=Path(str(cpk_decoration_payload.get("detail_path", ""))),
                decoration_path=Path(str(cpk_decoration_payload.get("decoration_path", ""))),
            )

        return SpcReportViewModel(
            period_capability_df=period_capability_df,
            sheet_features_df=sheet_features_df,
            raw_measurements_df=raw_measurements_df,
            indicators_df=indicators_df,
            sheet_oos_decoration_result=decoration_result,
            cpk_decoration_result=cpk_decoration_result,
        )

    @staticmethod
    @st.cache_data(show_spinner=False, max_entries=3, ttl=4 * 60 * 60)
    def fetch_spc_report_payload(
        _db_manager: "DatabaseManager",
        query_config_json: str,
        snapshot_signature: str = "",
        period_sigma_source: str = "",
    ) -> dict[str, object]:
        """Cache only reload-stable CPM/CPK payload values.

        max_entries=3：缓存为进程级共享，多标签/多产品同时使用时避免互相驱逐
        导致每次 rerun 全量重建；ttl=4h：跨日日期窗口变化与"刷新缓存"换 key
        产生的孤儿条目由 TTL 兜底回收，内存有界。
        """
        try:
            query_config = SpcQueryConfig.model_validate_json(query_config_json)
            query_config.data_type_filter = "SPC"
        except Exception as e:
            logger.error("[CPM] query config parse failed: %s", e, exc_info=True)
            return SpcReportService._empty_payload()

        try:
            snapshot_dir = Path("data") / query_config.prod_code
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            repo = SpcRepository(snapshot_dir=snapshot_dir, use_snapshot=True, db_manager=_db_manager)

            measurements_df = repo.get_spc_measurements(query_config)
            spec_df = repo.get_spc_spec_limits(query_config.prod_code)
            if measurements_df.empty or spec_df.empty:
                return SpcReportService._empty_payload()

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
                    return SpcReportService._empty_payload()

            decorated_spc_data = prepare_decorated_spc_data(
                raw_measurements_df=measurements_df,
                spec_df=spec_df,
                prod_code=query_config.prod_code,
            )
            original_measurements_df = measurements_df.copy()
            measurements_df = assign_indicator_chart_type(decorated_spc_data.raw_measurements_df)
            sheet_features_df = assign_indicator_chart_type(decorated_spc_data.sheet_features_df)
            original_sheet_features_df = decorated_spc_data.original_sheet_features_df
            if sheet_features_df.empty:
                return SpcReportService._empty_payload()

            capability_sheet_features_df = exclude_cpm_cpk_parameters(sheet_features_df)
            capability_original_sheet_features_df = exclude_cpm_cpk_parameters(original_sheet_features_df)
            capability_measurements_df = exclude_cpm_cpk_parameters(measurements_df)
            capability_original_measurements_df = exclude_cpm_cpk_parameters(original_measurements_df)
            capability_end_date = resolve_period_capability_end_date(
                capability_sheet_features_df,
                query_config.end_date,
            )

            if capability_sheet_features_df.empty or capability_end_date is None:
                corrected_period_capability_df = pd.DataFrame()
                real_period_capability_df = pd.DataFrame()
            else:
                resolved_period_sigma_source = normalize_period_sigma_source(
                    period_sigma_source or ConfigLoader.get_spc_period_sigma_source()
                )
                corrected_period_capability_df = build_period_capability_report(
                    sheet_features=capability_sheet_features_df,
                    end_date=capability_end_date,
                    raw_measurements=capability_measurements_df
                    if resolved_period_sigma_source == PERIOD_SIGMA_SOURCE_POINT_VALUE
                    else None,
                    sigma_source=resolved_period_sigma_source,
                )
                real_period_capability_df = build_period_capability_report(
                    sheet_features=capability_original_sheet_features_df,
                    end_date=capability_end_date,
                    raw_measurements=capability_original_measurements_df
                    if resolved_period_sigma_source == PERIOD_SIGMA_SOURCE_POINT_VALUE
                    else None,
                    sigma_source=resolved_period_sigma_source,
                )
            cpk_decoration_result = prepare_cpk_decoration(
                real_period_capability_df=real_period_capability_df,
                corrected_period_capability_df=corrected_period_capability_df,
                product_dir=resolve_product_resource_dir(query_config.prod_code),
            )
            period_capability_df = cpk_decoration_result.period_capability_df
            period_capability_df = assign_indicator_chart_type(period_capability_df)
            indicators_df = (
                sheet_features_df[["prod_code", "factory", "step_id", "param_name"]]
                .drop_duplicates()
                .sort_values(["param_name", "step_id", "factory"])
                .reset_index(drop=True)
                if not sheet_features_df.empty
                else pd.DataFrame(columns=["prod_code", "factory", "step_id", "param_name"])
            )
            indicators_df = assign_indicator_chart_type(indicators_df)

            decoration_result = decorated_spc_data.sheet_oos_decoration_result
            return {
                "period_capability_df": period_capability_df,
                "sheet_features_df": sheet_features_df,
                "raw_measurements_df": measurements_df,
                "indicators_df": indicators_df,
                "sheet_oos_decoration": {
                    "detail_df": decoration_result.detail_df,
                    "decoration_df": decoration_result.decoration_df,
                    "detail_path": str(decoration_result.detail_path),
                    "decoration_path": str(decoration_result.decoration_path),
                },
                "cpk_decoration": {
                    "detail_df": cpk_decoration_result.detail_df,
                    "decoration_df": cpk_decoration_result.decoration_df,
                    "detail_path": str(cpk_decoration_result.detail_path),
                    "decoration_path": str(cpk_decoration_result.decoration_path),
                },
            }
        except Exception as e:
            logger.error("[CPM] report generation failed: %s", e, exc_info=True)
            return SpcReportService._empty_payload()

    @staticmethod
    def get_spc_report_data(
        _db_manager: "DatabaseManager",
        query_config_json: str,
        snapshot_signature: str = "",
        period_sigma_source: str = "",
    ) -> SpcReportViewModel:
        """Load cached CPM data and construct project ViewModels outside the pickle boundary."""
        payload = SpcReportService.fetch_spc_report_payload(
            _db_manager=_db_manager,
            query_config_json=query_config_json,
            snapshot_signature=snapshot_signature,
            period_sigma_source=period_sigma_source,
        )
        return SpcReportService._view_model_from_payload(payload)
