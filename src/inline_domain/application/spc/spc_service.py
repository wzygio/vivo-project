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
from src.inline_domain.core.shared.sheet_oos_decoration import (
    SheetOosDecorationReadError,
    SheetOosDecorationResult,
)
from src.inline_domain.core.spc.cpk_decoration import CpkDecorationResult, prepare_cpk_decoration
from src.inline_domain.application.shared.decorated_features import (
    fetch_decorated_features,
)
from src.inline_domain.application.shared.decorated_data import (
    resolve_product_resource_dir,
)
from src.shared_kernel.config import ConfigLoader
from src.inline_domain.application.spc.dtos import SpcQueryConfig

if TYPE_CHECKING:
    from src.inline_domain.application.spc.ports import SpcDataPort

logger = logging.getLogger(__name__)

INDICATOR_CHART_TYPE_COLUMN = "chart_type"
INDICATOR_CHART_TYPE_BOX = "box"
INDICATOR_CHART_TYPE_LINE = "line"
CPM_CPK_EXCLUDED_PARAMETER_TOKEN = "PPA"


class SpcDecorationFileError(RuntimeError):
    """Raised when the SPC decoration workbook cannot be read safely."""


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
            decoration_df = decoration_payload.get("decoration_df")
            decision_df = decoration_payload.get("decision_df")
            decoration_result = SheetOosDecorationResult(
                raw_measurements_df=raw_measurements_df,
                decoration_df=decoration_df if isinstance(decoration_df, pd.DataFrame) else pd.DataFrame(),
                decoration_path=Path(str(decoration_payload.get("decoration_path", ""))),
                decoration_sheet=str(decoration_payload.get("decoration_sheet", "Sheet1")),
                decision_sheet=str(decoration_payload.get("decision_sheet", "")),
                decision_df=decision_df if isinstance(decision_df, pd.DataFrame) else None,
                refresh_reason=str(decoration_payload.get("refresh_reason", "")),
            )

        cpk_decoration_result = None
        if isinstance(cpk_decoration_payload, dict):
            decoration_df = cpk_decoration_payload.get("decoration_df")
            cpk_decoration_result = CpkDecorationResult(
                period_capability_df=period_capability_df,
                decoration_df=decoration_df if isinstance(decoration_df, pd.DataFrame) else pd.DataFrame(),
                decoration_path=Path(str(cpk_decoration_payload.get("decoration_path", ""))),
                decoration_sheet=str(cpk_decoration_payload.get("decoration_sheet", "Sheet1")),
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
        _data_port: "SpcDataPort",
        query_config_json: str,
        snapshot_signature: str = "",
        period_sigma_source: str = "",
        product_revision: str = "",
        decision_signature: str = "",
    ) -> dict[str, object]:
        """Cache only reload-stable CPM/CPK payload values.

        max_entries=3：缓存为进程级共享，多标签/多产品同时使用时避免互相驱逐
        导致每次 rerun 全量重建；ttl=4h：跨日日期窗口变化与"刷新缓存"换 key
        产生的孤儿条目由 TTL 兜底回收，内存有界。
        product_revision/decision_signature 进入缓存 key 并透传到共享管线门控。
        """
        try:
            query_config = SpcQueryConfig.model_validate_json(query_config_json)
            query_config.data_type_filter = "SPC"
        except Exception as e:
            logger.error("[CPM] query config parse failed: %s", e, exc_info=True)
            return SpcReportService._empty_payload()

        try:
            # 共享修饰+特征管线（scope='spc'）：缓存 key 含产品/窗口/签名，
            # 与 monitor 的 SPC 分组在窗口一致时命中同一条目。
            features_payload = fetch_decorated_features(
                _features_source=_data_port,
                prod_code=query_config.prod_code,
                scope="spc",
                start_date=query_config.start_date,
                end_date=query_config.end_date,
                snapshot_signature=snapshot_signature,
                product_revision=product_revision,
                decision_signature=decision_signature,
            )
            if features_payload["raw_measurements_df"].empty or features_payload["spec_empty"]:
                return SpcReportService._empty_payload()

            measurements_df = assign_indicator_chart_type(features_payload["raw_measurements_df"])
            sheet_features_df = assign_indicator_chart_type(features_payload["sheet_features_df"])
            if sheet_features_df.empty:
                return SpcReportService._empty_payload()

            capability_sheet_features_df = exclude_cpm_cpk_parameters(sheet_features_df)
            capability_measurements_df = exclude_cpm_cpk_parameters(measurements_df)
            capability_end_date = resolve_period_capability_end_date(
                capability_sheet_features_df,
                query_config.end_date,
            )

            if capability_sheet_features_df.empty or capability_end_date is None:
                period_capability_df = pd.DataFrame()
            else:
                resolved_period_sigma_source = normalize_period_sigma_source(
                    period_sigma_source or ConfigLoader.get_spc_period_sigma_source()
                )
                # CPK 仅基于修饰后的点位/特征计算，不再保留真实值口径
                period_capability_df = build_period_capability_report(
                    sheet_features=capability_sheet_features_df,
                    end_date=capability_end_date,
                    raw_measurements=capability_measurements_df
                    if resolved_period_sigma_source == PERIOD_SIGMA_SOURCE_POINT_VALUE
                    else None,
                    sigma_source=resolved_period_sigma_source,
                )
            cpk_decoration_result = prepare_cpk_decoration(
                period_capability_df=period_capability_df,
                product_dir=resolve_product_resource_dir(query_config.prod_code),
                sheet_name=query_config.prod_code,
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

            return {
                "period_capability_df": period_capability_df,
                "sheet_features_df": sheet_features_df,
                "raw_measurements_df": measurements_df,
                "indicators_df": indicators_df,
                "sheet_oos_decoration": features_payload["sheet_oos_decoration"],
                "cpk_decoration": {
                    "decoration_df": cpk_decoration_result.decoration_df,
                    "decoration_path": str(cpk_decoration_result.decoration_path),
                    "decoration_sheet": cpk_decoration_result.decoration_sheet,
                },
            }
        except SheetOosDecorationReadError as exc:
            logger.error(
                "[SPC] sheet OOS decoration workbook read failed: %s",
                exc,
                exc_info=True,
            )
            raise SpcDecorationFileError(
                "SPC sheet OOS decoration workbook could not be read."
            ) from exc
        except Exception as e:
            logger.error("[CPM] report generation failed: %s", e, exc_info=True)
            return SpcReportService._empty_payload()

    @staticmethod
    def get_spc_report_data(
        _data_port: "SpcDataPort",
        query_config_json: str,
        snapshot_signature: str = "",
        period_sigma_source: str = "",
        product_revision: str = "",
        decision_signature: str = "",
    ) -> SpcReportViewModel:
        """Load cached CPM data and construct project ViewModels outside the pickle boundary."""
        payload = SpcReportService.fetch_spc_report_payload(
            _data_port=_data_port,
            query_config_json=query_config_json,
            snapshot_signature=snapshot_signature,
            period_sigma_source=period_sigma_source,
            product_revision=product_revision,
            decision_signature=decision_signature,
        )
        return SpcReportService._view_model_from_payload(payload)
