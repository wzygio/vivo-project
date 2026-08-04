"""Public facade for monthly/weekly/daily yield-loss trend processing.

The implementation is intentionally kept in focused modules. This file owns
only the public workflow: prepare data, run automatic daily calculation,
apply period overrides, and format the result for application consumers.
"""

from __future__ import annotations

import logging
from datetime import datetime as dt
from pathlib import Path
from typing import Dict

import pandas as pd

from src.shared_kernel.config_model import AppConfig
from yield_domain.core.mwd_trend.aggregation import (
    safe_trend_aggregator as _safe_trend_aggregator,
)
from yield_domain.core.mwd_trend.allocation import reconcile_code_daily_counts
from yield_domain.core.mwd_trend.code_baseline import (
    CODE_BASELINE_COLUMNS,
    ensure_code_baseline_current as _ensure_code_baseline_current,
    load_code_baseline_frame as _load_code_baseline_frame,
)
from yield_domain.core.mwd_trend.data_preparation import (
    pad_daily_data_to_end as _pad_daily_data_to_today,
    prepare_code_raw_data as _prepare_code_raw_data,
    prepare_group_raw_data as _prepare_group_raw_data,
)
from yield_domain.core.mwd_trend.ema import (
    calculate_code_ema_noise as _calculate_code_ema_noise_impl,
)
from yield_domain.core.mwd_trend.formatting import (
    format_code_results as _format_code_results,
    format_group_results as _format_group_results,
)
from yield_domain.core.mwd_trend.manual_overrides import (
    apply_code_daily_overrides,
    apply_code_manual_overrides_to_daily,
    apply_code_period_overrides,
    apply_group_daily_overrides,
    apply_group_period_overrides,
    rebuild_code_daily_from_weekly,
    rebuild_group_daily_from_weekly,
)
from yield_domain.core.mwd_trend.pipeline import run_manual_period_pipeline
from yield_domain.core.mwd_trend.trend_regulator import TrendRegulator


def _calc_code_ema_noise(
    raw_df: pd.DataFrame,
    span: int,
    scale: float,
    volatility: float,
    config: AppConfig | None = None,
    prod_code: str | None = None,
) -> pd.DataFrame:
    """Compatibility wrapper preserving monkeypatchable baseline hooks."""
    return _calculate_code_ema_noise_impl(
        raw_df,
        span,
        scale,
        volatility,
        config=config,
        prod_code=prod_code,
        ensure_baseline_current=_ensure_code_baseline_current,
        load_baseline_frame=_load_code_baseline_frame,
    )


class MWDTrendProcessor:
    """Facade used by the Yield application service."""

    @staticmethod
    def reconcile_code_daily_counts(
        daily_df: pd.DataFrame,
        raw_daily_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return reconcile_code_daily_counts(daily_df, raw_daily_df)

    @staticmethod
    def apply_code_manual_overrides_to_daily(
        daily_df: pd.DataFrame,
        monthly_values: dict,
        weekly_values: dict,
        daily_values: dict,
    ) -> pd.DataFrame:
        return apply_code_manual_overrides_to_daily(
            daily_df,
            monthly_values,
            weekly_values,
            daily_values,
        )

    @staticmethod
    def create_mwd_trend_data(
        panel_details_df: pd.DataFrame,
        mwd_code_data: Dict[str, pd.DataFrame] | None,
        config: AppConfig,
        scaling_factor: float,
        volatility: float = 0.1,
        target_end_date: dt | None = None,
    ) -> Dict[str, pd.DataFrame] | None:
        """Create Group trends from the already calculated Code daily source."""
        logging.info("开始 Group 月/周/日趋势处理")
        if panel_details_df.empty or not mwd_code_data:
            return None

        try:
            raw_daily, last_day, target_defects = _prepare_group_raw_data(
                panel_details_df,
                target_end_date,
            )
            padded_daily = _pad_daily_data_to_today(
                raw_daily,
                is_group_level=True,
                end_date=last_day,
            )
            automatic_daily = _build_group_daily_from_code_data(
                padded_daily,
                mwd_code_data.get("daily_full"),
                target_defects,
            )

            processing = config.processing
            monthly_values = processing.get("group_monthly_values", {})
            weekly_values = processing.get("group_weekly_values", {})
            daily_values = processing.get("group_daily_values", {})

            monthly, weekly, daily = run_manual_period_pipeline(
                automatic_daily=automatic_daily,
                last_day=last_day,
                aggregate_monthly=lambda data, anchor: _safe_trend_aggregator(
                    data, anchor, "M", is_group_level=True
                ),
                aggregate_weekly=lambda data, anchor: _safe_trend_aggregator(
                    data, anchor, "W", is_group_level=True
                ),
                apply_monthly_override=lambda data, values: apply_group_period_overrides(
                    data, values, "monthly", target_defects
                ),
                apply_weekly_override=lambda data, values: apply_group_period_overrides(
                    data, values, "weekly", target_defects
                ),
                rebuild_daily_from_weekly=lambda data, period_data, values: rebuild_group_daily_from_weekly(
                    data,
                    period_data,
                    values,
                    target_defects,
                    volatility,
                ),
                apply_daily_override=lambda data, values: apply_group_daily_overrides(
                    data, values, target_defects
                ),
                monthly_values=monthly_values,
                weekly_values=weekly_values,
                daily_values=daily_values,
            )
            return _format_group_results(monthly, weekly, daily, target_defects)
        except Exception as error:
            logging.error("Group 趋势分析出错: %s", error, exc_info=True)
            return None

    @staticmethod
    def create_code_level_mwd_trend_data(
        panel_details_df: pd.DataFrame,
        config: AppConfig,
        ema_span: int,
        scaling_factor: float,
        volatility: float = 0.1,
        warning_lines: dict | None = None,
        target_end_date: dt | None = None,
    ) -> Dict[str, pd.DataFrame] | None:
        """Create Code trends with explicit daily/weekly/monthly precedence."""
        logging.info("开始 Code 月/周/日趋势处理")
        if panel_details_df.empty:
            return None

        try:
            raw_daily, last_day = _prepare_code_raw_data(
                panel_details_df,
                target_end_date,
            )
            padded_daily = _pad_daily_data_to_today(
                raw_daily,
                is_group_level=False,
                end_date=last_day,
            )
            prod_code = config.data_source.product_code
            automatic_daily = _calc_code_ema_noise(
                padded_daily,
                ema_span,
                scaling_factor,
                volatility,
                config=config,
                prod_code=prod_code,
            )
            automatic_daily = TrendRegulator.regulate_code_daily_base(
                automatic_daily,
                warning_lines=warning_lines or {},
            )
            calibrated_daily = reconcile_code_daily_counts(automatic_daily, raw_daily)

            processing = config.processing
            monthly_values = processing.get("code_monthly_values", {})
            weekly_values = processing.get("code_weekly_values", {})
            daily_values = processing.get("code_daily_values", {})

            aggregate_monthly = lambda data, anchor: _safe_trend_aggregator(
                data, anchor, "M", is_group_level=False
            )
            aggregate_weekly = lambda data, anchor: _safe_trend_aggregator(
                data, anchor, "W", is_group_level=False
            )
            monthly, weekly, daily = run_manual_period_pipeline(
                automatic_daily=calibrated_daily,
                last_day=last_day,
                aggregate_monthly=aggregate_monthly,
                aggregate_weekly=aggregate_weekly,
                apply_monthly_override=lambda data, values: apply_code_period_overrides(
                    data, values, "monthly"
                ),
                apply_weekly_override=lambda data, values: apply_code_period_overrides(
                    data, values, "weekly"
                ),
                rebuild_daily_from_weekly=lambda data, period_data, values: rebuild_code_daily_from_weekly(
                    data,
                    period_data,
                    values,
                    volatility,
                ),
                apply_daily_override=apply_code_daily_overrides,
                monthly_values=monthly_values,
                weekly_values=weekly_values,
                daily_values=daily_values,
            )
            return _format_code_results(monthly, weekly, daily)
        except Exception as error:
            logging.error("Code 趋势分析出错: %s", error, exc_info=True)
            return None


def _build_group_daily_from_code_data(
    daily_skeleton: pd.DataFrame,
    code_daily: pd.DataFrame | None,
    target_defects: list[str],
) -> pd.DataFrame:
    """Convert formatted Code daily rows into a Group-wide daily table."""
    result = daily_skeleton[["total_panels"]].copy()
    for group in target_defects:
        result[group] = 0
    if code_daily is None or code_daily.empty:
        return result

    source = code_daily.copy()
    source["warehousing_time"] = pd.to_datetime(source["time_period"], errors="coerce")
    counts = source.groupby(["warehousing_time", "defect_group"])[
        "defect_panel_count"
    ].sum()
    for group in target_defects:
        group_counts = counts.xs(group, level="defect_group", drop_level=True) if group in counts.index.get_level_values("defect_group") else pd.Series(dtype=float)
        result[group] = result.index.map(group_counts).fillna(0).astype(int)
    return result


def create_mwd_trend_data(
    panel_details_df: pd.DataFrame,
    config: AppConfig,
    resource_dir: Path | None = None,
    ema_span: int = 7,
    scaling_factor: float = 1.0,
    volatility: float = 0.1,
    target_end_date: dt | None = None,
) -> Dict[str, pd.DataFrame] | None:
    """Legacy Code-to-Group entrypoint retained for service callers."""
    del resource_dir
    code_results = MWDTrendProcessor.create_code_level_mwd_trend_data(
        panel_details_df=panel_details_df,
        config=config,
        ema_span=ema_span,
        scaling_factor=scaling_factor,
        volatility=volatility,
        warning_lines={},
        target_end_date=target_end_date,
    )
    if code_results is None:
        return None
    return MWDTrendProcessor.create_mwd_trend_data(
        panel_details_df=panel_details_df,
        mwd_code_data=code_results,
        config=config,
        scaling_factor=scaling_factor,
        volatility=volatility,
        target_end_date=target_end_date,
    )


__all__ = [
    "MWDTrendProcessor",
    "create_mwd_trend_data",
]
