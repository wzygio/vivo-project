"""Public facade for monthly/weekly/daily yield-loss trend processing.

指定良损驱动（数据修饰简化版）：

- Code 级日度由 `daily_generator.generate_daily_counts` 按"入库良率修饰表"
  解析出的 `modifier_targets`（{defect_desc: {月份: 目标良损}}）确定性生成；
  未指定的缺陷保持原始日度不良数。
- Group 级日度由 Code 最终日度按 Group 汇总；Group Sheet 的人工指定良损只覆写
  最终月度结果，不反向生成日度数据。
- 周度由最终日度直接聚合；月度先由日度聚合，再应用 Group Sheet 的月度覆写。
"""

from __future__ import annotations

import logging
from datetime import datetime as dt
from typing import Dict

import pandas as pd

from src.shared_kernel.config_model import AppConfig
from yield_domain.core.mwd_trend.aggregation import (
    safe_trend_aggregator as _safe_trend_aggregator,
)
from yield_domain.core.mwd_trend.daily_generator import (
    generate_daily_counts,
)
from yield_domain.core.mwd_trend.data_preparation import (
    pad_daily_data_to_end as _pad_daily_data_to_today,
    prepare_code_raw_data as _prepare_code_raw_data,
    prepare_group_raw_data as _prepare_group_raw_data,
)
from yield_domain.core.mwd_trend.formatting import (
    format_code_results as _format_code_results,
    format_group_results as _format_group_results,
)


class MWDTrendProcessor:
    """Facade used by the Yield application service."""

    @staticmethod
    def create_mwd_trend_data(
        panel_details_df: pd.DataFrame,
        config: AppConfig,
        mwd_code_data: Dict[str, pd.DataFrame] | None = None,
        modifier_targets: Dict[str, Dict[str, float]] | None = None,
        target_end_date: dt | None = None,
    ) -> Dict[str, pd.DataFrame] | None:
        """Create Group daily/weekly from Code data, then override monthly."""
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
            group_daily = _build_group_daily_from_code_data(
                padded_daily,
                mwd_code_data.get("daily_full"),
                target_defects,
            )
            monthly = _safe_trend_aggregator(
                group_daily, last_day, "M", is_group_level=True
            )
            monthly = _apply_group_monthly_overrides(
                monthly,
                modifier_targets or {},
                target_defects,
            )
            weekly = _safe_trend_aggregator(
                group_daily, last_day, "W", is_group_level=True
            )
            return _format_group_results(monthly, weekly, group_daily, target_defects)
        except Exception as error:
            logging.error("Group 趋势分析出错: %s", error, exc_info=True)
            return None

    @staticmethod
    def create_code_level_mwd_trend_data(
        panel_details_df: pd.DataFrame,
        config: AppConfig,
        modifier_targets: Dict[str, Dict[str, float]] | None = None,
        volatility: float = 0.3,
        target_end_date: dt | None = None,
    ) -> Dict[str, pd.DataFrame] | None:
        """Create Code trends driven by the specified monthly loss rates."""
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
            daily = generate_daily_counts(
                padded_daily,
                modifier_targets or {},
                product_code=config.data_source.product_code,
                volatility=volatility,
            )
            monthly = _safe_trend_aggregator(
                daily, last_day, "M", is_group_level=False
            )
            weekly = _safe_trend_aggregator(
                daily, last_day, "W", is_group_level=False
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
    """Aggregate final Code daily counts into a Group-wide daily table."""
    result = daily_skeleton[["total_panels"]].copy()
    for group in target_defects:
        result[group] = 0
    if code_daily is None or code_daily.empty:
        return result

    source = code_daily.copy()
    source["warehousing_time"] = pd.to_datetime(
        source["time_period"],
        errors="coerce",
    )
    counts = source.groupby(["warehousing_time", "defect_group"])[
        "defect_panel_count"
    ].sum()
    available_groups = set(counts.index.get_level_values("defect_group"))
    for group in target_defects:
        if group not in available_groups:
            continue
        group_counts = counts.xs(group, level="defect_group", drop_level=True)
        result[group] = result.index.map(group_counts).fillna(0).astype(int)
    return result


def _apply_group_monthly_overrides(
    monthly: pd.DataFrame,
    monthly_targets: Dict[str, Dict[str, float]],
    target_defects: list[str],
) -> pd.DataFrame:
    """Override final Group monthly counts without changing daily/weekly data."""
    if monthly.empty or not monthly_targets:
        return monthly.copy()

    result = monthly.copy()
    month_keys = pd.to_datetime(result.index).to_period("M").astype(str)
    for group in target_defects:
        if group not in result or group not in monthly_targets:
            continue
        for month, rate in monthly_targets[group].items():
            mask = month_keys == month
            if not mask.any():
                continue
            capacities = pd.to_numeric(
                result.loc[mask, "total_panels"],
                errors="coerce",
            ).fillna(0).clip(lower=0)
            target_counts = (capacities * float(rate)).round().clip(
                lower=0,
                upper=capacities,
            )
            result.loc[mask, group] = target_counts.astype(int)
    return result


__all__ = [
    "MWDTrendProcessor",
]
