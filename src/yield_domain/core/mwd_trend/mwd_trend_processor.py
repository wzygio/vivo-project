"""Public facade for monthly/weekly/daily yield-loss trend processing.

指定良损驱动（数据修饰简化版）：

- Code 级日度由 `daily_generator.generate_daily_counts` 按"入库良率修饰表"
  解析出的 `modifier_targets`（{defect_desc: {月份: 目标良损}}）确定性生成；
  修饰目标缺失时回退原始月度良损，不回落原始日度不良数。
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
            target_defects = sorted(
                panel_details_df["defect_group"].dropna().unique().tolist()
            )
            group_daily = _build_group_daily_from_code_data(
                mwd_code_data.get("daily_full"),
                target_defects,
            )
            if group_daily.empty:
                return _format_group_results(
                    pd.DataFrame(), pd.DataFrame(), group_daily, target_defects
                )
            last_day = (
                pd.to_datetime(target_end_date)
                if target_end_date is not None
                else group_daily.index.max()
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
            raw_daily, last_day, raw_monthly_targets = _prepare_code_raw_data(
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
                raw_monthly_targets=raw_monthly_targets,
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
    code_daily: pd.DataFrame | None,
    target_defects: list[str],
) -> pd.DataFrame:
    """从最终 Code 日度复用日期/投入，并汇总出 Group 日度宽表。"""
    if code_daily is None or code_daily.empty:
        return pd.DataFrame(columns=["total_panels", *target_defects])

    source = code_daily[
        ["time_period", "total_panels", "defect_group", "defect_panel_count"]
    ].copy()
    source["warehousing_time"] = pd.to_datetime(
        source["time_period"],
        errors="coerce",
    )
    source = source.dropna(subset=["warehousing_time"])
    if source.empty:
        return pd.DataFrame(columns=["total_panels", *target_defects])

    daily_totals = source.groupby("warehousing_time", sort=True, observed=True)[
        "total_panels"
    ].first()
    group_counts = (
        source.groupby(
            ["warehousing_time", "defect_group"],
            sort=True,
            observed=True,
        )["defect_panel_count"]
        .sum()
        .unstack("defect_group", fill_value=0)
        .reindex(index=daily_totals.index, columns=target_defects, fill_value=0)
    )
    result = pd.concat(
        [daily_totals.rename("total_panels"), group_counts], axis=1
    )
    result[target_defects] = result[target_defects].fillna(0).astype(int)
    result.index.name = "warehousing_time"
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
