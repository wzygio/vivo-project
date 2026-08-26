"""Public facade for monthly/weekly/daily yield-loss trend processing.

指定良损驱动（数据修饰简化版）：

- Code 级日度由 `daily_generator.generate_daily_counts` 按"入库良率修饰表"
  解析出的 `modifier_targets`（{defect_desc: {月份: 目标良损}}）确定性生成；
  未指定的缺陷保持原始日度不良数。
- Group 级由 Group Sheet 的人工指定良损独立生成，人工指定优先。
- 周度/月度不再有任何人工覆盖，由最终日度经 `aggregation.safe_trend_aggregator`
  直接聚合。
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
    generate_group_daily_counts,
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
        modifier_targets: Dict[str, Dict[str, float]] | None = None,
        volatility: float = 0.3,
        target_end_date: dt | None = None,
    ) -> Dict[str, pd.DataFrame] | None:
        """Create Group trends driven by Group Sheet monthly loss rates."""
        logging.info("开始 Group 月/周/日趋势处理")
        if panel_details_df.empty:
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
            group_daily = generate_group_daily_counts(
                padded_daily,
                modifier_targets or {},
                product_code=config.data_source.product_code,
                volatility=volatility,
            )
            monthly = _safe_trend_aggregator(
                group_daily, last_day, "M", is_group_level=True
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

__all__ = [
    "MWDTrendProcessor",
]
