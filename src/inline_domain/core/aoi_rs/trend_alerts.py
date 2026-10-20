"""RS adapter for the shared AOI density-trend rules."""
from __future__ import annotations

from datetime import date
import pandas as pd

from src.inline_domain.core.shared.density_trend_alerts import (
    BASELINE_DAYS, BASELINE_LAG_DAYS, DENSITY_TREND_POLICY_VERSION,
    EVALUATION_VALUE_COLUMNS, HISTORY_LOOKBACK_DAYS, LOOKBACK_DAYS, RISE_FACTOR,
    evaluate_density_trends,
)

AOI_RS_TREND_POLICY_VERSION = DENSITY_TREND_POLICY_VERSION
INDICATOR_KEYS = ["factory", "step_id", "rs_code"]
EVALUATION_COLUMNS = ["prod_code", *INDICATOR_KEYS, *EVALUATION_VALUE_COLUMNS]


def evaluate_aoi_rs_trends(
    trend_df: pd.DataFrame, indicators_df: pd.DataFrame, *, reference_date: date,
) -> pd.DataFrame:
    """Keep the RS public schema while sharing calendar and threshold rules."""
    return evaluate_density_trends(
        trend_df, indicators_df, reference_date=reference_date, indicator_keys=INDICATOR_KEYS,
    )
