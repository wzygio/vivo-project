"""TT adapter for AOI density rules, evaluating Total only."""
from __future__ import annotations

from datetime import date

import pandas as pd

from src.inline_domain.core.shared.density_trend_alerts import (
    DENSITY_TREND_POLICY_VERSION,
    EVALUATION_VALUE_COLUMNS,
    HISTORY_LOOKBACK_DAYS,
    evaluate_density_trends,
)

AOI_TT_TREND_POLICY_VERSION = f"{DENSITY_TREND_POLICY_VERSION}:tt-total-only-v1"
INDICATOR_KEYS = ["factory", "step_id", "tt_name"]
EVALUATION_COLUMNS = ["prod_code", *INDICATOR_KEYS, *EVALUATION_VALUE_COLUMNS]


def evaluate_aoi_tt_trends(
    trend_df: pd.DataFrame, indicators_df: pd.DataFrame, *, reference_date: date,
) -> pd.DataFrame:
    total_trend = trend_df.loc[trend_df["particle_size"].eq("Total")] if not trend_df.empty else trend_df
    total_indicators = (
        indicators_df.loc[indicators_df["particle_size"].eq("Total")]
        if "particle_size" in indicators_df.columns else indicators_df
    )
    return evaluate_density_trends(
        total_trend, total_indicators, reference_date=reference_date, indicator_keys=INDICATOR_KEYS,
    )
