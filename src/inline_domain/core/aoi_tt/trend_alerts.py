"""TT adapter for AOI density rules, with independent particle-size series."""
from __future__ import annotations

from datetime import date

import pandas as pd

from src.inline_domain.core.shared.density_trend_alerts import (
    DENSITY_TREND_POLICY_VERSION,
    EVALUATION_VALUE_COLUMNS,
    HISTORY_LOOKBACK_DAYS,
    evaluate_density_trends,
)

AOI_TT_TREND_POLICY_VERSION = DENSITY_TREND_POLICY_VERSION
INDICATOR_KEYS = ["factory", "step_id", "tt_name", "particle_size"]
EVALUATION_COLUMNS = ["prod_code", *INDICATOR_KEYS, *EVALUATION_VALUE_COLUMNS]


def evaluate_aoi_tt_trends(
    trend_df: pd.DataFrame, indicators_df: pd.DataFrame, *, reference_date: date,
) -> pd.DataFrame:
    return evaluate_density_trends(
        trend_df, indicators_df, reference_date=reference_date, indicator_keys=INDICATOR_KEYS,
    )
