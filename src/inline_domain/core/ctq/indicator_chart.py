from __future__ import annotations

import pandas as pd

INDICATOR_CHART_TYPE_COLUMN = "chart_type"
INDICATOR_CHART_TYPE_BOX = "box"
INDICATOR_CHART_TYPE_LINE = "line"


def assign_ctq_indicator_chart_type(indicator_df: pd.DataFrame) -> pd.DataFrame:
    """Attach the backend-owned CTQ chart type for each parameter."""
    result = indicator_df.copy()
    if "param_name" not in result.columns:
        result[INDICATOR_CHART_TYPE_COLUMN] = INDICATOR_CHART_TYPE_BOX
        return result

    is_uniformity_parameter = result["param_name"].astype(str).str.contains(
        "UNI",
        case=False,
        regex=False,
    )
    result[INDICATOR_CHART_TYPE_COLUMN] = is_uniformity_parameter.map(
        {
            True: INDICATOR_CHART_TYPE_LINE,
            False: INDICATOR_CHART_TYPE_BOX,
        }
    )
    return result

