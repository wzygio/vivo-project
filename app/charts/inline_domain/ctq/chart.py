"""Figure builders for CTQ charts."""

import pandas as pd
import plotly.graph_objects as go

from app.charts.inline_domain.sheet_charts import create_period_overview_chart


def create_ctq_period_overview_chart(
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    title: str,
    period_box_source: str = "point_value",
) -> go.Figure:
    """Create a capability-free CTQ month/week/day distribution figure."""
    return create_period_overview_chart(
        sheet_features_df=sheet_features_df,
        period_capability_df=pd.DataFrame(),
        raw_measurements_df=raw_measurements_df,
        period_box_source=period_box_source,
        title=title,
    )
