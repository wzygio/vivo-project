"""ECharts options for the Inline Top 10 station chart."""

from typing import Any

import pandas as pd

from app.utils.step_labels import format_step_label


def build_top_station_echarts_option(
    top_station_df: pd.DataFrame,
    *,
    is_scrap: bool,
    step_desc_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the stacked ECharts chart for ranked stations."""
    x_data = [format_step_label(step, step_desc_map) for step in top_station_df['step_id'].tolist()]

    option = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "legend": {"data": ["报废片数"] if is_scrap else ["OOC片数", "SOOS片数", "OOS片数"], "bottom": 0},
        "grid": {"left": "3%", "right": "3%", "bottom": "15%", "containLabel": True},
        "xAxis": {
            "type": "category",
            "data": x_data,
            "axisLabel": {"interval": 0, "fontWeight": "bold"}
        },
        "yAxis": {"type": "value", "name": "报废总片数" if is_scrap else "报警总片数"},
        "series": []
    }

    if 'OOC片数' in top_station_df.columns:
        option["series"].append({
            "name": "报废片数" if is_scrap else "OOC片数", "type": "bar", "stack": "总量", "barMaxWidth": 80,
            "itemStyle": {"color": "#F9D976"},
            "data": top_station_df['OOC片数'].tolist()
        })
    if not is_scrap and 'SOOS片数' in top_station_df.columns:
        option["series"].append({
            "name": "SOOS片数", "type": "bar", "stack": "总量", "barMaxWidth": 80,
            "itemStyle": {"color": "#81D8D0"},
            "data": top_station_df['SOOS片数'].tolist()
        })
    if not is_scrap and 'OOS片数' in top_station_df.columns:
        option["series"].append({
            "name": "OOS片数", "type": "bar", "stack": "总量", "barMaxWidth": 80,
            "itemStyle": {"color": "#7B9CE1"},
            "data": top_station_df['OOS片数'].tolist()
        })
    return option
