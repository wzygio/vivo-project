"""Chart figures and series for Inline OOS history."""

from collections.abc import Mapping, Sequence

import altair as alt
import pandas as pd


def build_period_trend_chart(
    summary: pd.DataFrame, *, label_column: str = "报警类型",
    metric_rows: Sequence[str] = ("OOS报警片数", "OOC报警片数", "SOOS报警片数"),
) -> alt.Chart:
    """Keep every summary period, color by granularity, retain alarm tooltips."""
    trend = summary.set_index(label_column).reindex(
        list(metric_rows)
    ).T.apply(pd.to_numeric, errors="coerce").fillna(0)
    trend.index.name = "周期"
    periods = trend.index.tolist()
    data = trend.reset_index().melt(id_vars="周期", var_name=label_column, value_name="数量")
    data["周期层级"] = data["周期"].str[0].map({"Y": "年", "Q": "季", "M": "月", "W": "周"})
    return alt.Chart(data).mark_bar().encode(
        x=alt.X("周期:N", sort=periods, scale=alt.Scale(domain=periods), axis=alt.Axis(labelAngle=0)),
        y=alt.Y("数量:Q", stack="zero"),
        color=alt.Color("周期层级:N", scale=alt.Scale(
            domain=["年", "季", "月", "周"],
            range=["#2563EB", "#F59E0B", "#10B981", "#8B5CF6"],
        ), title="时间周期"),
        detail=f"{label_column}:N",
        tooltip=["周期:N", "周期层级:N", f"{label_column}:N", "数量:Q"],
    ).properties(height=320)


def build_oos_top_station_chart_data(
    station_df: pd.DataFrame,
    step_desc_map: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Prepare ranked series with ten slots for the stacked station chart."""
    station = station_df.copy()
    descriptions = step_desc_map or {}
    station["站点"] = station["step_id"].astype(str).map(
        lambda value: f"{value} {descriptions.get(value, '')}".strip()
    )
    station["series"] = station["scope"].str.upper() + "-" + station["alarm_type"]
    station_chart = station.pivot_table(
        index="站点", columns="series", values="oos_count", aggfunc="sum", fill_value=0
    )
    # Rank stations by their combined count, then reserve ten categorical slots.
    # Whitespace-only labels keep unused positions visually empty, without
    # changing any of the source frames or the displayed detail table.
    ordered = station_chart.sum(axis=1).sort_values(ascending=False, kind="stable").head(10).index.tolist()
    slots = ordered + [" " * index for index in range(1, 11 - len(ordered))]
    return station_chart.reindex(slots, fill_value=0)
