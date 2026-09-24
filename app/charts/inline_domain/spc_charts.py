"""Shared SPC chart construction for the dashboard and exported reports."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date

import pandas as pd
import plotly.graph_objects as go

from app.charts.inline_domain import (
    create_period_overview_chart,
    create_sheet_points_box_charts,
    resolve_chart_type,
)
from app.utils.step_labels import format_step_label
from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range


@dataclass(frozen=True)
class SpcChartGroup:
    factory: str
    title: str
    figures: tuple[go.Figure, go.Figure, go.Figure]


def build_spc_indicator_figures(
    label: str,
    chart_type: str,
    indicator_features_df: pd.DataFrame,
    indicator_capability_df: pd.DataFrame,
    indicator_raw_df: pd.DataFrame,
    period_box_source: str,
    reference_date: date | None = None,
    *,
    period_chart_builder: Callable[..., go.Figure] | None = None,
    detail_chart_builder: Callable[..., tuple[go.Figure, go.Figure]] | None = None,
) -> dict[str, go.Figure]:
    """Build the same period, chamber and time figures for every presentation."""
    reference_date = reference_date or date.today()  # noqa: DTZ011 - Reports use the server's local day.
    period_builder = period_chart_builder or create_period_overview_chart
    detail_builder = detail_chart_builder or create_sheet_points_box_charts
    period_figure = period_builder(
        sheet_features_df=indicator_features_df,
        period_capability_df=indicator_capability_df,
        raw_measurements_df=indicator_raw_df,
        period_box_source=period_box_source,
        title=f"{label} | 月周天分布",
        reference_date=reference_date,
    )
    detail_start, _ = previous_iso_week_range(reference_date)
    detail_points = indicator_raw_df
    if not detail_points.empty:
        times = pd.to_datetime(detail_points["sheet_start_time"], errors="coerce")
        detail_points = detail_points.loc[
            times.ge(detail_start) & times.lt(pd.Timestamp(reference_date) + pd.Timedelta(days=1))
        ].copy()
    chamber_figure, time_figure = detail_builder(
        raw_measurements_df=detail_points,
        title_prefix=f"{label} | {detail_start:%m-%d} 起",
        spec_df=indicator_features_df,
        chart_type=chart_type,
        date_only=True,
    )
    return {
        "fig1": period_figure,
        "chamber_fig": chamber_figure,
        "time_fig": time_figure,
    }


def _indicator_frame(
    frame: pd.DataFrame, factory: object, step_id: object, param_name: object
) -> pd.DataFrame:
    keys = ("factory", "step_id", "param_name")
    if frame.empty or not set(keys).issubset(frame.columns):
        return frame.iloc[0:0].copy()
    mask = pd.Series(True, index=frame.index)
    for column, value in zip(keys, (factory, step_id, param_name)):
        mask &= frame[column].astype(str).eq(str(value))
    return frame.loc[mask].copy()


def iter_spc_chart_groups(
    period_capability_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    period_box_source: str = "point_value",
    reference_date: date | None = None,
    line_param_name_contains: Sequence[str] = (),
    step_desc_map: dict[str, str] | None = None,
) -> Iterator[SpcChartGroup]:
    """Yield the main report's indicator rows, excluding every alert section."""
    if raw_measurements_df.empty:
        return
    grouped = raw_measurements_df.groupby(["factory", "step_id", "param_name"], sort=True)
    for (factory, step_id, param_name), indicator_raw_df in grouped:
        label = f"{factory} | {format_step_label(step_id, step_desc_map)} | {param_name}"
        figures = build_spc_indicator_figures(
            label=label,
            chart_type=resolve_chart_type(param_name, line_param_name_contains),
            indicator_features_df=_indicator_frame(sheet_features_df, factory, step_id, param_name),
            indicator_capability_df=_indicator_frame(period_capability_df, factory, step_id, param_name),
            indicator_raw_df=indicator_raw_df,
            period_box_source=period_box_source,
            reference_date=reference_date,
        )
        yield SpcChartGroup(
            factory=str(factory),
            title=label,
            figures=(figures["fig1"], figures["chamber_fig"], figures["time_fig"]),
        )
