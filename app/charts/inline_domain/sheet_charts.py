"""SPC/CTQ 共用的 Sheet 级图表：月周天分布与 Sheet 点位分布。

业务规则：
- 月周天分布图：横轴为「月/周/日 | 周期标签」的类别轴，按周期类型着色箱线；
- Sheet 点位图 By 过货时间：chart_type=line 时横轴替换为真实过货时间（date 轴）；
- 规格线与纵轴范围规则见 ``app.charts.inline.spec_lines``（LSL 为空或 0 仅绘上限）。
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.graph_objects as go

from app.components.distribution_charts import (
    create_box_distribution_trace,
    create_point_line_trace,
)
from app.charts.inline_domain.chart_type import CHART_TYPE_BOX, CHART_TYPE_LINE
from app.charts.inline_domain.constants import (
    PERIOD_COLORS,
    PERIOD_FILL_COLORS,
    PERIOD_LABELS,
    SHEET_BOX_PALETTE,
)
from app.charts.inline_domain.spec_lines import (
    apply_measurement_spec_lines,
    resolve_measurement_y_range,
)
from src.inline_domain.core.spc.spc_calculator import (
    build_available_period_axis,
    build_period_axis,
)


def _display_period_label(period_type: str, period_label: str) -> str:
    return f"{PERIOD_LABELS.get(period_type, period_type)} | {period_label}"


def _empty_period_points_frame(value_column: str = "sheet_mean") -> pd.DataFrame:
    return pd.DataFrame(columns=["period_type", "period_label", "display_label", "period_sort", value_column])


def _add_display_labels(axis_df: pd.DataFrame) -> pd.DataFrame:
    axis_df = axis_df.copy()
    axis_df["display_label"] = [
        _display_period_label(period_type, period_label)
        for period_type, period_label in zip(axis_df["period_type"], axis_df["period_label"])
    ]
    return axis_df


def period_axis_with_display(end_date: date, sheet_features_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if sheet_features_df is not None and not sheet_features_df.empty:
        axis_df = build_available_period_axis(sheet_features_df, end_date).copy()
    else:
        axis_df = build_period_axis(end_date).copy()
    return _add_display_labels(axis_df)


def infer_period_axis_end_date(sheet_features_df: pd.DataFrame, period_capability_df: pd.DataFrame) -> date:
    for source_df, column in [
        (period_capability_df, "period_end"),
        (sheet_features_df, "sheet_start_time"),
    ]:
        if source_df.empty or column not in source_df.columns:
            continue
        max_value = pd.to_datetime(source_df[column], errors="coerce").max()
        if pd.notna(max_value):
            return max_value.date()
    return date.today()


def _period_points(
    source_df: pd.DataFrame,
    period_axis_df: pd.DataFrame,
    value_column: str,
) -> pd.DataFrame:
    if source_df.empty or "sheet_start_time" not in source_df.columns or value_column not in source_df.columns:
        return _empty_period_points_frame(value_column)

    df = source_df.copy()
    df["sheet_start_time"] = pd.to_datetime(df["sheet_start_time"], errors="coerce")
    df[value_column] = pd.to_numeric(df[value_column], errors="coerce")
    df = df.dropna(subset=["sheet_start_time", value_column]).copy()
    if df.empty:
        return _empty_period_points_frame(value_column)

    frames: list[pd.DataFrame] = []
    month_df = df.copy()
    month_df["period_type"] = "month"
    month_df["period_label"] = month_df["sheet_start_time"].dt.strftime("%Y-%m")
    frames.append(month_df)

    week_df = df.copy()
    iso_week = week_df["sheet_start_time"].dt.isocalendar()
    week_df["period_type"] = "week"
    week_df["period_label"] = iso_week.year.astype(str) + "-W" + iso_week.week.astype(str).str.zfill(2)
    frames.append(week_df)

    day_df = df.copy()
    day_df["period_type"] = "day"
    day_df["period_label"] = day_df["sheet_start_time"].dt.strftime("%Y-%m-%d")
    frames.append(day_df)

    points_df = pd.concat(frames, ignore_index=True)
    return points_df.merge(
        period_axis_df[["period_type", "period_label", "period_sort", "display_label"]],
        on=["period_type", "period_label"],
        how="inner",
    )


def sheet_period_points(sheet_features_df: pd.DataFrame, period_axis_df: pd.DataFrame) -> pd.DataFrame:
    return _period_points(sheet_features_df, period_axis_df, "sheet_mean")


def measurement_period_points(raw_measurements_df: pd.DataFrame, period_axis_df: pd.DataFrame) -> pd.DataFrame:
    return _period_points(raw_measurements_df, period_axis_df, "param_value")


def create_period_overview_chart(
    sheet_features_df: pd.DataFrame,
    period_capability_df: pd.DataFrame,
    title: str,
    raw_measurements_df: pd.DataFrame | None = None,
    period_box_source: str = "sheet_mean",
) -> go.Figure:
    """Create the month/week/day box distribution figure."""
    fig = go.Figure()

    axis_end_date = infer_period_axis_end_date(sheet_features_df, period_capability_df)
    period_axis_df = period_axis_with_display(axis_end_date, sheet_features_df)
    use_point_values = period_box_source == "point_value" and raw_measurements_df is not None
    if use_point_values:
        points_df = measurement_period_points(raw_measurements_df, period_axis_df)
        value_column = "param_value"
        value_label = "Point Value"
    else:
        points_df = sheet_period_points(sheet_features_df, period_axis_df)
        value_column = "sheet_mean"
        value_label = "Sheet Mean"
    ordered_labels = period_axis_df["display_label"].tolist()
    for period_type in ["month", "week", "day"]:
        type_points = points_df[points_df["period_type"] == period_type]
        labels = period_axis_df[period_axis_df["period_type"] == period_type]["display_label"].tolist()
        for label in labels:
            y_values = type_points[type_points["display_label"] == label][value_column]
            if y_values.empty:
                continue
            fig.add_trace(
                create_box_distribution_trace(
                    x_values=[label] * len(y_values),
                    y_values=y_values,
                    name=label,
                    color=PERIOD_COLORS.get(period_type, "#2563eb"),
                    fillcolor=PERIOD_FILL_COLORS.get(period_type, "rgba(37, 99, 235, 0.18)"),
                    showlegend=False,
                    width=0.42,
                    hovertemplate=f"{label}<br>{value_label}=%{{y:.4f}}<extra></extra>",
                ),
            )

    spec_source = sheet_features_df
    apply_measurement_spec_lines(fig, spec_source)
    y_range = resolve_measurement_y_range(points_df[value_column], spec_source)
    if y_range is not None:
        fig.update_yaxes(range=y_range)

    fig.update_layout(
        title=title,
        height=450,
        margin={"l": 32, "r": 24, "t": 58, "b": 82},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        boxmode="group",
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        yaxis={"title": value_label},
    )
    fig.update_xaxes(categoryorder="array", categoryarray=ordered_labels, tickangle=-35)
    return fig


def _resolve_chamber_column(df: pd.DataFrame) -> str:
    return "main_process_unit_id" if "main_process_unit_id" in df.columns else ""


def _sheet_id_order(df: pd.DataFrame) -> list[str]:
    if "sheet_id" not in df.columns:
        return []
    return df["sheet_id"].dropna().astype(str).drop_duplicates().tolist()


def create_sheet_points_box_chart(
    raw_measurements_df: pd.DataFrame,
    sort_mode: str,
    title: str,
    spec_df: pd.DataFrame | None = None,
    chart_type: str = CHART_TYPE_BOX,
) -> go.Figure:
    """Create point-level boxes or point-line trends by chamber/site or pass time."""
    fig = go.Figure()
    if raw_measurements_df.empty or "param_value" not in raw_measurements_df.columns:
        fig.update_layout(title=title, height=420)
        return fig

    df = raw_measurements_df.copy()
    df["param_value"] = pd.to_numeric(df["param_value"], errors="coerce")
    df["sheet_start_time"] = pd.to_datetime(df.get("sheet_start_time"), errors="coerce")
    df = df.dropna(subset=["param_value"]).copy()
    if df.empty:
        fig.update_layout(title=title, height=420)
        return fig

    uses_time_axis = sort_mode == "按过货时间排序" and chart_type == CHART_TYPE_LINE
    if sort_mode == "按过货时间排序":
        sorted_df = df.sort_values(["sheet_start_time", "sheet_id"], na_position="last")
        group_labels = _sheet_id_order(sorted_df)
        if chart_type == CHART_TYPE_LINE:
            trend_points = sorted_df.dropna(subset=["sheet_start_time"]).assign(
                sheet_id=lambda frame: frame["sheet_id"].astype(str)
            )
            if not trend_points.empty:
                fig.add_trace(
                    create_point_line_trace(
                        x_values=trend_points["sheet_start_time"],
                        y_values=trend_points["param_value"],
                        customdata=trend_points["sheet_id"],
                        name="Point Value",
                        color="#1d4ed8",
                        hovertemplate=(
                            "Time=%{x|%Y-%m-%d %H:%M:%S}<br>"
                            "Sheet=%{customdata}<br>"
                            "Param Value=%{y:.4f}<extra></extra>"
                        ),
                    )
                )
        else:
            for sheet_id in group_labels:
                y_values = sorted_df[sorted_df["sheet_id"].astype(str) == sheet_id]["param_value"]
                fig.add_trace(
                    create_box_distribution_trace(
                        y_values=y_values,
                        name=sheet_id,
                        color="#1d4ed8",
                        showlegend=False,
                    )
                )
    else:
        chamber_col = _resolve_chamber_column(df)
        df["chamber_label"] = (
            df[chamber_col].fillna("UNKNOWN").astype(str)
            if chamber_col
            else "UNKNOWN"
        )
        df["chamber_label"] = df["chamber_label"].fillna("UNKNOWN").astype(str)
        sorted_df = df.sort_values(["chamber_label", "sheet_start_time", "sheet_id"], na_position="last")
        sheet_order = _sheet_id_order(sorted_df)
        chamber_order = sorted(sorted_df["chamber_label"].dropna().astype(str).unique().tolist())
        chamber_colors = {
            chamber: SHEET_BOX_PALETTE[index % len(SHEET_BOX_PALETTE)]
            for index, chamber in enumerate(chamber_order)
        }
        if chart_type == CHART_TYPE_LINE:
            for chamber in chamber_order:
                trend_points = sorted_df[
                    sorted_df["chamber_label"] == chamber
                ].assign(
                    sheet_id=lambda frame: frame["sheet_id"].astype(str)
                )
                fig.add_trace(
                    create_point_line_trace(
                        x_values=trend_points["sheet_id"],
                        y_values=trend_points["param_value"],
                        name=chamber,
                        color=chamber_colors.get(chamber, SHEET_BOX_PALETTE[0]),
                        hovertemplate=f"Chamber={chamber}<br>Sheet=%{{x}}<br>Param Value=%{{y:.4f}}<extra></extra>",
                    )
                )
        else:
            shown_chambers: set[str] = set()
            for sheet_id in sheet_order:
                sheet_rows = sorted_df[sorted_df["sheet_id"].astype(str) == sheet_id]
                if sheet_rows.empty:
                    continue
                chamber = str(sheet_rows["chamber_label"].iloc[0])
                y_values = sheet_rows["param_value"]
                color = chamber_colors.get(chamber, SHEET_BOX_PALETTE[0])
                fig.add_trace(
                    create_box_distribution_trace(
                        x_values=[sheet_id] * len(y_values),
                        y_values=y_values,
                        name=chamber,
                        color=color,
                        legendgroup=chamber,
                        showlegend=chamber not in shown_chambers,
                    )
                )
                shown_chambers.add(chamber)

    if spec_df is not None:
        apply_measurement_spec_lines(fig, spec_df)
        y_range = resolve_measurement_y_range(df["param_value"], spec_df)
        if y_range is not None:
            fig.update_yaxes(range=y_range)

    fig.update_layout(
        title=title,
        height=430,
        margin={"l": 32, "r": 24, "t": 56, "b": 80},
        xaxis_title="过货时间" if uses_time_axis else None,
        yaxis_title="Param Value",
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
    )
    if uses_time_axis:
        fig.update_xaxes(type="date", tickformat="%m-%d\n%H:%M", tickangle=0)
    else:
        fig.update_xaxes(tickangle=-45)
    return fig


def create_sheet_points_box_charts(
    raw_measurements_df: pd.DataFrame,
    title_prefix: str,
    spec_df: pd.DataFrame | None = None,
    chart_type: str = CHART_TYPE_BOX,
) -> tuple[go.Figure, go.Figure]:
    chamber_fig = create_sheet_points_box_chart(
        raw_measurements_df=raw_measurements_df,
        sort_mode="按腔室排序",
        title=f"{title_prefix} | Sheet点位分布 By主站点设备/腔室",
        spec_df=spec_df,
        chart_type=chart_type,
    )
    time_fig = create_sheet_points_box_chart(
        raw_measurements_df=raw_measurements_df,
        sort_mode="按过货时间排序",
        title=f"{title_prefix} | Sheet点位分布 By过货时间",
        spec_df=spec_df,
        chart_type=chart_type,
    )
    return chamber_fig, time_fig
