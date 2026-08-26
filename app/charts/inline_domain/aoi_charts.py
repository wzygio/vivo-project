"""AOI 报表共用图表：月周天趋势图与 By Lot/By Sheet 点线图（aoi_rs / aoi_tt 共用）。

差异通过参数注入：
- ``spec_lines``：RS 传单值规格线，TT 传 USL/UCL 双上限（虚线/点线）；
- ``bar_unit_name`` / ``line_value_label`` / ``y_title``：RS「过货量 / RS/片」、TT「检测片数 / TT/片」；
- ``code_column`` 与 ``code_names``：RS 用 ``rs_code`` + 中文描述显示名，TT 用 ``tt_name``。
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from app.components.distribution_charts import create_point_line_trace
from app.charts.inline_domain.constants import (
    CODE_PALETTE,
    PERIOD_BAR_COLORS,
    PERIOD_SEPARATORS,
    PERIOD_TYPE_NAMES,
)
from app.charts.inline_domain.sheet_axis import build_sheet_time_axis_labels


@dataclass(frozen=True)
class AoiSpecLine:
    """一条水平规格线：value 为 None/NaN 时不绘制。"""

    value: float | None
    suffix: str
    color: str
    dash: str = "dash"


def code_color_map(codes: list[str]) -> dict[str, str]:
    return {code: CODE_PALETTE[index % len(CODE_PALETTE)] for index, code in enumerate(codes)}


def add_spec_trace(
    figure: go.Figure,
    x_values: list[str],
    spec_value: float,
    name: str,
    color: str,
    dash: str = "dash",
    showlegend: bool = True,
) -> None:
    figure.add_trace(
        go.Scatter(
            x=[x_values[0], x_values[-1]],
            y=[spec_value, spec_value],
            mode="lines",
            name=name,
            line={"color": color, "width": 1.5, "dash": dash},
            showlegend=showlegend,
            hovertemplate=f"{name}: %{{y}}<extra></extra>",
        )
    )


def _add_spec_lines(
    figure: go.Figure,
    x_values: list[str],
    spec_lines: list[AoiSpecLine],
    display_name: str,
    showlegend: bool,
) -> None:
    for spec_line in spec_lines:
        if spec_line.value is not None and pd.notna(spec_line.value):
            add_spec_trace(
                figure,
                x_values,
                float(spec_line.value),
                f"{display_name} {spec_line.suffix}",
                spec_line.color,
                dash=spec_line.dash,
                showlegend=showlegend,
            )


def create_aoi_period_trend_chart(
    *,
    trend_df: pd.DataFrame,
    throughput_df: pd.DataFrame,
    spec_lines: list[AoiSpecLine],
    code_name: str,
    title: str,
    line_value_label: str,
    bar_unit_name: str,
    y_title: str,
) -> go.Figure:
    """单 Code 月周天趋势图：双 Y 轴（左=比值线+规格，右=过货/检测量柱）。

    x 轴按 period_sort 排列（2月→3周→7天），月/周/天组间插入零宽空格留白，
    柱状按 period_type 分组配色以区分粒度。
    """
    figure = make_subplots(specs=[[{"secondary_y": True}]])

    axis_source = throughput_df if not throughput_df.empty else trend_df
    axis = (
        axis_source[["period_type", "period_label", "period_sort"]]
        .drop_duplicates()
        .sort_values("period_sort", kind="stable")
    )
    if axis.empty:
        return figure

    grouped_labels: list[tuple[str, list[str]]] = []
    for period_type in ("month", "week", "day"):
        labels = axis[axis["period_type"] == period_type]["period_label"].astype(str).tolist()
        if labels:
            grouped_labels.append((period_type, labels))

    # 显示标签去掉年份前缀（2026-07→07、2026-W31→W31、2026-08-10→08-10），值映射仍用原始标签
    def _display(label: str) -> str:
        return re.sub(r"^\d{4}-", "", label)

    x_labels: list[str] = []
    raw_labels: list[str] = []
    separator_positions: list[int] = []
    for group_index, (_period_type, labels) in enumerate(grouped_labels):
        if group_index > 0:
            separator_positions.append(len(x_labels))
            x_labels.append(PERIOD_SEPARATORS[(group_index - 1) % len(PERIOD_SEPARATORS)])
            raw_labels.append("")
        x_labels.extend(_display(label) for label in labels)
        raw_labels.extend(labels)

    # 过货/检测量柱状（次 Y 轴，按粒度分组配色）
    throughput_map = (
        throughput_df.set_index("period_label")["sheet_qty"].to_dict()
        if not throughput_df.empty
        else {}
    )
    for period_type, labels in grouped_labels:
        figure.add_trace(
            go.Bar(
                x=[_display(label) for label in labels],
                y=[int(throughput_map.get(label, 0)) for label in labels],
                name=f"{bar_unit_name}（{PERIOD_TYPE_NAMES[period_type]}）",
                marker_color=PERIOD_BAR_COLORS[period_type],
                hovertemplate=f"%{{x}}<br>{bar_unit_name}: %{{y}} 片<extra></extra>",
            ),
            secondary_y=True,
        )

    # 单 Code 比值线（主 Y 轴），分隔位断开；折线与规格线不进图注
    trend_map = (
        trend_df.set_index("period_label")["value"].to_dict() if not trend_df.empty else {}
    )
    y_values = []
    for position, raw_label in enumerate(raw_labels):
        if position in separator_positions:
            y_values.append(None)
            continue
        value = trend_map.get(raw_label)
        y_values.append(float(value) if value is not None and pd.notna(value) else None)
    line_trace = create_point_line_trace(
        x_values=x_labels,
        y_values=y_values,
        name=code_name,
        color=CODE_PALETTE[0],
        hovertemplate=f"%{{x}}<br>{line_value_label}: %{{y:.3f}}<extra></extra>",
    )
    line_trace.showlegend = False
    figure.add_trace(line_trace, secondary_y=False)

    _add_spec_lines(figure, x_labels, spec_lines, code_name, showlegend=False)

    figure.update_layout(
        title=title,
        xaxis={"type": "category", "title": "期间（月/周/天）"},
        yaxis={"title": y_title},
        yaxis2={"title": f"{bar_unit_name}（片）", "overlaying": "y", "side": "right", "showgrid": False},
        legend={"orientation": "h", "yanchor": "top", "y": -0.22},
        margin={"l": 40, "r": 20, "t": 60, "b": 130},
        height=460,
    )
    return figure


def create_aoi_point_chart(
    *,
    point_df: pd.DataFrame,
    id_col: str,
    code_column: str,
    code_specs: dict[str, list[AoiSpecLine]],
    title: str,
    y_title: str,
    y_col: str,
    code_names: dict[str, str] | None = None,
) -> go.Figure:
    """By Lot / By Sheet 点线图：x 按首次过货时间排序，每个 Code 一条线 + 规格线。"""
    figure = go.Figure()
    if point_df.empty:
        return figure

    is_sheet_axis = id_col == "sheet_id"
    if is_sheet_axis:
        x_order, label_by_id = build_sheet_time_axis_labels(
            point_df,
            time_column="first_start_time",
        )
    else:
        x_order = (
            point_df[[id_col, "first_start_time"]]
            .drop_duplicates()
            .sort_values("first_start_time", kind="stable")[id_col]
            .astype(str)
            .tolist()
        )
        label_by_id = {x_value: x_value for x_value in x_order}
    display_x_order = [label_by_id[x_value] for x_value in x_order]
    codes = sorted(point_df[code_column].astype(str).unique().tolist())
    colors = code_color_map(codes)
    display_names = code_names or {}
    for code in codes:
        code_df = point_df[point_df[code_column].astype(str) == code].set_index(id_col)
        y_values = [
            (float(code_df.loc[x, y_col]) if x in code_df.index and pd.notna(code_df.loc[x, y_col]) else None)
            for x in x_order
        ]
        display_name = display_names.get(code, code)
        figure.add_trace(
            create_point_line_trace(
                x_values=display_x_order,
                y_values=y_values,
                name=display_name,
                color=colors[code],
                hovertemplate=f"%{{x}}<br>{y_title}: %{{y}}<extra></extra>",
            )
        )
        _add_spec_lines(
            figure,
            display_x_order,
            code_specs.get(code, []),
            display_name,
            showlegend=True,
        )

    figure.update_layout(
        title=title,
        xaxis={
            "type": "category",
            "title": "Sheet ID / 过货时间（小时）" if is_sheet_axis else id_col,
            "categoryorder": "array",
            "categoryarray": display_x_order,
            "tickangle": -50 if is_sheet_axis else 0,
            "tickfont": {"size": 9} if is_sheet_axis else None,
            "automargin": True,
        },
        yaxis={"title": y_title},
        legend={"orientation": "h", "yanchor": "top", "y": -0.5},
        margin={"l": 40, "r": 20, "t": 60, "b": 200},
        height=520,
    )
    return figure
