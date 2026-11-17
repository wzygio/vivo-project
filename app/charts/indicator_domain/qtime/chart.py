"""Deterministic Plotly model for the Q-Time Lot chart."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


BAR_COLOR = "#31c7c9"
BAR_COLORS = (BAR_COLOR, "#2563eb", "#7c3aed", "#0891b2", "#0f766e")
SPEC_COLOR = "#ef4444"
MAX_LOT_TICKS = 24


def build_qtime_figure(
    details: pd.DataFrame,
    *,
    title: str = "北极星QTime监控",
) -> go.Figure:
    """Build the Lot wait-time bars and optional row-level specification line."""
    frame = details.copy().reset_index(drop=True)
    if "step_desc" in frame:
        step_names = frame["step_desc"].fillna("Q-Time").astype(str)
    else:
        step_names = pd.Series("Q-Time", index=frame.index, dtype="object")
    frame = frame.assign(_step_name=step_names, _plot_lot=_lot_ids(frame))
    separate_passages = bool(frame.duplicated(["_step_name", "_plot_lot"]).any())
    if separate_passages:
        keys = frame.get("timekey", pd.Series("", index=frame.index)).astype("string")
        timestamps = pd.to_datetime(
            keys.str.slice(0, 14), format="%Y%m%d%H%M%S", errors="coerce",
        ).dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
        # Repeated x categories inside a Bar trace may be accumulated by Plotly,
        # even in group mode. Each source row needs its own plot coordinate.
        frame = frame.assign(
            _plot_time=timestamps,
            _plot_x=[f"{lot}｜{stamp}｜{index + 1}" for index, (lot, stamp) in enumerate(
                zip(frame["_plot_lot"], timestamps),
            )],
        )
    else:
        frame = frame.assign(_plot_x=frame["_plot_lot"])
    step_groups = list(frame.groupby("_step_name", sort=False))

    figure = go.Figure()
    for index, (step_name, group) in enumerate(step_groups):
        figure.add_bar(
            x=group["_plot_x"],
            y=_numeric_column(group, "wait_time"),
            name=str(step_name),
            showlegend=False,
            marker_color=BAR_COLORS[index % len(BAR_COLORS)],
            customdata=group[["_plot_lot", "_plot_time"]].to_numpy() if separate_passages else None,
            hovertemplate=(
                "Lot %{customdata[0]}<br>过货时间 %{customdata[1]}<br>等待时长 %{y:.2f} h<extra></extra>"
                if separate_passages else "Lot %{x}<br>等待时长 %{y:.2f} h<extra></extra>"
            ),
        )

    for _step_name, group in step_groups:
        specs = _numeric_column(group, "q_spec")
        if not specs.notna().any():
            continue
        if len(step_groups) == 1 and specs.dropna().nunique() == 1:
            # A one-point scatter in lines mode has no visible segment.
            figure.add_hline(
                y=float(specs.dropna().iloc[0]),
                line_color=SPEC_COLOR,
                line_width=2,
            )
        figure.add_scatter(
            x=group["_plot_x"],
            y=specs,
            name="QTime规格",
            mode="lines",
            line={"color": SPEC_COLOR, "width": 2},
            hovertemplate="QTime规格 %{y:.2f} h<extra></extra>",
            legendgroup="qtime-spec",
            showlegend=False,
        )

    figure.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center"},
        height=430,
        margin={"l": 50, "r": 24, "t": 72, "b": 82},
        legend={"orientation": "h", "x": 0.5, "xanchor": "center", "y": 1.02},
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        bargap=0.35,
        barmode="group",
        xaxis={"title": "Lot ID", "tickangle": -45, "showgrid": False},
        yaxis={
            "title": "等待时长（小时）",
            "rangemode": "tozero",
            "gridcolor": "#d7dde5",
            "zeroline": True,
            "zerolinecolor": "#94a3b8",
            "zerolinewidth": 1.5,
        },
        hovermode="x unified",
    )
    if separate_passages:
        stride = max(1, (len(frame) + MAX_LOT_TICKS - 1) // MAX_LOT_TICKS)
        ticks = frame.iloc[::stride]
        figure.update_xaxes(tickmode="array", tickvals=ticks["_plot_x"], ticktext=ticks["_plot_lot"])
    values = pd.concat([
        _numeric_column(frame, "wait_time"), _numeric_column(frame, "q_spec"),
    ]).replace([float("inf"), -float("inf")], float("nan"))
    upper = values.max()
    if pd.notna(upper) and upper > 0:
        figure.update_yaxes(range=[0, float(upper) * 1.1])
    return figure


def _lot_ids(frame: pd.DataFrame) -> pd.Series:
    values = frame.get("lot_id", pd.Series(index=frame.index, dtype="object"))
    return values.fillna("").astype(str)


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")
