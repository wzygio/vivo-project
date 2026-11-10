"""Point distributions from detail samples, independently of maintained summaries."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from app.components.distribution_charts import create_box_distribution_trace


def create_detail_box_charts(detail: pd.DataFrame) -> dict[tuple[str, str], go.Figure]:
    """For one product, separate steps/parameters and box each Sheet ID."""
    if detail.empty:
        return {}
    points = detail.reindex(columns=["SHEET_ID", "STEP_ID", "PARAM_NAME", "PARAM_VALUE"]).copy()
    points["PARAM_VALUE"] = pd.to_numeric(points["PARAM_VALUE"], errors="coerce")
    points = points.loc[np.isfinite(points["PARAM_VALUE"])].dropna(subset=["SHEET_ID", "PARAM_NAME"])
    for column in ("SHEET_ID", "PARAM_NAME"):
        points[column] = points[column].astype(str).str.strip()
        points = points.loc[points[column] != ""]
    points["STEP_ID"] = points["STEP_ID"].fillna("").astype(str).str.strip().replace("", "未提供站点")
    charts = {}
    for (step, parameter), parameter_points in points.groupby(["STEP_ID", "PARAM_NAME"], sort=False):
        figure = go.Figure()
        for sheet_id, samples in parameter_points.groupby("SHEET_ID", sort=False):
            trace = create_box_distribution_trace(
                y_values=samples["PARAM_VALUE"].tolist(),
                x_values=[sheet_id] * len(samples),
                name=sheet_id,
                color="#1d4ed8",
            )
            trace.quartilemethod = "linear"
            figure.add_trace(trace)
        figure.update_layout(
            title=f"{step} / {parameter} · 明细测量值分布",
            height=430,
            margin={"l": 32, "r": 24, "t": 56, "b": 80},
            xaxis_title="Sheet ID",
            yaxis_title="Param Value",
            plot_bgcolor="#ffffff",
            paper_bgcolor="#ffffff",
        )
        figure.update_xaxes(type="category", categoryorder="array",
                            categoryarray=parameter_points["SHEET_ID"].drop_duplicates().tolist(),
                            tickangle=-45)
        charts[(str(step), str(parameter))] = figure
    return charts
