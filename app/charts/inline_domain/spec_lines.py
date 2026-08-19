"""Inline 报表规格线绘制与纵轴范围推导（SPC/CTQ 共用）。

业务规则：
- 仅绘制上限：当 LSL 为空或等于 0 时，只画 USL/UCL，不画 LSL/LCL/Target/CL。
- 规格值取自首行携带至少一个数值规格的记录。
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def format_spec_value(value: object) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    float_value = float(numeric_value)
    absolute_value = abs(float_value)
    if float_value != 0.0 and (absolute_value < 0.001 or absolute_value >= 1_000_000):
        return f"{float_value:.4g}"
    value_text = f"{float_value:.3f}".rstrip("0").rstrip(".")
    return value_text if value_text else "0"


def format_spec_line_label(label: str, value: object) -> str:
    return f"{label}: {format_spec_value(value)}"


def _add_spec_line(fig: go.Figure, y_value: object, label: str, color: str, row: int) -> None:
    if pd.isna(y_value):
        return
    fig.add_hline(
        y=float(y_value),
        line_dash="dash",
        line_color=color,
        line_width=1.4,
        annotation_text=format_spec_line_label(label, y_value),
        annotation_position="top right",
        row=row,
        col=1,
    )


def _add_plain_spec_line(fig: go.Figure, y_value: object, label: str, color: str) -> None:
    if pd.isna(y_value):
        return
    fig.add_hline(
        y=float(y_value),
        line_dash="dash",
        line_color=color,
        line_width=1.4,
        annotation_text=format_spec_line_label(label, y_value),
        annotation_position="top right",
    )


def resolve_target_value(spec_row: pd.Series) -> float | None:
    target = spec_row.get("target")
    if pd.notna(target):
        return float(target)
    usl = spec_row.get("usl")
    lsl = spec_row.get("lsl")
    if pd.notna(usl) and pd.notna(lsl):
        return float((float(usl) + float(lsl)) / 2.0)
    return None


def resolve_cl_value(spec_row: pd.Series) -> float | None:
    ucl = spec_row.get("ucl")
    lcl = spec_row.get("lcl")
    if pd.notna(ucl) and pd.notna(lcl):
        return float((float(ucl) + float(lcl)) / 2.0)
    return resolve_target_value(spec_row)


def first_measurement_spec_row(spec_df: pd.DataFrame) -> pd.Series | None:
    """Return the first row carrying at least one numeric specification limit."""
    if spec_df.empty:
        return None

    limit_columns = ["usl", "lsl", "ucl", "lcl"]
    numeric_limits = spec_df.reindex(columns=limit_columns).apply(
        pd.to_numeric,
        errors="coerce",
    )
    valid_rows = numeric_limits.notna().any(axis=1)
    if not valid_rows.any():
        return None
    return spec_df.loc[valid_rows].iloc[0]


def apply_measurement_spec_lines(
    fig: go.Figure,
    spec_df: pd.DataFrame,
    row: int | None = None,
) -> None:
    """Draw specification limit lines; LSL empty or 0 means upper limits only."""
    spec_row = first_measurement_spec_row(spec_df)
    if spec_row is None:
        return
    line_func = (
        (lambda value, label, color: _add_spec_line(fig, value, label, color, row=row))
        if row is not None
        else (lambda value, label, color: _add_plain_spec_line(fig, value, label, color))
    )
    line_func(spec_row.get("usl"), "USL", "#dc2626")
    lsl = pd.to_numeric(pd.Series([spec_row.get("lsl")]), errors="coerce").iloc[0]
    if pd.isna(lsl) or float(lsl) == 0.0:
        line_func(spec_row.get("ucl"), "UCL", "#16a34a")
        return

    line_func(spec_row.get("lsl"), "LSL", "#dc2626")
    line_func(spec_row.get("ucl"), "UCL", "#16a34a")
    line_func(spec_row.get("lcl"), "LCL", "#16a34a")
    target_value = resolve_target_value(spec_row)
    if target_value is not None:
        line_func(target_value, "Target", "#f97316")
    cl_value = resolve_cl_value(spec_row)
    if cl_value is not None:
        line_func(cl_value, "CL", "#16a34a")


def resolve_measurement_y_range(data_values: object, spec_df: pd.DataFrame) -> list[float] | None:
    spec_row = first_measurement_spec_row(spec_df)
    if spec_row is None:
        return None

    usl = pd.to_numeric(pd.Series([spec_row.get("usl")]), errors="coerce").iloc[0]
    lsl = pd.to_numeric(pd.Series([spec_row.get("lsl")]), errors="coerce").iloc[0]
    values = pd.to_numeric(pd.Series(data_values), errors="coerce").dropna()

    if pd.notna(usl) and pd.notna(lsl) and usl > lsl:
        if values.empty:
            return [float(lsl), float(usl)]
        lower = min(float(lsl), float(values.min()))
        upper = max(float(usl), float(values.max()))
        if lower == float(lsl) and upper == float(usl):
            return [float(lsl), float(usl)]
    else:
        limit_values = pd.to_numeric(
            pd.Series([spec_row.get(column) for column in ["usl", "lsl", "ucl", "lcl"]]),
            errors="coerce",
        ).dropna()
        bounds = pd.concat([values.reset_index(drop=True), limit_values.reset_index(drop=True)])
        if bounds.empty:
            return None
        lower = float(bounds.min())
        upper = float(bounds.max())

    span = upper - lower
    padding = span * 0.06 if span > 0 else max(abs(upper), 1.0) * 0.06
    return [float(lower - padding), float(upper + padding)]
