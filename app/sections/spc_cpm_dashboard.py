from __future__ import annotations

from datetime import date
from typing import Iterable

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.spc_domain.core.cpm_calculator import build_period_axis, get_period_window_start

CPM_FACTORY_OPTIONS = ["ARRAY", "OLED", "TP"]
CPM_METRIC_OPTIONS = ["CPM", "CPK"]
PERIOD_LABELS = {"month": "月", "week": "周", "day": "日"}
PERIOD_COLORS = {"month": "#2563eb", "week": "#16a34a", "day": "#f59e0b"}
PERIOD_FILL_COLORS = {
    "month": "rgba(37, 99, 235, 0.18)",
    "week": "rgba(22, 163, 74, 0.18)",
    "day": "rgba(245, 158, 11, 0.18)",
}
SHEET_BOX_PALETTE = ["#2563eb", "#16a34a", "#f59e0b", "#8b5cf6", "#0f766e", "#dc2626", "#64748b"]


def get_default_cpm_start_date(end_date: date) -> date:
    """Return the first day needed by the CPM/CPK Task2 report."""
    return get_period_window_start(end_date)


def _normalise_selection(selection: Iterable[str], available: list[str]) -> list[str]:
    return [item for item in selection if item in available]


def _unique_sorted(df: pd.DataFrame, column: str) -> list[str]:
    if df.empty or column not in df.columns:
        return []
    return sorted(df[column].dropna().astype(str).unique().tolist())


def get_available_factories(report_df: pd.DataFrame) -> list[str]:
    """Return available factories with known factory order preserved."""
    factories = set(_unique_sorted(report_df, "factory"))
    ordered = [factory for factory in CPM_FACTORY_OPTIONS if factory in factories]
    extras = sorted(factories.difference(CPM_FACTORY_OPTIONS))
    return ordered + extras


def get_steps_for_factory(report_df: pd.DataFrame, selected_factory: str) -> list[str]:
    """Return stations available under the selected factory."""
    if report_df.empty or not selected_factory:
        return []
    factory_df = report_df[report_df["factory"].astype(str) == str(selected_factory)]
    return _unique_sorted(factory_df, "step_id")


def get_params_for_factory_steps(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_steps: list[str],
) -> list[str]:
    """Return parameters available under the selected factory and stations."""
    if report_df.empty or not selected_factory or not selected_steps:
        return []
    df = report_df[
        (report_df["factory"].astype(str) == str(selected_factory))
        & (report_df["step_id"].astype(str).isin(selected_steps))
    ]
    return _unique_sorted(df, "param_name")


def _filter_signature(
    selected_factory: str,
    selected_steps: list[str],
    selected_params: list[str],
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    return selected_factory, tuple(selected_steps), tuple(selected_params)


def render_cpm_filters(indicator_df: pd.DataFrame) -> tuple[str, str, list[str], list[str], bool]:
    """Render CPM/CPK filters and return selected metric, factory, params, steps, and query state."""
    with st.container(border=True):
        st.markdown("#### 筛选")
        c_metric, c_factory, c_step, c_param, c_query = st.columns(
            [0.9, 1.1, 2.3, 3.2, 0.9],
            vertical_alignment="bottom",
        )

        with c_metric:
            selected_metric = st.selectbox(
                "指标",
                options=CPM_METRIC_OPTIONS,
                key="cpm_metric_filter",
            )

        available_factories = get_available_factories(indicator_df) or CPM_FACTORY_OPTIONS
        factory_key = "cpm_factory_filter"
        if st.session_state.get(factory_key) not in available_factories:
            st.session_state[factory_key] = available_factories[0]

        with c_factory:
            selected_factory = st.selectbox(
                "厂别",
                options=available_factories,
                key=factory_key,
            )

        available_steps = get_steps_for_factory(indicator_df, selected_factory)
        step_key = "cpm_step_filter"
        param_key = "cpm_param_filter"
        previous_factory_key = "cpm_previous_factory_filter"
        if st.session_state.get(previous_factory_key) != selected_factory:
            st.session_state[step_key] = []
            st.session_state[param_key] = []
            st.session_state[previous_factory_key] = selected_factory

        with c_step:
            st.session_state[step_key] = _normalise_selection(st.session_state.get(step_key, []), available_steps)
            selected_steps = st.multiselect("站点", options=available_steps, key=step_key)

        available_params = get_params_for_factory_steps(indicator_df, selected_factory, selected_steps)
        steps_signature_key = "cpm_steps_for_param_autoselect"
        steps_signature = _filter_signature(selected_factory, selected_steps, [])
        if st.session_state.get(steps_signature_key) != steps_signature:
            st.session_state[param_key] = available_params
            st.session_state[steps_signature_key] = steps_signature
        st.session_state[param_key] = _normalise_selection(st.session_state.get(param_key, []), available_params)

        with c_param:
            selected_params = st.multiselect(
                "参数名称",
                options=available_params,
                key=param_key,
                disabled=not selected_steps,
            )

        current_signature = _filter_signature(selected_factory, selected_steps, selected_params)
        applied_signature_key = "cpm_applied_filter_signature"
        can_query = bool(selected_factory and selected_steps and selected_params)
        with c_query:
            if st.button("查询", type="primary", width="stretch", disabled=not can_query):
                st.session_state[applied_signature_key] = current_signature

    should_render = bool(can_query and st.session_state.get(applied_signature_key) == current_signature)
    return selected_metric, selected_factory, selected_params, selected_steps, should_render


def filter_cpm_report(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_params: list[str],
    selected_steps: list[str],
) -> pd.DataFrame:
    """Apply frontend CPM/CPK filters to any report frame with factory/step/param columns."""
    if report_df.empty:
        return report_df

    df = report_df.copy()
    if selected_factory and "factory" in df.columns:
        df = df[df["factory"].astype(str) == str(selected_factory)]
    if selected_params and "param_name" in df.columns:
        df = df[df["param_name"].astype(str).isin(selected_params)]
    if selected_steps and "step_id" in df.columns:
        df = df[df["step_id"].astype(str).isin(selected_steps)]
    return df.reset_index(drop=True)


def _display_period_label(period_type: str, period_label: str) -> str:
    return f"{PERIOD_LABELS.get(period_type, period_type)} | {period_label}"


def _empty_period_points_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["period_type", "period_label", "display_label", "period_sort", "sheet_mean"])


def _period_axis_with_display(end_date: date) -> pd.DataFrame:
    axis_df = build_period_axis(end_date).copy()
    axis_df["display_label"] = [
        _display_period_label(period_type, period_label)
        for period_type, period_label in zip(axis_df["period_type"], axis_df["period_label"])
    ]
    return axis_df


def _infer_period_axis_end_date(sheet_features_df: pd.DataFrame, period_capability_df: pd.DataFrame) -> date:
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


def _sheet_period_points(sheet_features_df: pd.DataFrame, period_axis_df: pd.DataFrame) -> pd.DataFrame:
    if sheet_features_df.empty or "sheet_start_time" not in sheet_features_df.columns or "sheet_mean" not in sheet_features_df.columns:
        return _empty_period_points_frame()

    df = sheet_features_df.copy()
    df["sheet_start_time"] = pd.to_datetime(df["sheet_start_time"], errors="coerce")
    df = df.dropna(subset=["sheet_start_time", "sheet_mean"]).copy()
    if df.empty:
        return _empty_period_points_frame()

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


def _capability_axis_frame(period_capability_df: pd.DataFrame, period_axis_df: pd.DataFrame, metric_key: str) -> pd.DataFrame:
    axis_cols = ["period_type", "period_label", "period_sort", "display_label"]
    value_cols = [metric_key, "sample_count", "mean_value", "std_value"]
    if period_capability_df.empty or not {"period_type", "period_label"}.issubset(period_capability_df.columns):
        result = period_axis_df[axis_cols].copy()
        for col in value_cols:
            result[col] = pd.NA
        return result

    capability_df = period_capability_df.copy()
    for col in value_cols:
        if col not in capability_df.columns:
            capability_df[col] = pd.NA
    capability_df = capability_df[["period_type", "period_label", *value_cols]].drop_duplicates(
        ["period_type", "period_label"],
        keep="last",
    )
    return period_axis_df[axis_cols].merge(capability_df, on=["period_type", "period_label"], how="left")


def _add_spec_line(fig: go.Figure, y_value: object, label: str, color: str, row: int) -> None:
    if pd.isna(y_value):
        return
    fig.add_hline(
        y=float(y_value),
        line_dash="dash",
        line_color=color,
        line_width=1.4,
        annotation_text=label,
        annotation_position="top right",
        row=row,
        col=1,
    )


def _resolve_target_value(spec_row: pd.Series) -> float | None:
    target = spec_row.get("target")
    if pd.notna(target):
        return float(target)
    usl = spec_row.get("usl")
    lsl = spec_row.get("lsl")
    if pd.notna(usl) and pd.notna(lsl):
        return float((float(usl) + float(lsl)) / 2.0)
    return None


def _resolve_cl_value(spec_row: pd.Series) -> float | None:
    ucl = spec_row.get("ucl")
    lcl = spec_row.get("lcl")
    if pd.notna(ucl) and pd.notna(lcl):
        return float((float(ucl) + float(lcl)) / 2.0)
    return _resolve_target_value(spec_row)


def _add_plain_spec_line(fig: go.Figure, y_value: object, label: str, color: str) -> None:
    if pd.isna(y_value):
        return
    fig.add_hline(
        y=float(y_value),
        line_dash="dash",
        line_color=color,
        line_width=1.4,
        annotation_text=label,
        annotation_position="top right",
    )


def _apply_measurement_spec_lines(fig: go.Figure, spec_df: pd.DataFrame, row: int | None = None) -> None:
    if spec_df.empty:
        return
    spec_source = spec_df.dropna(subset=["usl", "lsl"]).head(1)
    if spec_source.empty:
        return
    spec_row = spec_source.iloc[0]
    line_func = (
        (lambda value, label, color: _add_spec_line(fig, value, label, color, row=row))
        if row is not None
        else (lambda value, label, color: _add_plain_spec_line(fig, value, label, color))
    )
    line_func(spec_row.get("usl"), "USL", "#dc2626")
    line_func(spec_row.get("lsl"), "LSL", "#dc2626")
    line_func(spec_row.get("ucl"), "UCL", "#16a34a")
    line_func(spec_row.get("lcl"), "LCL", "#16a34a")
    target_value = _resolve_target_value(spec_row)
    if target_value is not None:
        line_func(target_value, "Target", "#f97316")
    cl_value = _resolve_cl_value(spec_row)
    if cl_value is not None:
        line_func(cl_value, "CL", "#16a34a")


def _create_period_overview_chart(
    sheet_features_df: pd.DataFrame,
    period_capability_df: pd.DataFrame,
    metric_key: str,
    metric_label: str,
    title: str,
) -> go.Figure:
    """Create Figure1: M/W/D sheet_mean boxes and period CPM/CPK line trend."""
    metric_key = metric_key.lower()
    metric_label = metric_label.upper()
    fig = go.Figure()

    axis_end_date = _infer_period_axis_end_date(sheet_features_df, period_capability_df)
    period_axis_df = _period_axis_with_display(axis_end_date)
    points_df = _sheet_period_points(sheet_features_df, period_axis_df)
    capability_df = _capability_axis_frame(period_capability_df, period_axis_df, metric_key)
    ordered_labels = period_axis_df["display_label"].tolist()

    for period_type in ["month", "week", "day"]:
        type_points = points_df[points_df["period_type"] == period_type]
        type_capability = capability_df[capability_df["period_type"] == period_type]
        labels = type_capability["display_label"].tolist()
        for label in labels:
            y_values = type_points[type_points["display_label"] == label]["sheet_mean"]
            if y_values.empty:
                continue
            fig.add_trace(
                go.Box(
                    x=[label] * len(y_values),
                    y=y_values,
                    name=label,
                    boxpoints=False,
                    fillcolor=PERIOD_FILL_COLORS.get(period_type, "rgba(37, 99, 235, 0.18)"),
                    line={"color": PERIOD_COLORS.get(period_type, "#2563eb"), "width": 1.4},
                    showlegend=False,
                    width=0.42,
                    hovertemplate=f"{label}<br>Sheet Mean=%{{y:.4f}}<extra></extra>",
                ),
            )

        type_capability = type_capability.dropna(subset=[metric_key])
        if not type_capability.empty:
            custom_cols = type_capability[["sample_count", "mean_value", "std_value"]].apply(pd.to_numeric, errors="coerce")
            fig.add_trace(
                go.Scatter(
                    x=type_capability["display_label"],
                    y=type_capability[metric_key],
                    mode="lines+markers",
                    name=f"{PERIOD_LABELS.get(period_type, period_type)}{metric_label}",
                    yaxis="y2",
                    line={"color": PERIOD_COLORS.get(period_type, "#2563eb"), "width": 2.2},
                    marker={"size": 7, "symbol": "circle"},
                    connectgaps=False,
                    customdata=custom_cols.round(4),
                    hovertemplate=(
                        f"%{{x}}<br>{metric_label}=%{{y:.3f}}<br>Sheet=%{{customdata[0]}}"
                        "<br>Mean=%{customdata[1]}<br>Std=%{customdata[2]}<extra></extra>"
                    ),
                ),
            )

    spec_source = (
        sheet_features_df.dropna(subset=["usl", "lsl"]).head(1)
        if {"usl", "lsl"}.issubset(sheet_features_df.columns)
        else pd.DataFrame()
    )
    _apply_measurement_spec_lines(fig, spec_source)
    if not spec_source.empty:
        spec_row = spec_source.iloc[0]
        if pd.notna(spec_row.get("usl")) and pd.notna(spec_row.get("lsl")) and spec_row.get("usl") > spec_row.get("lsl"):
            fig.update_yaxes(range=[float(spec_row.get("lsl")), float(spec_row.get("usl"))])

    if metric_key == "cpk":
        fig.add_shape(
            type="line",
            xref="paper",
            x0=0,
            x1=1,
            yref="y2",
            y0=1.33,
            y1=1.33,
            line={"dash": "dot", "color": "#dc2626", "width": 1.4},
        )
        fig.add_annotation(
            xref="paper",
            x=1,
            yref="y2",
            y=1.33,
            text="CPK=1.33",
            showarrow=False,
            font={"color": "#dc2626", "size": 11},
            xanchor="right",
            yanchor="bottom",
        )

    fig.update_layout(
        title=title,
        height=450,
        margin={"l": 32, "r": 58, "t": 58, "b": 82},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        boxmode="group",
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        yaxis={"title": "Sheet Mean"},
        yaxis2={
            "title": metric_label,
            "overlaying": "y",
            "side": "right",
            "showgrid": False,
            "zeroline": False,
            "rangemode": "tozero",
        },
    )
    fig.update_xaxes(categoryorder="array", categoryarray=ordered_labels, tickangle=-35)
    return fig


def _resolve_chamber_column(df: pd.DataFrame) -> str:
    for column in ["chamber", "chamber_id", "sub_equip_id", "eqp_id", "main_eqp_type", "site_name"]:
        if column in df.columns:
            return column
    return ""


def _create_sheet_points_box_chart(
    raw_measurements_df: pd.DataFrame,
    sort_mode: str,
    title: str,
    spec_df: pd.DataFrame | None = None,
) -> go.Figure:
    """Create Figure2: point-level boxes by chamber/site or by Sheet pass time."""
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

    if sort_mode == "按过货时间排序":
        sorted_df = df.sort_values(["sheet_start_time", "sheet_id"])
        group_labels = sorted_df["sheet_id"].dropna().astype(str).drop_duplicates().tolist()
        for idx, sheet_id in enumerate(group_labels):
            y_values = sorted_df[sorted_df["sheet_id"].astype(str) == sheet_id]["param_value"]
            fig.add_trace(
                go.Box(
                    y=y_values,
                    name=sheet_id,
                    boxpoints=False,
                    marker_color="#1d4ed8",
                    showlegend=False,
                )
            )
    else:
        chamber_col = _resolve_chamber_column(df)
        df["chamber_label"] = df[chamber_col].fillna("UNKNOWN").astype(str) if chamber_col else "UNKNOWN"
        group_labels = sorted(df["chamber_label"].dropna().astype(str).unique().tolist())
        for idx, chamber in enumerate(group_labels):
            y_values = df[df["chamber_label"] == chamber]["param_value"]
            fig.add_trace(
                go.Box(
                    y=y_values,
                    name=chamber,
                    boxpoints=False,
                    marker_color=SHEET_BOX_PALETTE[idx % len(SHEET_BOX_PALETTE)],
                    showlegend=True,
                )
            )

    if spec_df is not None:
        _apply_measurement_spec_lines(fig, spec_df)
        spec_source = spec_df.dropna(subset=["usl", "lsl"]).head(1)
        if not spec_source.empty:
            spec_row = spec_source.iloc[0]
            if pd.notna(spec_row.get("usl")) and pd.notna(spec_row.get("lsl")) and spec_row.get("usl") > spec_row.get("lsl"):
                fig.update_yaxes(range=[float(spec_row.get("lsl")), float(spec_row.get("usl"))])

    fig.update_layout(
        title=title,
        height=430,
        margin={"l": 32, "r": 24, "t": 56, "b": 80},
        xaxis_title=None,
        yaxis_title="Param Value",
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
    )
    fig.update_xaxes(tickangle=-45)
    return fig


def render_cpm_indicator_sections(
    period_capability_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    metric_key: str = "cpm",
    metric_label: str = "CPM",
) -> None:
    """Render one expander per monitoring indicator with Task2 distribution figures."""
    metric_key = metric_key.lower()
    metric_label = metric_label.upper()
    if sheet_features_df.empty:
        st.info(f"当前筛选条件下无 {metric_label} 数据。")
        return

    grouped = sheet_features_df.groupby(["factory", "step_id", "param_name"], sort=True)
    for (factory, step_id, param_name), indicator_features_df in grouped:
        label = f"{factory} | {step_id} | {param_name}"
        if {"factory", "step_id", "param_name"}.issubset(period_capability_df.columns):
            indicator_capability_df = period_capability_df[
                (period_capability_df["factory"].astype(str) == str(factory))
                & (period_capability_df["step_id"].astype(str) == str(step_id))
                & (period_capability_df["param_name"].astype(str) == str(param_name))
            ].copy()
        else:
            indicator_capability_df = pd.DataFrame()
        if {"factory", "step_id", "param_name"}.issubset(raw_measurements_df.columns):
            indicator_raw_df = raw_measurements_df[
                (raw_measurements_df["factory"].astype(str) == str(factory))
                & (raw_measurements_df["step_id"].astype(str) == str(step_id))
                & (raw_measurements_df["param_name"].astype(str) == str(param_name))
            ].copy()
        else:
            indicator_raw_df = pd.DataFrame()

        with st.expander(label, expanded=True):
            metric_cols = st.columns(4)
            metric_cols[0].metric("Sheet数", int(indicator_features_df["sheet_id"].nunique()))
            metric_cols[1].metric("点位数", int(len(indicator_raw_df)))
            metric_cols[2].metric("周期数", int(len(indicator_capability_df)))
            if metric_key in indicator_capability_df.columns and not indicator_capability_df.empty:
                metric_cols[3].metric(f"中位{metric_label}", f"{indicator_capability_df[metric_key].median():.3f}")
            else:
                metric_cols[3].metric(f"中位{metric_label}", "-")

            if indicator_capability_df.empty:
                st.warning("当前监控指标可用于箱线图展示，但周期 CPM/CPK 样本不足。")

            _, sort_col = st.columns([1.08, 1], gap="large")
            with sort_col:
                sort_mode = st.selectbox(
                    "Sheet排序",
                    options=["按腔室排序", "按过货时间排序"],
                    key=f"cpm_sheet_sort_{factory}_{step_id}_{param_name}",
                )

            period_col, sheet_col = st.columns([1.08, 1], gap="large")
            with period_col:
                fig1 = _create_period_overview_chart(
                    sheet_features_df=indicator_features_df,
                    period_capability_df=indicator_capability_df,
                    metric_key=metric_key,
                    metric_label=metric_label,
                    title=f"{label} | 月周天分布与{metric_label}趋势",
                )
                st.plotly_chart(fig1, width="stretch")

            with sheet_col:
                fig2 = _create_sheet_points_box_chart(
                    raw_measurements_df=indicator_raw_df,
                    sort_mode=sort_mode,
                    title=f"{label} | Sheet点位分布",
                    spec_df=indicator_features_df,
                )
                st.plotly_chart(fig2, width="stretch")
