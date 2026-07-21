from __future__ import annotations

from datetime import date
from typing import Iterable

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.sections.spc.spc_dashboard import (
    _create_period_overview_chart,
    _create_sheet_points_box_charts,
    _resolve_chart_type,
    render_sheet_oos_decoration_admin,
)
from src.inline_domain.core.spc.spc_calculator import get_period_window_start
from src.inline_domain.core.spc.spc_sheet_oos_decoration import SheetOosDecorationResult

CTQ_FACTORY_OPTIONS = ["ARRAY", "OLED", "TP"]


def get_default_ctq_start_date(end_date: date) -> date:
    """Return the first date needed by CTQ month/week/day distributions."""
    return get_period_window_start(end_date)


def _normalise_selection(selection: Iterable[str], available: list[str]) -> list[str]:
    return [item for item in selection if item in available]


def _unique_sorted(dataframe: pd.DataFrame, column: str) -> list[str]:
    if dataframe.empty or column not in dataframe.columns:
        return []
    return sorted(dataframe[column].dropna().astype(str).unique().tolist())


def get_available_factories(report_df: pd.DataFrame) -> list[str]:
    """Return available CTQ factories with the standard order preserved."""
    factories = set(_unique_sorted(report_df, "factory"))
    ordered = [factory for factory in CTQ_FACTORY_OPTIONS if factory in factories]
    return ordered + sorted(factories.difference(CTQ_FACTORY_OPTIONS))


def get_steps_for_factory(report_df: pd.DataFrame, selected_factory: str) -> list[str]:
    if report_df.empty or not selected_factory or "factory" not in report_df.columns:
        return []
    factory_df = report_df[report_df["factory"].astype(str) == str(selected_factory)]
    return _unique_sorted(factory_df, "step_id")


def get_params_for_factory_steps(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_steps: list[str],
) -> list[str]:
    if report_df.empty or not selected_factory or not selected_steps:
        return []
    required_columns = {"factory", "step_id", "param_name"}
    if not required_columns.issubset(report_df.columns):
        return []
    filtered_df = report_df[
        (report_df["factory"].astype(str) == str(selected_factory))
        & (report_df["step_id"].astype(str).isin(selected_steps))
    ]
    return _unique_sorted(filtered_df, "param_name")


def _filter_signature(
    selected_factory: str,
    selected_steps: list[str],
    selected_params: list[str],
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    return selected_factory, tuple(selected_steps), tuple(selected_params)


def render_ctq_filters(indicator_df: pd.DataFrame) -> tuple[str, list[str], list[str], bool]:
    """Render CTQ-owned cascade filters and return the applied query state."""
    with st.container(border=True):
        st.markdown("#### 筛选")
        factory_col, step_col, param_col, query_col = st.columns(
            [1.1, 2.5, 3.4, 0.9],
            vertical_alignment="bottom",
        )
        available_factories = get_available_factories(indicator_df) or CTQ_FACTORY_OPTIONS
        factory_key = "ctq_factory_filter"
        if st.session_state.get(factory_key) not in available_factories:
            st.session_state[factory_key] = available_factories[0]

        with factory_col:
            selected_factory = st.selectbox("厂别", options=available_factories, key=factory_key)

        available_steps = get_steps_for_factory(indicator_df, selected_factory)
        step_key = "ctq_step_filter"
        param_key = "ctq_param_filter"
        previous_factory_key = "ctq_previous_factory_filter"
        if st.session_state.get(previous_factory_key) != selected_factory:
            st.session_state[step_key] = []
            st.session_state[param_key] = []
            st.session_state[previous_factory_key] = selected_factory

        with step_col:
            st.session_state[step_key] = _normalise_selection(
                st.session_state.get(step_key, []),
                available_steps,
            )
            selected_steps = st.multiselect("站点", options=available_steps, key=step_key)

        available_params = get_params_for_factory_steps(
            indicator_df,
            selected_factory,
            selected_steps,
        )
        steps_signature_key = "ctq_steps_for_param_autoselect"
        steps_signature = _filter_signature(selected_factory, selected_steps, [])
        if st.session_state.get(steps_signature_key) != steps_signature:
            st.session_state[param_key] = available_params
            st.session_state[steps_signature_key] = steps_signature
        st.session_state[param_key] = _normalise_selection(
            st.session_state.get(param_key, []),
            available_params,
        )

        with param_col:
            selected_params = st.multiselect(
                "参数名称",
                options=available_params,
                key=param_key,
                disabled=not selected_steps,
            )

        current_signature = _filter_signature(selected_factory, selected_steps, selected_params)
        applied_signature_key = "ctq_applied_filter_signature"
        can_query = bool(selected_factory and selected_steps and selected_params)
        with query_col:
            if st.button("查询", type="primary", width="stretch", disabled=not can_query):
                st.session_state[applied_signature_key] = current_signature

    should_render = bool(
        can_query and st.session_state.get(applied_signature_key) == current_signature
    )
    return selected_factory, selected_params, selected_steps, should_render


def filter_ctq_report(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_params: list[str],
    selected_steps: list[str],
) -> pd.DataFrame:
    """Apply CTQ filters to a report frame."""
    if report_df.empty:
        return report_df
    filtered_df = report_df.copy()
    if selected_factory and "factory" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["factory"].astype(str) == str(selected_factory)]
    if selected_params and "param_name" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["param_name"].astype(str).isin(selected_params)]
    if selected_steps and "step_id" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["step_id"].astype(str).isin(selected_steps)]
    return filtered_df.reset_index(drop=True)


def render_ctq_decoration_admin(
    decoration_result: SheetOosDecorationResult | None,
) -> None:
    """Render the CTQ-only OOS admin panel without CPK controls."""
    with st.expander("开发者后台：CTQ 数据修饰", expanded=False):
        if decoration_result is None:
            st.info("当前没有可管理的 CTQ 超规片修饰数据。")
            return
        render_sheet_oos_decoration_admin(
            decoration_result,
            show_expander=False,
            report_name="CTQ",
            key_prefix="ctq",
        )


def create_ctq_period_overview_chart(
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    title: str,
    chart_type: str,
    period_box_source: str = "point_value",
) -> go.Figure:
    """Create a capability-free CTQ month/week/day distribution figure."""
    return _create_period_overview_chart(
        sheet_features_df=sheet_features_df,
        period_capability_df=pd.DataFrame(),
        raw_measurements_df=raw_measurements_df,
        period_box_source=period_box_source,
        chart_type=chart_type,
        title=title,
    )


def render_ctq_indicator_sections(
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    period_box_source: str = "point_value",
) -> None:
    """Render capability-free CTQ distribution figures by indicator."""
    if sheet_features_df.empty:
        st.info("当前筛选条件下无 CTQ 数据。")
        return

    grouped = sheet_features_df.groupby(["factory", "step_id", "param_name"], sort=True)
    for (factory, step_id, param_name), indicator_features_df in grouped:
        label = f"{factory} | {step_id} | {param_name}"
        if {"factory", "step_id", "param_name"}.issubset(raw_measurements_df.columns):
            indicator_raw_df = raw_measurements_df[
                (raw_measurements_df["factory"].astype(str) == str(factory))
                & (raw_measurements_df["step_id"].astype(str) == str(step_id))
                & (raw_measurements_df["param_name"].astype(str) == str(param_name))
            ].copy()
        else:
            indicator_raw_df = pd.DataFrame()
        chart_type = _resolve_chart_type(indicator_features_df, indicator_raw_df)

        with st.expander(label, expanded=True):
            period_figure = create_ctq_period_overview_chart(
                sheet_features_df=indicator_features_df,
                raw_measurements_df=indicator_raw_df,
                title=f"{label} | 月周天分布",
                chart_type=chart_type,
                period_box_source=period_box_source,
            )
            chamber_figure, time_figure = _create_sheet_points_box_charts(
                raw_measurements_df=indicator_raw_df,
                title_prefix=label,
                spec_df=indicator_features_df,
                chart_type=chart_type,
            )
            st.plotly_chart(period_figure, width="stretch")
            st.plotly_chart(chamber_figure, width="stretch")
            st.plotly_chart(time_figure, width="stretch")
