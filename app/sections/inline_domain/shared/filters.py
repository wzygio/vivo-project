"""Inline 报表级联筛选公共管线（spc / ctq / aoi_rs / aoi_tt 共用）。

交互契约（四个页面一致）：
- 厂别 → 站点 → 第三级（参数名称 / Code名称）三级级联；
- 厂别切换时下级选择清空；
- 站点组合变化时第三级自动全选；
- 「查询」按钮以签名门控：点击后签名一致才渲染图表区。

session key 以 ``key_prefix`` 隔离（如 ``spc_`` / ``ctq_`` / ``aoi_rs_`` / ``aoi_tt_``），
第三级键名由 ``third_kind`` 决定（``param`` 或 ``code``），与历史键保持一致。
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd
import streamlit as st

from app.sections.inline_domain.shared.constants import INLINE_FACTORY_OPTIONS
from app.utils.step_labels import format_step_label


def unique_sorted(df: pd.DataFrame, column: str) -> list[str]:
    if df.empty or column not in df.columns:
        return []
    return sorted(df[column].dropna().astype(str).unique().tolist())


def normalise_selection(selection: Iterable[str] | None, available: list[str]) -> list[str]:
    return [item for item in (selection or []) if item in available]


def filter_signature(
    selected_factory: str,
    selected_steps: list[str],
    selected_third: list[str],
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    return selected_factory, tuple(selected_steps), tuple(selected_third)


def get_available_factories(
    report_df: pd.DataFrame,
    factory_options: list[str] | None = None,
) -> list[str]:
    """Return available factories with the standard order preserved."""
    options = factory_options or INLINE_FACTORY_OPTIONS
    factories = set(unique_sorted(report_df, "factory"))
    ordered = [factory for factory in options if factory in factories]
    return ordered + sorted(factories.difference(options))


def get_steps_for_factory(report_df: pd.DataFrame, selected_factory: str) -> list[str]:
    if report_df.empty or not selected_factory or "factory" not in report_df.columns:
        return []
    factory_df = report_df[report_df["factory"].astype(str) == str(selected_factory)]
    return unique_sorted(factory_df, "step_id")


def get_options_for_factory_steps(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_steps: list[str],
    option_column: str,
) -> list[str]:
    """Return third-level options (param_name / rs_code / tt_name) under factory+steps."""
    if report_df.empty or not selected_factory or not selected_steps:
        return []
    required_columns = {"factory", "step_id", option_column}
    if not required_columns.issubset(report_df.columns):
        return []
    filtered_df = report_df[
        (report_df["factory"].astype(str) == str(selected_factory))
        & (report_df["step_id"].astype(str).isin(selected_steps))
    ]
    return unique_sorted(filtered_df, option_column)


def apply_report_filter(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_third: list[str],
    selected_steps: list[str],
    third_column: str,
) -> pd.DataFrame:
    """Apply the cascade filter to any report frame with factory/step/third columns."""
    if report_df.empty:
        return report_df
    filtered_df = report_df.copy()
    if selected_factory and "factory" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["factory"].astype(str) == str(selected_factory)]
    if selected_third and third_column in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[third_column].astype(str).isin(selected_third)]
    if selected_steps and "step_id" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["step_id"].astype(str).isin(selected_steps)]
    return filtered_df.reset_index(drop=True)


def render_cascade_filters(
    indicator_df: pd.DataFrame,
    *,
    key_prefix: str,
    third_label: str,
    third_column: str,
    third_kind: str = "param",
    factory_options: list[str] | None = None,
    step_desc_map: dict[str, str] | None = None,
) -> tuple[str, list[str], list[str], bool]:
    """Render the cascade filters and return (factory, third_values, steps, should_render)."""
    with st.container(border=True):
        st.markdown("#### 筛选")
        factory_col, step_col, third_col, query_col = st.columns(
            [1.1, 2.5, 3.4, 0.9],
            vertical_alignment="bottom",
        )
        options = factory_options or INLINE_FACTORY_OPTIONS
        available_factories = get_available_factories(indicator_df, options) or options
        factory_key = f"{key_prefix}_factory_filter"
        if st.session_state.get(factory_key) not in available_factories:
            st.session_state[factory_key] = available_factories[0]

        with factory_col:
            selected_factory = st.selectbox("厂别", options=available_factories, key=factory_key)

        available_steps = get_steps_for_factory(indicator_df, selected_factory)
        step_key = f"{key_prefix}_step_filter"
        third_key = f"{key_prefix}_{third_kind}_filter"
        previous_factory_key = f"{key_prefix}_previous_factory_filter"
        if st.session_state.get(previous_factory_key) != selected_factory:
            st.session_state[step_key] = []
            st.session_state[third_key] = []
            st.session_state[previous_factory_key] = selected_factory

        with step_col:
            st.session_state[step_key] = normalise_selection(
                st.session_state.get(step_key, []),
                available_steps,
            )
            selected_steps = st.multiselect(
                "站点",
                options=available_steps,
                key=step_key,
                format_func=lambda step: format_step_label(step, step_desc_map),
            )

        available_third = get_options_for_factory_steps(
            indicator_df,
            selected_factory,
            selected_steps,
            third_column,
        )
        steps_signature_key = f"{key_prefix}_steps_for_{third_kind}_autoselect"
        steps_signature = filter_signature(selected_factory, selected_steps, [])
        if st.session_state.get(steps_signature_key) != steps_signature:
            st.session_state[third_key] = available_third
            st.session_state[steps_signature_key] = steps_signature
        st.session_state[third_key] = normalise_selection(
            st.session_state.get(third_key, []),
            available_third,
        )

        with third_col:
            selected_third = st.multiselect(
                third_label,
                options=available_third,
                key=third_key,
                disabled=not selected_steps,
            )

        current_signature = filter_signature(selected_factory, selected_steps, selected_third)
        applied_signature_key = f"{key_prefix}_applied_filter_signature"
        can_query = bool(selected_factory and selected_steps and selected_third)
        with query_col:
            if st.button("查询", type="primary", width="stretch", disabled=not can_query):
                st.session_state[applied_signature_key] = current_signature

    should_render = bool(
        can_query and st.session_state.get(applied_signature_key) == current_signature
    )
    return selected_factory, selected_third, selected_steps, should_render
