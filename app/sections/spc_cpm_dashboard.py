from __future__ import annotations

from datetime import date
from typing import Iterable

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.spc_domain.core.cpm_calculator import derive_lot_id

CPM_FACTORY_OPTIONS = ["ARRAY", "OLED", "TP"]
CPM_METRIC_OPTIONS = ["CPM", "CPK"]


def get_default_cpm_start_date(end_date: date) -> date:
    """Return the first day of the three-month CPM reporting window."""
    month = end_date.month - 2
    year = end_date.year
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1)


def _normalise_selection(selection: Iterable[str], available: list[str]) -> list[str]:
    selected = [item for item in selection if item in available]
    return selected


def _unique_sorted(df: pd.DataFrame, column: str) -> list[str]:
    if df.empty or column not in df.columns:
        return []
    return sorted(df[column].dropna().astype(str).unique().tolist())


def get_available_factories(lot_cpm_df: pd.DataFrame) -> list[str]:
    """Return available factories with known factory order preserved."""
    factories = set(_unique_sorted(lot_cpm_df, "factory"))
    ordered = [factory for factory in CPM_FACTORY_OPTIONS if factory in factories]
    extras = sorted(factories.difference(CPM_FACTORY_OPTIONS))
    return ordered + extras


def get_steps_for_factory(lot_cpm_df: pd.DataFrame, selected_factory: str) -> list[str]:
    """Return stations available under the selected factory."""
    if lot_cpm_df.empty or not selected_factory:
        return []
    factory_df = lot_cpm_df[lot_cpm_df["factory"].astype(str) == str(selected_factory)]
    return _unique_sorted(factory_df, "step_id")


def get_params_for_factory_steps(
    lot_cpm_df: pd.DataFrame,
    selected_factory: str,
    selected_steps: list[str],
) -> list[str]:
    """Return parameters available under the selected factory and stations."""
    if lot_cpm_df.empty or not selected_factory or not selected_steps:
        return []
    df = lot_cpm_df[
        (lot_cpm_df["factory"].astype(str) == str(selected_factory))
        & (lot_cpm_df["step_id"].astype(str).isin(selected_steps))
    ]
    return _unique_sorted(df, "param_name")


def _filter_signature(
    selected_factory: str,
    selected_steps: list[str],
    selected_params: list[str],
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    return selected_factory, tuple(selected_steps), tuple(selected_params)


def render_cpm_filters(lot_cpm_df: pd.DataFrame) -> tuple[str, str, list[str], list[str], bool]:
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

        available_factories = get_available_factories(lot_cpm_df)
        if not available_factories:
            available_factories = CPM_FACTORY_OPTIONS
        factory_key = "cpm_factory_filter"
        if st.session_state.get(factory_key) not in available_factories:
            st.session_state[factory_key] = available_factories[0]

        with c_factory:
            selected_factory = st.selectbox(
                "厂别",
                options=available_factories,
                key="cpm_factory_filter",
            )

        available_steps = get_steps_for_factory(lot_cpm_df, selected_factory)
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

        available_params = get_params_for_factory_steps(lot_cpm_df, selected_factory, selected_steps)
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
    lot_cpm_df: pd.DataFrame,
    selected_factory: str,
    selected_params: list[str],
    selected_steps: list[str],
) -> pd.DataFrame:
    """Apply frontend CPM filters. Empty param/step selections mean no narrowing."""
    if lot_cpm_df.empty:
        return lot_cpm_df

    df = lot_cpm_df.copy()
    if selected_factory:
        df = df[df["factory"].astype(str) == str(selected_factory)]
    if selected_params:
        df = df[df["param_name"].astype(str).isin(selected_params)]
    if selected_steps:
        df = df[df["step_id"].astype(str).isin(selected_steps)]
    return df.reset_index(drop=True)


def _create_lot_cpm_chart(
    indicator_df: pd.DataFrame,
    title: str,
    metric_key: str = "cpm",
    metric_label: str = "CPM",
) -> go.Figure:
    metric_key = metric_key.lower()
    metric_label = metric_label.upper()
    sort_cols = [col for col in ["last_sheet_time", "lot_id"] if col in indicator_df.columns]
    chart_df = indicator_df.dropna(subset=[metric_key]).copy()
    chart_df = chart_df.sort_values(sort_cols).copy() if sort_cols else chart_df
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=chart_df["lot_id"],
            y=chart_df[metric_key],
            marker={"color": "#2563eb"},
            customdata=chart_df[["sheet_count", "lot_mean", "lot_std"]].round(4),
            hovertemplate=(
                f"Lot=%{{x}}<br>{metric_label}=%{{y:.3f}}<br>Sheet=%{{customdata[0]}}"
                "<br>Mean=%{customdata[1]}<br>Std=%{customdata[2]}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        title=title,
        xaxis_title="Lot ID",
        yaxis_title=metric_label,
        height=320,
        bargap=0.35,
        margin={"l": 20, "r": 20, "t": 48, "b": 20},
    )
    return fig


def _sheet_detail_for_lot(sheet_measurements_df: pd.DataFrame, lot_id: str, step_id: str, param_name: str) -> pd.DataFrame:
    if sheet_measurements_df.empty:
        return pd.DataFrame()

    df = sheet_measurements_df.copy()
    df["lot_id"] = df["sheet_id"].apply(derive_lot_id)
    detail = df[
        (df["lot_id"] == lot_id)
        & (df["step_id"].astype(str) == str(step_id))
        & (df["param_name"].astype(str) == str(param_name))
    ].copy()
    if detail.empty:
        return detail

    keep_cols = [
        "lot_id",
        "sheet_id",
        "factory",
        "step_id",
        "param_name",
        "sheet_mean",
    ]
    keep_cols = [col for col in keep_cols if col in detail.columns]
    detail = detail[keep_cols].sort_values("sheet_id").reset_index(drop=True)
    return detail.rename(
        columns={
            "factory": "厂别",
            "step_id": "站点",
            "param_name": "参数名称",
        }
    )


def render_cpm_indicator_sections(
    lot_cpm_df: pd.DataFrame,
    sheet_measurements_df: pd.DataFrame,
    metric_key: str = "cpm",
    metric_label: str = "CPM",
) -> None:
    """Render one expander per monitoring indicator with Lot trend and Sheet drilldown."""
    metric_key = metric_key.lower()
    metric_label = metric_label.upper()
    if lot_cpm_df.empty:
        st.info(f"当前筛选条件下无 {metric_label} 数据。")
        return

    grouped = lot_cpm_df.groupby(["factory", "step_id", "param_name"], sort=True)
    for (factory, step_id, param_name), indicator_df in grouped:
        label = f"{factory} | {step_id} | {param_name}"
        with st.expander(label, expanded=True):
            metric_cols = st.columns(4)
            metric_cols[0].metric("Lot数", len(indicator_df))
            metric_cols[1].metric(f"最低{metric_label}", f"{indicator_df[metric_key].min():.3f}")
            metric_cols[2].metric(f"中位{metric_label}", f"{indicator_df[metric_key].median():.3f}")
            metric_cols[3].metric("Sheet总数", int(indicator_df["sheet_count"].sum()))

            fig = _create_lot_cpm_chart(
                indicator_df=indicator_df,
                title=label,
                metric_key=metric_key,
                metric_label=metric_label,
            )
            event = st.plotly_chart(fig, width="stretch", on_select="rerun", selection_mode="points")

            selected_lot = ""
            if event and event.selection and event.selection.get("points"):  # type: ignore[attr-defined]
                selected_lot = str(event.selection["points"][0]["x"])  # type: ignore[index]

            manual_lot = st.selectbox(
                "Lot 下钻",
                options=[""] + indicator_df["lot_id"].astype(str).tolist(),
                index=0,
                key=f"cpm_lot_select_{metric_key}_{factory}_{step_id}_{param_name}",
            )
            target_lot = selected_lot or manual_lot
            if not target_lot:
                st.caption("点击趋势图中的 Lot 点，或在下拉框选择 Lot 查看 Sheet 测量值。")
                continue

            sheet_detail = _sheet_detail_for_lot(sheet_measurements_df, target_lot, str(step_id), str(param_name))
            if sheet_detail.empty:
                st.warning(f"未找到 Lot {target_lot} 的 Sheet 测量值。")
            else:
                st.dataframe(sheet_detail, width="stretch", hide_index=True, height=260)
