from __future__ import annotations

from datetime import date
from functools import partial

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.manager.render_gate import RenderGate
from app.sections.inline_domain.shared import (
    INLINE_FACTORY_OPTIONS,
    apply_report_filter,
    get_available_factories as _shared_available_factories,
    get_options_for_factory_steps,
    get_steps_for_factory as _shared_steps_for_factory,
    render_cascade_filters,
    render_sheet_oos_decoration_admin,
    resolve_chart_type,
)
from app.sections.inline_domain.shared.sheet_charts import (
    create_period_overview_chart,
    create_sheet_points_box_charts,
)
from app.utils.step_labels import format_step_label
from src.inline_domain.core.spc.spc_calculator import get_period_window_start
from src.inline_domain.core.shared.sheet_oos_decoration import SheetOosDecorationResult
from src.shared_kernel.config import ConfigLoader

CTQ_FACTORY_OPTIONS = INLINE_FACTORY_OPTIONS

# 公共管线别名：供本模块组合层引用，并保持既有测试的 monkeypatch 锚点。
_create_sheet_points_box_charts = create_sheet_points_box_charts


def get_default_ctq_start_date(end_date: date) -> date:
    """Return the first date needed by CTQ month/week/day distributions."""
    return get_period_window_start(end_date)


def get_available_factories(report_df: pd.DataFrame) -> list[str]:
    """Return available CTQ factories with the standard order preserved."""
    return _shared_available_factories(report_df, CTQ_FACTORY_OPTIONS)


def get_steps_for_factory(report_df: pd.DataFrame, selected_factory: str) -> list[str]:
    return _shared_steps_for_factory(report_df, selected_factory)


def get_params_for_factory_steps(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_steps: list[str],
) -> list[str]:
    return get_options_for_factory_steps(report_df, selected_factory, selected_steps, "param_name")


def render_ctq_filters(
    indicator_df: pd.DataFrame,
    *,
    step_desc_map: dict[str, str] | None = None,
) -> tuple[str, list[str], list[str], bool]:
    """Render CTQ-owned cascade filters and return the applied query state."""
    return render_cascade_filters(
        indicator_df,
        key_prefix="ctq",
        third_label="参数名称",
        third_column="param_name",
        third_kind="param",
        factory_options=CTQ_FACTORY_OPTIONS,
        step_desc_map=step_desc_map,
    )


def filter_ctq_report(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_params: list[str],
    selected_steps: list[str],
) -> pd.DataFrame:
    """Apply CTQ filters to a report frame."""
    return apply_report_filter(
        report_df,
        selected_factory,
        selected_params,
        selected_steps,
        third_column="param_name",
    )


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
    period_box_source: str = "point_value",
) -> go.Figure:
    """Create a capability-free CTQ month/week/day distribution figure."""
    return create_period_overview_chart(
        sheet_features_df=sheet_features_df,
        period_capability_df=pd.DataFrame(),
        raw_measurements_df=raw_measurements_df,
        period_box_source=period_box_source,
        title=title,
    )


def _build_ctq_indicator_render_payload(
    label: str,
    chart_type: str,
    indicator_features_df: pd.DataFrame,
    indicator_raw_df: pd.DataFrame,
    period_box_source: str,
) -> dict[str, object]:
    """[RenderGate 阶段1] 纯计算：构建单个指标的全部图表，禁止触碰 st.*。"""
    period_figure = create_ctq_period_overview_chart(
        sheet_features_df=indicator_features_df,
        raw_measurements_df=indicator_raw_df,
        title=f"{label} | 月周天分布",
        period_box_source=period_box_source,
    )
    chamber_figure, time_figure = _create_sheet_points_box_charts(
        raw_measurements_df=indicator_raw_df,
        title_prefix=label,
        spec_df=indicator_features_df,
        chart_type=chart_type,
    )
    return {
        "label": label,
        "period_figure": period_figure,
        "chamber_figure": chamber_figure,
        "time_figure": time_figure,
    }


def _render_ctq_indicator_payload(payload: dict[str, object]) -> None:
    """[RenderGate 阶段2] 集中渲染：仅执行 st.* 调用，不做任何重计算。"""
    with st.expander(payload["label"], expanded=True):
        st.plotly_chart(payload["period_figure"], width="stretch")
        st.plotly_chart(payload["chamber_figure"], width="stretch")
        st.plotly_chart(payload["time_figure"], width="stretch")


def render_ctq_indicator_sections(
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    period_box_source: str = "point_value",
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """Render capability-free CTQ distribution figures by indicator.

    两阶段渲染：先在 RenderGate 统一 spinner 下构建全部图表，再集中回流渲染，
    避免图表随计算进度一张一张跳出导致页面抖动卡顿。
    """
    if sheet_features_df.empty:
        st.info("当前筛选条件下无 CTQ 数据。")
        return

    gate = RenderGate()
    line_param_name_contains = ConfigLoader.get_spc_line_chart_param_name_contains()
    grouped = sheet_features_df.groupby(["factory", "step_id", "param_name"], sort=True)
    for (factory, step_id, param_name), indicator_features_df in grouped:
        label = f"{factory} | {format_step_label(step_id, step_desc_map)} | {param_name}"
        if {"factory", "step_id", "param_name"}.issubset(raw_measurements_df.columns):
            indicator_raw_df = raw_measurements_df[
                (raw_measurements_df["factory"].astype(str) == str(factory))
                & (raw_measurements_df["step_id"].astype(str) == str(step_id))
                & (raw_measurements_df["param_name"].astype(str) == str(param_name))
            ].copy()
        else:
            indicator_raw_df = pd.DataFrame()
        chart_type = resolve_chart_type(param_name, line_param_name_contains)
        gate.stage(
            partial(
                _build_ctq_indicator_render_payload,
                label=label,
                chart_type=chart_type,
                indicator_features_df=indicator_features_df,
                indicator_raw_df=indicator_raw_df,
                period_box_source=period_box_source,
            )
        )

    for payload in gate.collect():
        _render_ctq_indicator_payload(payload)
