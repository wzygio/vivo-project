"""Query-gated CPK warning dashboard presentation."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Protocol

import streamlit as st

from app.components.product_labels import get_product_label_formatter
from src.inline_domain.application.monitor.cpk_monitor_service import (
    CpkMonitorViewModel,
)
from app.sections.inline_domain.monitor.refresh_controls import (
    clear_cpk_source_cache,
    render_board_refresh_controls,
)

logger = logging.getLogger(__name__)


class CpkDashboardService(Protocol):
    def build_dashboard(
        self, *, products: Sequence[str], factories: Sequence[str],
    ) -> CpkMonitorViewModel: ...


def render_cpk_monitor_results(view: CpkMonitorViewModel) -> None:
    render_capability_monitor_results(view, metric="cpk")


def render_capability_monitor_results(view: CpkMonitorViewModel, *, metric: str) -> None:
    from app.sections.inline_domain.monitor.oos_monitor_dashboard import build_period_trend_chart

    name = metric.upper()
    st.markdown(f"#### {name} 汇总表")
    st.dataframe(view.summary_df.astype(str), hide_index=True, width="stretch")
    st.markdown(f"#### {name} 预警趋势")
    st.altair_chart(build_period_trend_chart(
        view.summary_df, label_column="指标", metric_rows=("预警项目数",),
    ), width="stretch")
    if st.query_params.get("admin") == "true" and not view.refresh_status_df.empty:
        st.markdown(f"#### {name} 数据更新状态（管理员）")
        st.dataframe(view.refresh_status_df, hide_index=True, width="stretch")
    if view.detail_df.empty:
        st.info(f"暂无所选最新周期的 {name} 项目明细。")
        return
    st.markdown(f"#### {name} 最新项目明细")
    display = view.detail_df.rename(columns={
        "prod_code": "产品", "factory": "厂别", "step_id": "站点",
        "param_name": "参数", "period_label": "周期", "period_type": "周期类型",
        f"{metric}_corrected": name, "flag": "已达标", "status": "判定状态",
    })
    if "判定状态" in display:
        display["判定状态"] = display["判定状态"].replace("已修饰或达标", "达标")
    columns = [column for column in ("周期类型", "周期", "产品", "厂别", "站点", "参数", name, "已达标", "判定状态") if column in display]
    st.dataframe(display[columns], hide_index=True, width="stretch")


def render_cpk_monitor_section(
    service: CpkDashboardService, available_products: Sequence[str],
    available_factories: Sequence[str],
) -> None:
    render_capability_monitor_section(
        service, available_products, available_factories, metric="cpk",
        clear_cache=clear_cpk_source_cache, render_results=render_cpk_monitor_results,
    )


def render_capability_monitor_section(
    service: CpkDashboardService, available_products: Sequence[str],
    available_factories: Sequence[str], *, metric: str,
    clear_cache: Callable[[], None], render_results: Callable[[CpkMonitorViewModel], None],
) -> None:
    name, prefix = metric.upper(), f"{metric}_monitor"
    columns = st.columns([2.6, 1.6, 0.8], vertical_alignment="bottom")
    products = columns[0].multiselect(
        "产品型号", available_products, default=available_products, key=f"{prefix}_products",
        format_func=get_product_label_formatter(),
    )
    factories = columns[1].multiselect(
        "厂别", available_factories, default=available_factories, key=f"{prefix}_factories",
    )
    clicked = columns[2].button("查询", type="primary", key=f"{prefix}_query_submit")
    signature = tuple(sorted(products))
    refresh_data = render_board_refresh_controls(
        key=prefix, clear_cache=clear_cache,
        query_state_key=f"{prefix}_query_signature",
        selection_valid=bool(products),
    )
    if clicked or refresh_data:
        st.session_state[f"{prefix}_query_signature"] = signature
    if st.session_state.get(f"{prefix}_query_signature") != signature:
        return
    if not products:
        st.warning("请至少选择一个产品。")
        return
    try:
        with st.spinner(f"正在生成 {name} 预警看板…"):
            view = service.build_dashboard(products=products, factories=factories)
    except Exception:
        logger.exception("%s warning dashboard query failed", name)
        st.error(f"{name} 数据读取失败，请稍后重试。")
        return
    render_results(view)
