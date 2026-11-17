"""Query-gated CPK/CPM project counts and pass-rate trends."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Protocol

import streamlit as st

from app.components.product_labels import get_product_label_formatter
from app.charts.inline_domain.capability_trend_chart import create_capability_trend_chart
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
    name = metric.upper()
    st.markdown(f"#### {name} 汇总表")
    st.dataframe(view.summary_df.astype(str), hide_index=True, width="stretch")
    st.markdown("#### 全产品汇总趋势图")
    st.plotly_chart(
        create_capability_trend_chart(view.summary_df, metric=metric),
        width="stretch", key=f"{metric}_monitor_pass_rate_chart", config={"scrollZoom": False},
    )
    product_label = get_product_label_formatter()
    for product, summary in view.product_summary_dfs.items():
        st.markdown(f"#### {product_label(product)} {name} 达标率趋势图")
        st.plotly_chart(
            create_capability_trend_chart(summary, metric=metric),
            width="stretch", key=f"{metric}_monitor_pass_rate_chart_{product}",
            config={"scrollZoom": False},
        )
    if st.query_params.get("admin") == "true" and not view.refresh_status_df.empty:
        st.markdown(f"#### {name} 数据更新状态（管理员）")
        st.dataframe(view.refresh_status_df, hide_index=True, width="stretch")


@st.fragment
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
    available_products = list(available_products)
    product_key = f"{prefix}_products"
    if product_key in st.session_state:
        st.session_state[product_key] = [value for value in st.session_state[product_key] if value in available_products]
    columns = st.columns([4.2, 0.8], vertical_alignment="bottom")
    products = columns[0].multiselect(
        "产品型号", available_products,
        default=available_products if product_key not in st.session_state else None, key=product_key,
        format_func=get_product_label_formatter(),
    )
    products = products or available_products
    clicked = columns[1].button("查询", type="primary", key=f"{prefix}_query_submit")
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
        with st.spinner(f"正在生成 {name} 达标率趋势图…"):
            view = service.build_dashboard(products=products, factories=available_factories)
    except Exception:
        logger.exception("%s warning dashboard query failed", name)
        st.error(f"{name} 数据读取失败，请稍后重试。")
        return
    render_results(view)
