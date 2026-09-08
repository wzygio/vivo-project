"""Query-gated CPK warning dashboard presentation."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Protocol

import streamlit as st

from src.inline_domain.application.monitor.cpk_monitor_service import (
    CpkMonitorViewModel,
)

logger = logging.getLogger(__name__)


class CpkDashboardService(Protocol):
    def build_dashboard(
        self, *, products: Sequence[str], factories: Sequence[str],
    ) -> CpkMonitorViewModel: ...


def render_cpk_monitor_results(view: CpkMonitorViewModel) -> None:
    from app.sections.inline_domain.monitor.oos_monitor_dashboard import build_period_trend_chart

    st.markdown("#### CPK 汇总表")
    st.dataframe(view.summary_df.astype(str), hide_index=True, width="stretch")
    st.markdown("#### CPK 预警趋势")
    st.altair_chart(build_period_trend_chart(
        view.summary_df, label_column="指标", metric_rows=("预警项目数",),
    ), width="stretch")
    st.caption("上一完整周及当月：从 CPK 修饰表统计未修饰且 CPK＜1.33 的项目；总项目数取汇总 Excel。年、季和其他历史保持维护值。图表缺失值仅按0占位，不回写历史。")
    if st.query_params.get("admin") == "true" and not view.refresh_status_df.empty:
        st.markdown("#### CPK 数据更新状态（管理员）")
        st.dataframe(view.refresh_status_df, hide_index=True, width="stretch")
    if view.detail_df.empty:
        st.info("暂无所选最新周期的 CPK 项目明细。")
        return
    st.markdown("#### CPK 最新项目明细")
    display = view.detail_df.rename(columns={
        "prod_code": "产品", "factory": "厂别", "step_id": "站点",
        "param_name": "参数", "period_label": "周期", "period_type": "周期类型",
        "cpk_corrected": "CPK", "flag": "已修饰", "status": "判定状态",
    })
    columns = [column for column in ("周期类型", "周期", "产品", "厂别", "站点", "参数", "CPK", "已修饰", "判定状态") if column in display]
    st.dataframe(display[columns], hide_index=True, width="stretch")


def render_cpk_monitor_section(
    service: CpkDashboardService, available_products: Sequence[str],
    available_factories: Sequence[str],
) -> None:
    columns = st.columns([2.6, 1.6, 0.8], vertical_alignment="bottom")
    products = columns[0].multiselect(
        "产品型号", available_products, default=available_products, key="cpk_monitor_products",
    )
    factories = columns[1].multiselect(
        "厂别", available_factories, default=available_factories, key="cpk_monitor_factories",
    )
    clicked = columns[2].button("查询", type="primary", key="cpk_monitor_query_submit")
    signature = (tuple(sorted(products)), tuple(sorted(factories)))
    if clicked:
        st.session_state["cpk_monitor_query_signature"] = signature
    if st.session_state.get("cpk_monitor_query_signature") != signature:
        st.info("选择产品和厂别后，点击查询生成 CPK 预警看板。")
        return
    if not products or not factories:
        st.warning("请至少选择一个产品和厂别。")
        return
    try:
        with st.spinner("正在生成 CPK 预警看板…"):
            view = service.build_dashboard(products=products, factories=factories)
    except Exception:
        logger.exception("CPK warning dashboard query failed")
        st.error("CPK 查询失败，请检查规格、修饰配置和汇总工作簿是否可读取。")
        return
    render_cpk_monitor_results(view)
