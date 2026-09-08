"""Query-gated CPK warning dashboard presentation."""

from __future__ import annotations

import logging
from collections.abc import Sequence

import streamlit as st

from src.inline_domain.application.monitor.cpk_monitor_service import (
    CpkMonitorService, CpkMonitorViewModel,
)

logger = logging.getLogger(__name__)


def render_cpk_monitor_results(view: CpkMonitorViewModel) -> None:
    st.markdown("#### CPK 汇总表")
    st.dataframe(view.summary_df.astype(str), hide_index=True, width="stretch")
    st.caption("达标条件：CPK ≥ 1.33。项目按产品、厂别、站点、参数统计；无法计算的项目不计入达标率。")
    current = view.detail_df[view.detail_df["period_type"].eq("month")]
    with st.container(horizontal=True):
        st.metric("当月有效项目", int(current["cpk"].notna().sum()), border=True)
        st.metric("当月达标项目", int(current["status"].eq("达标").sum()), border=True)
        st.metric("当月预警项目", int(current["status"].eq("预警").sum()), border=True)
    warnings = view.detail_df[view.detail_df["status"].ne("达标")]
    st.markdown("#### CPK 预警指标明细")
    if warnings.empty:
        st.info("当前年、季、月、周没有 CPK 预警或无法计算的指标。")
        return
    display = warnings.rename(columns={
        "prod_code": "产品", "factory": "厂别", "step_id": "站点",
        "param_name": "参数", "period_label": "周期", "sample_count": "Sheet数",
        "cpk": "CPK", "status": "状态", "mean_value": "均值",
        "std_value": "标准差", "usl": "规格上限", "lsl": "规格下限",
    })
    st.dataframe(
        display[["周期", "产品", "厂别", "站点", "参数", "Sheet数", "CPK", "状态",
                 "均值", "标准差", "规格上限", "规格下限"]],
        hide_index=True, width="stretch",
    )


def render_cpk_monitor_section(
    service: CpkMonitorService, available_products: Sequence[str],
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
