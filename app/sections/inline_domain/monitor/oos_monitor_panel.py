"""Independently rerun the OOS board; payload cache remains page-owned."""
import logging
import streamlit as st
from app.sections.inline_domain.monitor.oos_monitor_dashboard import (
    MONITOR_QUERY_SIGNATURE_KEY, render_oos_monitor_control_panel,
    render_oos_monitor_results, render_oos_refresh_status, render_monitor_query_button,
    render_monitor_query_gate, selected_scopes,
)
from app.sections.inline_domain.monitor.refresh_controls import render_board_refresh_controls
from app.sections.inline_domain.monitor import refresh_controls
from src.inline_domain.application.monitor.oos_monitor_service import OosMonitorViewModel
from src.inline_domain import composition
from app.sections.inline_domain.monitor import oos_monitor_dashboard as dashboard

@st.fragment
def render_oos_monitor_panel(*, available_products, factory_options, load_payload,
                             start_date_str, end_date_str, step_desc_map, is_admin):
    st.subheader("⚠️ 异常片预警看板")
    with st.expander("Inline超规预警", expanded=True):
        available_factories = factory_options

        # 4. 组装积木: 渲染控制台（「查询」按钮与筛选三件套同行最右列，
        # 点击状态经 box 传回门控——按钮渲染先于签名计算）
        query_click_box: dict[str, bool] = {}

        def _render_query_button_in_row() -> None:
            query_click_box["clicked"] = render_monitor_query_button()

        filter_state = dashboard.render_oos_monitor_control_panel(
            available_products,
            available_factories,
            action_renderer=_render_query_button_in_row,
        )

        def _clear_oos_monitor_cache() -> None:
            refresh_controls.clear_oos_source_cache(selected_scopes(filter_state.data_type_filter))
            load_payload.clear()

        refresh_oos_data = render_board_refresh_controls(
            key="oos_monitor", clear_cache=_clear_oos_monitor_cache,
            query_state_key=MONITOR_QUERY_SIGNATURE_KEY,
            selection_valid=bool(filter_state.selected_products and filter_state.selected_factories),
        )
        if render_monitor_query_gate(
            filter_state, clicked=query_click_box.get("clicked", False) or refresh_oos_data
        ):
            products = tuple(filter_state.selected_products)
            scopes = selected_scopes(filter_state.data_type_filter)
            factories = tuple(filter_state.selected_factories)
            try:
                history_service = composition.build_oos_history_service()
                ooc_history_service = composition.build_ooc_history_service()
                summary_workbook_service = composition.build_monitor_summary_workbook_service()
                source_signature = "|".join(
                    [
                        history_service.source_signature(list(products), list(scopes)),
                        ooc_history_service.source_signature(list(products), list(scopes)),
                    ]
                )
                summary_workbook_signature = summary_workbook_service.source_signature()
                with st.spinner("正在读取 Excel 超规明细与汇总表..."):
                    payload = load_payload(
                        products,
                        scopes,
                        factories,
                        start_date_str,
                        end_date_str,
                        source_signature,
                        summary_workbook_signature,
                    )
            except Exception:
                logging.exception("共享超规历史读取失败")
                st.error("超规历史读取失败，请确认数据文件可用且未被占用后重试。")
            else:
                view_model = OosMonitorViewModel(**payload)
                dashboard.render_oos_monitor_results(view_model, step_desc_map=step_desc_map)
                if is_admin:
                    st.divider()
                    dashboard.render_oos_refresh_status(view_model.refresh_status_df)
                    st.caption("请在各模块超规明细 Excel 的产品 sheet 中维护 Flag，随后点击页头刷新缓存并重新查询。看板不再合并旧决策台账。")
