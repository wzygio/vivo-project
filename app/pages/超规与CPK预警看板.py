import sys
import logging
from pathlib import Path

current_dir = Path(__file__).resolve().parent
project_root = None
for parent in [current_dir] + list(current_dir.parents):
    if (parent / "pyproject.toml").exists():
        project_root = parent
        break
if project_root:
    root_str = str(project_root)
    src_str = str(project_root / "src")
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)

import streamlit as st
import pandas as pd

from app.manager.session_manager import SessionManager
from app.utils.app_setup import AppSetup
from app.sections.inline_domain.monitor.oos_monitor_dashboard import (
    MONITOR_QUERY_SIGNATURE_KEY,
    render_oos_monitor_control_panel,
    render_oos_monitor_results,
    render_oos_refresh_status,
    render_monitor_query_button,
    render_monitor_query_gate,
    selected_scopes,
)
from app.sections.inline_domain.monitor.refresh_controls import (
    clear_oos_source_cache,
    render_board_refresh_controls,
)
from src.inline_domain.application.monitor.oos_monitor_service import (
    OosMonitorService,
    OosMonitorViewModel,
)
from app.sections.inline_domain.monitor.cpk_monitor_dashboard import render_cpk_monitor_section
from src.inline_domain.composition import (
    build_monitor_summary_workbook_service,
    build_ooc_history_service,
    build_oos_history_service,
    build_cpk_monitor_service,
)
from src.shared_kernel.config import ConfigLoader

MONITOR_FACTORY_OPTIONS = ["ARRAY", "OLED", "TP"]


@st.cache_data(show_spinner=False, ttl=ConfigLoader.get_cache_ttl_seconds())
def get_cached_query_window(current_date: str) -> tuple[str, str]:
    """Include the whole current ISO week; the date key advances after midnight."""
    today = pd.Timestamp(current_date).normalize()
    week_start = today - pd.Timedelta(days=today.weekday())
    start = min(today.replace(month=1, day=1), week_start)
    return start.date().isoformat(), today.date().isoformat()


@st.cache_data(show_spinner=False, ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=16)
def get_cached_oos_monitor_payload(
    products: tuple[str, ...],
    scopes: tuple[str, ...],
    factories: tuple[str, ...],
    start_date: str,
    end_date: str,
    source_signature: str,
    summary_workbook_signature: str,
    period_layout_version: str = "year-quarter-3month-4week-v1",
) -> dict[str, pd.DataFrame]:
    del source_signature, summary_workbook_signature, period_layout_version
    view = OosMonitorService(
        build_oos_history_service(),
        ooc_reader=build_ooc_history_service(),
        summary_workbook=build_monitor_summary_workbook_service(),
    ).build_dashboard(
        products=products,
        scopes=scopes,
        factories=factories,
        start_date=start_date,
        end_date=end_date,
    )
    return {
        "detail_df": view.detail_df,
        "summary_df": view.summary_df,
        "trend_df": view.trend_df,
        "station_df": view.station_df,
        "refresh_status_df": view.refresh_status_df,
        "period_summary_df": view.period_summary_df,
    }


st.set_page_config(page_title="超规与CPK预警看板", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

# [权限控制] 检测 URL 参数，控制管理员刷新入口与状态面板显示
query_params = st.query_params
is_admin = query_params.get("admin") == "true"

# ==============================================================================
#  数据加载
# ==============================================================================
try:
    step_desc_map = {}
    start_date_str, end_date_str = get_cached_query_window(
        pd.Timestamp.today().date().isoformat()
    )
except Exception:
    logging.exception("超规与CPK预警看板初始化失败")
    st.error("页面初始化失败，请稍后重试。")
    st.stop()

# --------------------------------------------------------------------------
# 模块二：共享超规事实看板（「查询」门控）
# 页面打开不读取历史：未点击「查询」时 Expander 内只有控制台与查询
# 按钮（无 info 文案，门控语义由按钮承担）；点击后普通 rerun 保持已提交
# 状态；筛选 signature 变化静默回到未提交态；
# 本看板刷新只清除自身缓存和查询状态。
# --------------------------------------------------------------------------
st.subheader("⚠️ 超规片预警看板")
with st.expander("Inline超规预警", expanded=True):
    available_products = SessionManager.AVAILABLE_PRODUCTS
    available_factories = MONITOR_FACTORY_OPTIONS

    # 4. 组装积木: 渲染控制台（「查询」按钮与筛选三件套同行最右列，
    # 点击状态经 box 传回门控——按钮渲染先于签名计算）
    query_click_box: dict[str, bool] = {}

    def _render_query_button_in_row() -> None:
        query_click_box["clicked"] = render_monitor_query_button()

    filter_state = render_oos_monitor_control_panel(
        available_products,
        available_factories,
        action_renderer=_render_query_button_in_row,
    )

    def _clear_oos_monitor_cache() -> None:
        clear_oos_source_cache(selected_scopes(filter_state.data_type_filter))
        get_cached_oos_monitor_payload.clear()

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
            history_service = build_oos_history_service()
            ooc_history_service = build_ooc_history_service()
            summary_workbook_service = build_monitor_summary_workbook_service()
            source_signature = "|".join(
                [
                    history_service.source_signature(list(products), list(scopes)),
                    ooc_history_service.source_signature(list(products), list(scopes)),
                ]
            )
            summary_workbook_signature = summary_workbook_service.source_signature()
            with st.spinner("正在读取 Excel 超规明细与汇总表..."):
                payload = get_cached_oos_monitor_payload(
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
            render_oos_monitor_results(view_model, step_desc_map=step_desc_map)
            if is_admin:
                st.divider()
                render_oos_refresh_status(view_model.refresh_status_df)
                st.caption("请在各模块超规明细 Excel 的产品 sheet 中维护 Flag，随后点击刷新缓存。看板不再合并旧决策台账。")

st.subheader("📊 CPK预警看板")
with st.expander("SPC CPK超规预警", expanded=True):
    render_cpk_monitor_section(
        build_cpk_monitor_service(),
        SessionManager.AVAILABLE_PRODUCTS,
        MONITOR_FACTORY_OPTIONS,
    )
