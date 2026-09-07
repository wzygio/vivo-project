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

# ==============================================================================
#  配置与初始化
# ==============================================================================
from app.manager.session_manager import SessionManager
from app.utils.app_setup import AppSetup
from app.utils.step_labels import get_cached_step_description_map
from app.components.page_header import (
    render_page_header,
)
from app.sections.inline_domain.monitor.oos_monitor_dashboard import (
    render_oos_monitor_control_panel,
    render_oos_monitor_results,
    render_oos_refresh_status,
    render_monitor_query_button,
    render_monitor_query_gate,
    selected_scopes,
)
from app.sections.inline_domain.monitor.alert_matrix import (
    MATRIX_SELECTION_STATE_KEY,
    render_alert_matrix_board,
    render_alert_matrix_filter_bar,
)
from app.sections.inline_domain.monitor.alert_matrix_cache import (
    get_alert_matrix_cached_funcs,
)
from src.inline_domain.application.monitor.oos_monitor_service import (
    OosMonitorService,
    OosMonitorViewModel,
)
from src.inline_domain.application.shared.oos_history_service import OosHistoryService
from src.inline_domain.composition import build_oos_history_service
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

MONITOR_FACTORY_OPTIONS = ["ARRAY", "OLED", "TP"]


@st.cache_data(show_spinner=False, ttl=ConfigLoader.get_cache_ttl_seconds())
def get_cached_query_window() -> tuple[str, str]:
    """Keep this page's time window stable until the user clears cache."""
    end_dt = pd.Timestamp.today().normalize()
    start_dt = (end_dt - pd.DateOffset(months=2)).replace(day=1)
    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")


@st.cache_data(show_spinner=False, ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=16)
def get_cached_oos_monitor_payload(
    products: tuple[str, ...],
    scopes: tuple[str, ...],
    factories: tuple[str, ...],
    start_date: str,
    end_date: str,
    source_signature: str,
) -> dict[str, pd.DataFrame]:
    del source_signature
    view = OosMonitorService(build_oos_history_service()).build_dashboard(
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
    }


st.set_page_config(page_title="自动预警看板", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

# [权限控制] 检测 URL 参数，仅用于控制修饰器面板显示
query_params = st.query_params
is_admin = query_params.get("admin") == "true"

# ==============================================================================
#  数据加载
# ==============================================================================
try:
    db_manager = DatabaseManager()
    step_desc_map = get_cached_step_description_map(db_manager)
    start_date_str, end_date_str = get_cached_query_window()
    active_config = SessionManager.get_active_config()
except Exception:
    logging.exception("自动预警看板初始化失败")
    st.error("页面初始化失败，请稍后重试或联系管理员。")
    st.stop()

funcs_to_clear = [
    get_cached_query_window,
    get_cached_oos_monitor_payload,
    get_cached_step_description_map,
] + get_alert_matrix_cached_funcs()
render_page_header(
    title="自动预警看板",
    config=active_config,
    cached_funcs=funcs_to_clear,
    refresh_handlers=[],
    # 本页为全产品视图（矩阵与超规片预警均跨产品），页头产品框无意义
    show_product_filter=False,
)

# --------------------------------------------------------------------------
# 模块一：预警矩阵（产品 × 监控参数，按钮门控加载）
# 矩阵为全产品视图，不参与 Header 单产品筛选（D3）；筛选条（监控类型/产品
# 型号/厂别）在 Expander 内常驻渲染，未加载时也可先选条件；默认只渲染
# 「加载预警矩阵」按钮（无 info 文案，门控语义由按钮承担，UI 优化轮次），
# 点击后才读取 L2 缓存 payload 并按当前筛选选择客户端切片展示（真正的
# 全量计算仍由既有 monitor 管线在缓存 miss 时完成，按钮本身只是读取缓存）。
# 已加载状态存 session_state，普通 rerun 保持可见；「刷新缓存/刷新数据」由
# perform_hard_reset 阶段 4 / _refresh_data_callback 清除该状态。
# --------------------------------------------------------------------------
ALERT_MATRIX_LOADED_STATE_KEY = "alert_matrix_board_loaded"


def _load_alert_matrix() -> None:
    st.session_state[ALERT_MATRIX_LOADED_STATE_KEY] = True


def _collapse_alert_matrix() -> None:
    st.session_state.pop(ALERT_MATRIX_LOADED_STATE_KEY, None)
    st.session_state.pop(MATRIX_SELECTION_STATE_KEY, None)


def _render_matrix_action_button() -> None:
    """矩阵操作按钮（加载/收起），渲染在筛选条同行的最右列。"""
    if st.session_state.get(ALERT_MATRIX_LOADED_STATE_KEY):
        st.button(
            "收起预警矩阵",
            key="btn_collapse_alert_matrix",
            on_click=_collapse_alert_matrix,
            help="收起矩阵区；再次点击「加载预警矩阵」重新读取缓存展示。",
        )
    else:
        st.button(
            "查询",
            type="primary",
            key="btn_load_alert_matrix",
            on_click=_load_alert_matrix,
        )


st.subheader("🚦 Q-Time预警看板")
with st.expander("Q-Time超规预警", expanded=True):
    # 筛选条常驻（与下方「超规片自动预警」控制台同观感）：未加载时也可先选
    # 条件，点击加载后按当前选择客户端切片；widget key 只在此渲染一处，
    # 已加载分支经 filter_selection 透传给矩阵，不重复渲染。
    # 操作按钮（加载/收起）与筛选三件套同处一行最右列（参照 Q-Time 页布局）。
    matrix_filter_selection = render_alert_matrix_filter_bar(
        SessionManager.AVAILABLE_PRODUCTS,
        action_renderer=_render_matrix_action_button,
    )
    if st.session_state.get(ALERT_MATRIX_LOADED_STATE_KEY):
        render_alert_matrix_board(
            db_manager=db_manager,
            step_desc_map=step_desc_map,
            filter_selection=matrix_filter_selection,
        )

# --------------------------------------------------------------------------
# 模块二：共享超规事实看板（「查询」门控）
# 页面打开不读取历史：未点击「查询」时 Expander 内只有控制台与查询
# 按钮（无 info 文案，门控语义由按钮承担）；点击后普通 rerun 保持已提交
# 状态；筛选 signature 变化静默回到未提交态；
# 「刷新缓存/刷新数据」由 perform_hard_reset 阶段 4 /
# _refresh_data_callback 清除该 session key。
# --------------------------------------------------------------------------
st.subheader("⚠️ Inline预警看板")
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

    if render_monitor_query_gate(
        filter_state, clicked=query_click_box.get("clicked", False)
    ):
        products = tuple(filter_state.selected_products)
        scopes = selected_scopes(filter_state.data_type_filter)
        factories = tuple(filter_state.selected_factories)
        try:
            history_service = build_oos_history_service()
            source_signature = history_service.source_signature(list(products), list(scopes))
            with st.spinner("正在读取共享超规历史..."):
                payload = get_cached_oos_monitor_payload(
                    products,
                    scopes,
                    factories,
                    start_date_str,
                    end_date_str,
                    source_signature,
                )
        except Exception:
            logging.exception("共享超规历史读取失败")
            st.error("超规历史读取失败，请确认数据文件可用且未被占用后重试。")
            st.stop()
        view_model = OosMonitorViewModel(**payload)
        render_oos_monitor_results(view_model, step_desc_map=step_desc_map)
        if is_admin:
            st.divider()
            render_oos_refresh_status(view_model.refresh_status_df)
