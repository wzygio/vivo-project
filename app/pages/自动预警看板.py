import sys
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
from app.utils.step_labels import get_cached_step_description_map
from app.sections.inline_domain.monitor.alert_matrix import (
    MATRIX_SELECTION_STATE_KEY,
    render_alert_matrix_board,
    render_alert_matrix_filter_bar,
    render_matrix_refresh_controls,
)
from app.sections.inline_domain.monitor.alert_matrix_snapshot import has_daily_matrix_snapshot
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

st.set_page_config(page_title="自动预警看板", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

# --------------------------------------------------------------------------
# 模块一：预警矩阵（产品 × 监控参数，按钮门控加载）
# 矩阵为全产品视图，不参与 Header 单产品筛选（D3）；筛选条（监控类型/产品
# 型号/厂别）在 Expander 内常驻渲染，未加载时也可先选条件；默认只渲染
# 「加载预警矩阵」按钮（无 info 文案，门控语义由按钮承担，UI 优化轮次），
# 点击后才读取 L2 缓存 payload 并按当前筛选选择客户端切片展示（真正的
# 全量计算仍由既有 monitor 管线在缓存 miss 时完成，按钮本身只是读取缓存）。
# 已加载状态存 session_state，普通 rerun 保持可见；刷新只影响本看板。
# --------------------------------------------------------------------------
ALERT_MATRIX_LOADED_STATE_KEY = "alert_matrix_board_loaded"
ALERT_MATRIX_AUTO_OPENED_KEY = "alert_matrix_auto_opened_date"

today_label = pd.Timestamp.today().date().isoformat()
if (
    not st.session_state.get(ALERT_MATRIX_LOADED_STATE_KEY)
    and st.session_state.get(ALERT_MATRIX_AUTO_OPENED_KEY) != today_label
    and has_daily_matrix_snapshot()
):
    st.session_state[ALERT_MATRIX_LOADED_STATE_KEY] = True
    st.session_state[ALERT_MATRIX_AUTO_OPENED_KEY] = today_label


def _load_alert_matrix() -> None:
    st.session_state[ALERT_MATRIX_LOADED_STATE_KEY] = True


def _collapse_alert_matrix() -> None:
    st.session_state.pop(ALERT_MATRIX_LOADED_STATE_KEY, None)
    st.session_state.pop(MATRIX_SELECTION_STATE_KEY, None)
    st.session_state[ALERT_MATRIX_AUTO_OPENED_KEY] = today_label


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


st.subheader("🚦 全指标预警看板")
with st.expander("全指标状态总览", expanded=True):
    # 筛选条常驻（与下方「超规片自动预警」控制台同观感）：未加载时也可先选
    # 条件，点击加载后按当前选择客户端切片；widget key 只在此渲染一处，
    # 已加载分支经 filter_selection 透传给矩阵，不重复渲染。
    # 操作按钮（加载/收起）与筛选三件套同处一行最右列（参照 Q-Time 页布局）。
    matrix_filter_selection = render_alert_matrix_filter_bar(
        SessionManager.AVAILABLE_PRODUCTS,
        action_renderer=_render_matrix_action_button,
    )
    # 管理入口不依赖本会话是否查询过；切换 admin URL 后可直接定向失效。
    # 查询仍使用跨会话共享的单元格缓存，admin 只控制操作面板的显示。
    render_matrix_refresh_controls()
    if st.session_state.get(ALERT_MATRIX_LOADED_STATE_KEY):
        db_manager = DatabaseManager()
        render_alert_matrix_board(
            db_manager=db_manager,
            step_desc_map=get_cached_step_description_map(db_manager),
            filter_selection=matrix_filter_selection,
            show_refresh_controls=False,
        )
