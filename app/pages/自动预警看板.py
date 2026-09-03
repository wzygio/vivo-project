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
from functools import partial

# ==============================================================================
#  配置与初始化
# ==============================================================================
from app.manager.session_manager import SessionManager
from app.utils.app_setup import AppSetup
from app.utils.step_labels import get_cached_step_description_map
from app.components.page_header import (
    extract_cached_funcs,
    get_product_cache_revision,
    render_page_header,
)
from app.sections.inline_domain.monitor.monitor_dashboard import (
    get_cached_alarm_detail_tables,
    render_alarm_detail_tables,
    render_monitor_control_panel,
    render_monitor_query_gate,
    render_monitor_summary_chart,
    render_station_top10_section,
    filter_and_rollup_monitor_data,
)
from app.sections.inline_domain.monitor.alert_matrix import (
    MATRIX_SELECTION_STATE_KEY,
    render_alert_matrix_board,
)
from app.sections.inline_domain.monitor.alert_matrix_cache import (
    get_alert_matrix_cached_funcs,
)
# [新增] 导入数据修饰配置模块（文件配置版）
from app.manager.compliance_manager import (
    get_compliance_file_signature,
    render_compliance_config_panel,
)

# --- 2. 引入真实的 SPC 后端 Service 与数据模型 ---
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.shared.decorated_features import fetch_decorated_features
from src.inline_domain.application.shared.decision_signature import get_scope_decision_signature
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.composition import build_monitor_repository
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    SheetOosDecorationReadError,
)
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

MONITOR_PAGE_CACHE_SIGNATURE = "auto_warning_dashboard_manual_clear_v1"
MONITOR_FACTORY_OPTIONS = ["ARRAY", "OLED", "TP"]


@st.cache_data(show_spinner=False)
def get_cached_query_window() -> tuple[str, str]:
    """Keep this page's time window stable until the user clears cache."""
    start_dt, end_dt = MonitorAnalysisService.get_time_window()
    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")


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
    header_data_type_filter = st.session_state.get("spc_data_type_filter", "ALL")
    header_query_config = SpcQueryConfig(
        prod_code="ALL",
        start_date=start_date_str,
        end_date=end_date_str,
        data_type_filter=header_data_type_filter,
    )
except Exception as e:
    import logging, traceback
    logging.error(f"❌ 初始化 SPC 看板失败: {e}", exc_info=True)
    st.error(f"❌ 初始化 SPC 看板失败: {str(e)}")
    with st.expander("查看详细错误堆栈"):
        st.code(traceback.format_exc())
    st.stop()

funcs_to_clear = extract_cached_funcs(MonitorAnalysisService) + [
    fetch_decorated_features,
    get_cached_query_window,
    get_cached_alarm_detail_tables,
    get_cached_step_description_map,
] + get_alert_matrix_cached_funcs()
render_page_header(
    title="自动预警看板",
    config=active_config,
    cached_funcs=funcs_to_clear,
    refresh_handlers=[
        lambda: MonitorAnalysisService.safe_refresh_snapshots(
            partial(build_monitor_repository, db_manager),
            header_query_config.model_dump_json(),
        )
    ],
    # 本页为全产品视图（矩阵与超规片预警均跨产品），页头产品框无意义
    show_product_filter=False,
)

# --------------------------------------------------------------------------
# 模块一：预警矩阵（产品 × 监控参数，按钮门控加载）
# 矩阵为全产品视图，不参与 Header 单产品筛选（D3）；默认只渲染
# 「加载预警矩阵」按钮（无 info 文案，门控语义由按钮承担，UI 优化轮次），
# 点击后才读取 L2 缓存 payload 并渲染（真正的全量计算仍由既有 monitor
# 管线在缓存 miss 时完成，按钮本身只是读取缓存）。已加载状态存
# session_state，普通 rerun 保持可见；「刷新缓存/刷新数据」由
# perform_hard_reset 阶段 4 / _refresh_data_callback 清除该状态。
# --------------------------------------------------------------------------
ALERT_MATRIX_LOADED_STATE_KEY = "alert_matrix_board_loaded"


def _load_alert_matrix() -> None:
    st.session_state[ALERT_MATRIX_LOADED_STATE_KEY] = True


def _collapse_alert_matrix() -> None:
    st.session_state.pop(ALERT_MATRIX_LOADED_STATE_KEY, None)
    st.session_state.pop(MATRIX_SELECTION_STATE_KEY, None)


st.subheader("🚦 预警矩阵")
with st.expander("产品 × 监控参数 · 上一周期预警状态", expanded=True):
    if st.session_state.get(ALERT_MATRIX_LOADED_STATE_KEY):
        render_alert_matrix_board(db_manager=db_manager, step_desc_map=step_desc_map)
        st.button(
            "收起预警矩阵",
            key="btn_collapse_alert_matrix",
            on_click=_collapse_alert_matrix,
            help="收起矩阵区；再次点击「加载预警矩阵」重新读取缓存展示。",
        )
    else:
        st.button(
            "🚦 加载预警矩阵",
            type="primary",
            key="btn_load_alert_matrix",
            on_click=_load_alert_matrix,
        )

# --------------------------------------------------------------------------
# 模块二：超规片自动预警（「查询」门控，2026-09-03 需求）
# 页面打开不自动全量加载：未点击「查询」时 Expander 内只有控制台与查询
# 按钮（无 info 文案，门控语义由按钮承担）；点击后普通 rerun 保持已提交
# 状态；筛选 signature 变化静默回到未提交态（仿 Q-Time 页签名过期模式）；
# 「刷新缓存/刷新数据」由 perform_hard_reset 阶段 4 /
# _refresh_data_callback 清除该 session key。
# --------------------------------------------------------------------------
st.subheader("⚠️ 超规片自动预警")
with st.expander("筛选控制台与预警结果", expanded=True):
    available_products = SessionManager.AVAILABLE_PRODUCTS
    available_factories = MONITOR_FACTORY_OPTIONS

    # 4. 组装积木: 渲染控制台
    filter_state = render_monitor_control_panel(available_products, available_factories)

    # [新增] 渲染数据修饰配置面板（仅管理员可见）
    if is_admin:
        render_compliance_config_panel(
            data_type=filter_state.data_type_filter,
            selected_products=filter_state.selected_products or ["ALL"],
            selected_factories=filter_state.selected_factories or ["ALL"]
        )

    if render_monitor_query_gate(filter_state):
        # Phase 4 门控：monitor 涉及多产品/多修饰口径（spc/ctq），按产品逐个预算
        # 共享 revision 与决策签名；服务内部按产品循环取用，进入 L2 缓存键与 core 门控。
        # 决策表读取失败时显式失败（不降级为空决策）。
        product_revisions = {
            prod: get_product_cache_revision(prod) for prod in available_products
        }
        data_forward_signature = ConfigLoader.get_data_forward_policy().signature
        try:
            decision_signatures = {
                prod: {
                    scope: get_scope_decision_signature(scope, prod)
                    for scope in ("spc", "ctq")
                }
                for prod in available_products
            }
        except SheetOosDecorationReadError as exc:
            st.error(
                f"超规片修饰决策表读取失败：{exc}。"
                "请确认 Excel 文件可正常打开且未被锁定，然后点击页头“刷新缓存”重试。"
            )
            st.stop()

        # 只加载一次 ALL 数据；其它监控类型从同一份 st.cache_data 结果中切片秒切。
        with st.spinner("正在加载 ALL 监控数据..."):
            query_config_all = SpcQueryConfig(
                prod_code="ALL",
                start_date=start_date_str,
                end_date=end_date_str,
                data_type_filter="ALL",
            )
            view_model = MonitorAnalysisService.get_monitor_dashboard_data(
                _repository_factory=partial(build_monitor_repository, db_manager),
                query_config_json=query_config_all.model_dump_json(),
                time_type='MIXED',
                force_compliant=True,
                data_type_filter="ALL",
                snapshot_signature=(
                    f"{MONITOR_PAGE_CACHE_SIGNATURE}:{get_compliance_file_signature()}:"
                    f"{data_forward_signature}"
                ),
                product_revisions=product_revisions,
                decision_signatures=decision_signatures,
            )

        # 更新数据引用
        detail_df = getattr(view_model, "detail_df", pd.DataFrame())
        global_summary_df = getattr(view_model, "global_summary_df", pd.DataFrame())
        station_detail_df = getattr(view_model, "station_detail_df", pd.DataFrame())

        filtered_summary_df, filtered_detail_df, filtered_station_df = filter_and_rollup_monitor_data(
            detail_df, global_summary_df, station_detail_df, filter_state
        )

        # 5. 组装积木: 仅保留图表，移除旧的汇总/透视明细表
        render_monitor_summary_chart(filtered_summary_df, filter_state.data_type_filter)

        st.divider()

        # 6. 组装积木: 仅保留 Top 10 站点图，移除旧的站点汇总/明细表
        render_station_top10_section(
            filtered_station_df,
            filter_state.data_type_filter,
            is_admin,
            show_tables=False,
            step_desc_map=step_desc_map,
        )

        if is_admin:
            st.divider()
            render_alarm_detail_tables(
                db_manager=db_manager,
                query_config_json=query_config_all.model_dump_json(),
                filter_state=filter_state,
                snapshot_signature=(
                    f"{MONITOR_PAGE_CACHE_SIGNATURE}:{get_compliance_file_signature()}:"
                    f"{data_forward_signature}"
                ),
                is_admin=is_admin,
                product_revisions=product_revisions,
                decision_signatures=decision_signatures,
            )
