import streamlit as st
import pandas as pd

# ==============================================================================
#  配置与初始化
# ==============================================================================
from app.utils.session_manager import SessionManager
from app.utils.app_setup import AppSetup
from app.components.page_header import (
    extract_cached_funcs,
    render_page_header,
)
from app.sections.monitor.monitor_dashboard import (
    get_cached_alarm_detail_tables,
    render_alarm_detail_tables,
    render_monitor_control_panel,
    render_monitor_summary_chart,
    render_station_top10_section,
    filter_and_rollup_monitor_data,
)
# [新增] 导入数据修饰配置模块（文件配置版）
from app.compliance.compliance_manager import (
    render_compliance_config_panel,
)

# --- 2. 引入真实的 SPC 后端 Service 与数据模型 ---
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.infrastructure.spc.data_loader import SpcQueryConfig
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
    get_cached_query_window,
    get_cached_alarm_detail_tables,
]
render_page_header(
    title="自动预警看板",
    config=active_config,
    cached_funcs=funcs_to_clear,
    refresh_handlers=[
        lambda: MonitorAnalysisService.safe_refresh_snapshots(
            db_manager,
            header_query_config.model_dump_json(),
        )
    ],
)

# --------------------------------------------------------------------------
# 页面积木组装层 (UI Assembly)
# --------------------------------------------------------------------------
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

# 只加载一次 ALL 数据；其它监控类型从同一份 st.cache_data 结果中切片秒切。
with st.spinner("正在加载 ALL 监控数据..."):
    query_config_all = SpcQueryConfig(
        prod_code="ALL",
        start_date=start_date_str,
        end_date=end_date_str,
        data_type_filter="ALL",
    )
    view_model = MonitorAnalysisService.get_monitor_dashboard_data(
        _db_manager=db_manager,
        query_config_json=query_config_all.model_dump_json(),
        time_type='MIXED',
        force_compliant=True,
        data_type_filter="ALL",
        snapshot_signature=MONITOR_PAGE_CACHE_SIGNATURE,
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
)

if is_admin:
    st.divider()
    render_alarm_detail_tables(
        db_manager=db_manager,
        query_config_json=query_config_all.model_dump_json(),
        filter_state=filter_state,
        snapshot_signature=MONITOR_PAGE_CACHE_SIGNATURE,
        is_admin=is_admin,
    )
