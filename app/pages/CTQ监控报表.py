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

from app.components.page_header import (
    build_product_cache_signature,
    extract_cached_funcs,
    render_page_header,
)
from app.sections.ctq.ctq_dashboard import (
    filter_ctq_report,
    get_default_ctq_start_date,
    render_ctq_decoration_admin,
    render_ctq_filters,
    render_ctq_indicator_sections,
)
from app.utils.app_setup import AppSetup
from app.utils.session_manager import SessionManager
from src.inline_domain.application.ctq.ctq_service import CtqReportService
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.infrastructure.spc.data_loader import SpcQueryConfig
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

CTQ_PAGE_CACHE_SIGNATURE = "ctq_distribution_report_v1"


st.set_page_config(page_title="CTQ监控报表", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

active_config = SessionManager.get_active_config()
current_product = active_config.data_source.product_code
product_cache_signature = build_product_cache_signature(
    CTQ_PAGE_CACHE_SIGNATURE,
    current_product,
)
db_manager = DatabaseManager()

_, default_end_dt = MonitorAnalysisService.get_time_window()
default_start_dt = get_default_ctq_start_date(default_end_dt.date())
query_config = SpcQueryConfig(
    prod_code=current_product,
    start_date=default_start_dt.strftime("%Y-%m-%d"),
    end_date=default_end_dt.strftime("%Y-%m-%d"),
    data_type_filter="CTQ",
)

render_page_header(
    title="CTQ监控报表",
    config=active_config,
    cached_funcs=extract_cached_funcs(CtqReportService),
    product_cache_scope=current_product,
    refresh_handlers=[
        lambda: MonitorAnalysisService.safe_refresh_snapshots(
            db_manager,
            query_config.model_dump_json(),
        )
    ],
)

with st.spinner("正在加载 CTQ 分布数据..."):
    view_model = CtqReportService.get_ctq_report_data(
        _db_manager=db_manager,
        query_config_json=query_config.model_dump_json(),
        snapshot_signature=product_cache_signature,
    )

sheet_features_df = view_model.sheet_features_df
raw_measurements_df = view_model.raw_measurements_df
indicator_df = view_model.indicators_df

if sheet_features_df.empty or indicator_df.empty:
    st.info("当前产品暂无可展示的 CTQ 数据。")
    st.stop()

query_params = st.query_params
is_admin = query_params.get("admin") == "true" or "admin-true" in query_params
if is_admin:
    render_ctq_decoration_admin(
        getattr(view_model, "sheet_oos_decoration_result", None),
    )

selected_factory, selected_params, selected_steps, should_render_report = render_ctq_filters(
    indicator_df=indicator_df
)
if not should_render_report:
    st.info("当前筛选条件尚未查询。")
    st.stop()

filtered_sheet_features_df = filter_ctq_report(
    report_df=sheet_features_df,
    selected_factory=selected_factory,
    selected_params=selected_params,
    selected_steps=selected_steps,
)
filtered_raw_measurements_df = filter_ctq_report(
    report_df=raw_measurements_df,
    selected_factory=selected_factory,
    selected_params=selected_params,
    selected_steps=selected_steps,
)

render_ctq_indicator_sections(
    sheet_features_df=filtered_sheet_features_df,
    raw_measurements_df=filtered_raw_measurements_df,
    period_box_source=ConfigLoader.get_spc_period_box_source(),
)
