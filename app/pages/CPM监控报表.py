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

from app.components.page_header import extract_cached_funcs, render_page_header
from app.sections.spc_cpm_dashboard import (
    build_daily_cpk_alerts,
    filter_cpm_report,
    get_default_cpm_start_date,
    render_cpk_alert_center,
    render_cpm_filters,
    render_cpm_indicator_sections,
    render_sheet_oos_decoration_admin,
)
from app.utils.app_setup import AppSetup
from app.utils.session_manager import SessionManager
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.spc_domain.application.cpm_service import CpmReportService
from src.spc_domain.application.spc_service import SpcAnalysisService
from src.spc_domain.infrastructure.data_loader import SpcQueryConfig

CPM_PAGE_CACHE_SIGNATURE = "spc_cpm_cpk_distribution_report_v1"


st.set_page_config(page_title="CPM/CPK监控报表", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

active_config = SessionManager.get_active_config()
current_product = active_config.data_source.product_code
db_manager = DatabaseManager()

_, default_end_dt = SpcAnalysisService.get_time_window()
default_start_dt = get_default_cpm_start_date(default_end_dt.date())

query_config = SpcQueryConfig(
    prod_code=current_product,
    start_date=default_start_dt.strftime("%Y-%m-%d"),
    end_date=default_end_dt.strftime("%Y-%m-%d"),
    data_type_filter="SPC",
)

render_page_header(
    title="CPM/CPK监控报表",
    config=active_config,
    cached_funcs=extract_cached_funcs(CpmReportService),
    refresh_handlers=[
        lambda: SpcAnalysisService.safe_refresh_snapshots(
            db_manager,
            query_config.model_dump_json(),
        )
    ],
)

with st.spinner("正在加载 SPC CPM/CPK 分布数据..."):
    view_model = CpmReportService.get_cpm_report_data(
        _db_manager=db_manager,
        query_config_json=query_config.model_dump_json(),
        snapshot_signature=CPM_PAGE_CACHE_SIGNATURE,
        period_sigma_source=ConfigLoader.get_cpm_period_sigma_source(),
    )

sheet_features_df = view_model.sheet_features_df
raw_measurements_df = view_model.raw_measurements_df
indicator_df = view_model.indicators_df
period_capability_df = view_model.period_capability_df

cpk_alerts_df = build_daily_cpk_alerts(period_capability_df)
# render_cpk_alert_center(
#     cpk_alerts_df,
#     has_capability_data=not period_capability_df.empty,
# )

if sheet_features_df.empty or indicator_df.empty:
    st.info("当前产品暂无可展示 CPM/CPK 分布的 SPC 数据。")
    st.stop()

query_params = st.query_params
is_admin = query_params.get("admin") == "true" or "admin-true" in query_params
if is_admin and view_model.sheet_oos_decoration_result is not None:
    decoration_result = view_model.sheet_oos_decoration_result
    render_sheet_oos_decoration_admin(decoration_result)

selected_factory, selected_params, selected_steps, should_render_report = render_cpm_filters(
    indicator_df=indicator_df
)

if not should_render_report:
    st.info("当前筛选条件尚未查询。")
    st.stop()

filtered_period_capability_df = filter_cpm_report(
    report_df=period_capability_df,
    selected_factory=selected_factory,
    selected_params=selected_params,
    selected_steps=selected_steps,
)
filtered_sheet_features_df = filter_cpm_report(
    report_df=sheet_features_df,
    selected_factory=selected_factory,
    selected_params=selected_params,
    selected_steps=selected_steps,
)
filtered_raw_measurements_df = filter_cpm_report(
    report_df=raw_measurements_df,
    selected_factory=selected_factory,
    selected_params=selected_params,
    selected_steps=selected_steps,
)

render_cpm_indicator_sections(
    period_capability_df=filtered_period_capability_df,
    sheet_features_df=filtered_sheet_features_df,
    raw_measurements_df=filtered_raw_measurements_df,
    period_box_source=ConfigLoader.get_cpm_period_box_source(),
)
