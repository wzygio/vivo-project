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
from app.sections.inline_domain.ctq.ctq_dashboard import (
    build_ctq_sheet_oos_alerts,
    filter_ctq_report,
    get_default_ctq_start_date,
    render_ctq_decoration_admin,
    render_ctq_filters,
    render_ctq_indicator_sections,
    render_ctq_sheet_oos_alert_indicator_sections,
)
from app.sections.inline_domain.shared.alert_center import render_sheet_oos_alert_center
from app.utils.app_setup import AppSetup
from app.utils.step_labels import get_cached_step_description_map
from app.manager.session_manager import SessionManager
from src.inline_domain.application.ctq.ctq_service import CtqReportService
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.shared.decorated_features import fetch_decorated_features
from src.inline_domain.composition import build_ctq_repository, refresh_raw_measurements
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range
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
step_desc_map = get_cached_step_description_map(db_manager)

_, default_end_dt = MonitorAnalysisService.get_time_window()
default_start_dt = get_default_ctq_start_date(default_end_dt.date())

query_config = SpcQueryConfig(
    prod_code=current_product,
    start_date=default_start_dt.strftime("%Y-%m-%d"),
    end_date=default_end_dt.strftime("%Y-%m-%d"),
    data_type_filter="CTQ",
)
ctq_data_port = build_ctq_repository(db_manager, current_product)

render_page_header(
    title="CTQ监控报表",
    config=active_config,
    cached_funcs=extract_cached_funcs(CtqReportService) + [fetch_decorated_features, get_cached_step_description_map],
    product_cache_scope=current_product,
    refresh_handlers=[
        lambda: refresh_raw_measurements(
            db_manager,
            current_product,
            query_config.end_date,
        )
    ],
)

with st.spinner("正在加载 CTQ 分布数据..."):
    view_model = CtqReportService.get_ctq_report_data(
        _data_port=ctq_data_port,
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
    indicator_df=indicator_df,
    step_desc_map=step_desc_map,
)

ctq_oos_alerts_df = build_ctq_sheet_oos_alerts(
    view_model.sheet_oos_decoration_result,
    reference_date=default_end_dt.date(),
)
oos_week_start, _ = previous_iso_week_range(default_end_dt.date())
oos_iso_week = oos_week_start.isocalendar()
render_sheet_oos_alert_center(
    ctq_oos_alerts_df,
    title=f"单片异常预警中心（上一周 {oos_iso_week.year}-W{oos_iso_week.week:02d}）",
    has_source_data=view_model.sheet_oos_decoration_result is not None,
    step_desc_map=step_desc_map,
)
render_ctq_sheet_oos_alert_indicator_sections(
    alerts_df=ctq_oos_alerts_df,
    sheet_features_df=sheet_features_df,
    raw_measurements_df=raw_measurements_df,
    period_box_source=ConfigLoader.get_spc_period_box_source(),
    step_desc_map=step_desc_map,
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
    step_desc_map=step_desc_map,
)
