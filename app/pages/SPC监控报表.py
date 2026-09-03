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
    get_product_cache_revision,
    render_page_header,
)
from app.sections.inline_domain.spc.spc_dashboard import (
    build_spc_sheet_oos_alerts,
    build_weekly_cpk_alerts,
    build_weekly_cpm_alerts,
    filter_spc_report,
    get_default_spc_start_date,
    render_cpk_alert_section,
    render_cpm_alert_section,
    render_sheet_oos_alert_indicator_sections,
    render_spc_filters,
    render_spc_indicator_sections,
    render_spc_decoration_admin,
)
from app.sections.inline_domain.shared.alert_center import render_sheet_oos_alert_center
from app.utils.app_setup import AppSetup
from app.utils.step_labels import get_cached_step_description_map
from app.manager.session_manager import SessionManager
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.inline_domain.application.spc import spc_service
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.shared.decorated_features import fetch_decorated_features
from src.inline_domain.application.shared.decision_signature import get_scope_decision_signature
from src.inline_domain.composition import build_spc_repository, refresh_raw_measurements
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    SheetOosDecorationReadError,
)

SPC_PAGE_CACHE_SIGNATURE = "spc_capability_distribution_report_v1"
SpcReportService = spc_service.SpcReportService
_spc_decoration_file_error = getattr(spc_service, "SpcDecorationFileError", None)
SPC_DECORATION_FILE_ERRORS = (
    (_spc_decoration_file_error,)
    if isinstance(_spc_decoration_file_error, type)
    and issubclass(_spc_decoration_file_error, BaseException)
    else ()
)

st.set_page_config(page_title="SPC监控报表", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

active_config = SessionManager.get_active_config()
current_product = active_config.data_source.product_code
product_cache_signature = build_product_cache_signature(
    SPC_PAGE_CACHE_SIGNATURE,
    current_product,
)
db_manager = DatabaseManager()
step_desc_map = get_cached_step_description_map(db_manager)

_, default_end_dt = MonitorAnalysisService.get_time_window()
default_start_dt = get_default_spc_start_date(default_end_dt.date())

query_config = SpcQueryConfig(
    prod_code=current_product,
    start_date=default_start_dt.strftime("%Y-%m-%d"),
    end_date=default_end_dt.strftime("%Y-%m-%d"),
    data_type_filter="SPC",
)
spc_data_port = build_spc_repository(db_manager, current_product)

render_page_header(
    title="SPC监控报表",
    config=active_config,
    cached_funcs=extract_cached_funcs(SpcReportService) + [fetch_decorated_features, get_cached_step_description_map],
    product_cache_scope=current_product,
    refresh_handlers=[
        lambda: refresh_raw_measurements(
            db_manager,
            current_product,
            query_config.end_date,
        )
    ],
)

try:
    # Phase 4 门控：共享产品 revision + 两阶段决策签名进入 L2 缓存键；
    # 决策表读取失败时显式失败（不降级为空决策），由现有错误路径提示。
    product_revision = get_product_cache_revision(current_product)
    decision_signature = get_scope_decision_signature("spc", current_product)
    with st.spinner("正在加载 SPC 分布数据..."):
        view_model = SpcReportService.get_spc_report_data(
            _data_port=spc_data_port,
            query_config_json=query_config.model_dump_json(),
            snapshot_signature=product_cache_signature,
            period_sigma_source=ConfigLoader.get_spc_period_sigma_source(),
            product_revision=product_revision,
            decision_signature=decision_signature,
        )
except SPC_DECORATION_FILE_ERRORS + (SheetOosDecorationReadError,):
    st.error(
        "SPC 超规片修饰表读取失败。请确认 Excel 文件可正常打开且未被锁定，"
        "然后点击页头“刷新缓存”重试。"
    )
    st.stop()

sheet_features_df = view_model.sheet_features_df
raw_measurements_df = view_model.raw_measurements_df
indicator_df = view_model.indicators_df
period_capability_df = view_model.period_capability_df

if sheet_features_df.empty or indicator_df.empty:
    st.info("当前产品暂无可展示的 SPC 数据。")
    st.stop()

selected_factory, selected_params, selected_steps, should_render_report = render_spc_filters(
    indicator_df=indicator_df,
    step_desc_map=step_desc_map,
)

cpk_alerts_df = build_weekly_cpk_alerts(
    period_capability_df,
    reference_date=default_end_dt.date(),
)
cpm_alerts_df = build_weekly_cpm_alerts(
    period_capability_df,
    reference_date=default_end_dt.date(),
)

query_params = st.query_params
is_admin = query_params.get("admin") == "true" or "admin-true" in query_params
if is_admin:
    render_spc_decoration_admin(
        getattr(view_model, "sheet_oos_decoration_result", None),
        getattr(view_model, "cpk_decoration_result", None),
        getattr(view_model, "cpm_decoration_result", None),
    )
    
render_cpk_alert_section(
    cpk_alerts_df,
    has_capability_data=not period_capability_df.empty,
    period_capability_df=period_capability_df,
    sheet_features_df=sheet_features_df,
    raw_measurements_df=raw_measurements_df,
    period_box_source=ConfigLoader.get_spc_period_box_source(),
    step_desc_map=step_desc_map,
)

render_cpm_alert_section(
    cpm_alerts_df,
    has_capability_data=not period_capability_df.empty,
    period_capability_df=period_capability_df,
    sheet_features_df=sheet_features_df,
    raw_measurements_df=raw_measurements_df,
    period_box_source=ConfigLoader.get_spc_period_box_source(),
    step_desc_map=step_desc_map,
)

sheet_oos_decoration_result = getattr(view_model, "sheet_oos_decoration_result", None)
spc_oos_alerts_df = build_spc_sheet_oos_alerts(
    sheet_oos_decoration_result,
    reference_date=default_end_dt.date(),
)
previous_week_start, _ = previous_iso_week_range(default_end_dt.date())
previous_week_iso = previous_week_start.isocalendar()
previous_week_label = f"{previous_week_iso.year}-W{previous_week_iso.week:02d}"

render_sheet_oos_alert_center(
    spc_oos_alerts_df,
    title=f"单片异常预警中心（上一周 {previous_week_label}）",
    has_source_data=sheet_oos_decoration_result is not None,
    step_desc_map=step_desc_map,
)

render_sheet_oos_alert_indicator_sections(
    alerts_df=spc_oos_alerts_df,
    period_capability_df=period_capability_df,
    sheet_features_df=sheet_features_df,
    raw_measurements_df=raw_measurements_df,
    period_box_source=ConfigLoader.get_spc_period_box_source(),
    step_desc_map=step_desc_map,
)

if not should_render_report:
    st.info("当前筛选条件尚未查询。")
    st.stop()

filtered_period_capability_df = filter_spc_report(
    report_df=period_capability_df,
    selected_factory=selected_factory,
    selected_params=selected_params,
    selected_steps=selected_steps,
)
filtered_sheet_features_df = filter_spc_report(
    report_df=sheet_features_df,
    selected_factory=selected_factory,
    selected_params=selected_params,
    selected_steps=selected_steps,
)
filtered_raw_measurements_df = filter_spc_report(
    report_df=raw_measurements_df,
    selected_factory=selected_factory,
    selected_params=selected_params,
    selected_steps=selected_steps,
)

render_spc_indicator_sections(
    period_capability_df=filtered_period_capability_df,
    sheet_features_df=filtered_sheet_features_df,
    raw_measurements_df=filtered_raw_measurements_df,
    period_box_source=ConfigLoader.get_spc_period_box_source(),
    step_desc_map=step_desc_map,
)
