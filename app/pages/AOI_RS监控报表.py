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
from app.sections.inline_domain.aoi_rs.aoi_rs_dashboard import (
    build_aoi_rs_sheet_oos_alerts,
    filter_aoi_rs_report,
    get_default_aoi_rs_start_date,
    load_cached_aoi_rs_sheet_oos_decoration,
    render_aoi_rs_filters,
    render_aoi_rs_indicator_sections,
    render_aoi_rs_sheet_oos_alert_indicator_sections,
)
from app.sections.inline_domain.shared.alert_center import render_sheet_oos_alert_center
from app.utils.app_setup import AppSetup
from app.utils.step_labels import get_cached_step_description_map
from app.manager.session_manager import SessionManager
from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportService
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.shared.decorated_data import resolve_product_resource_dir
from src.inline_domain.application.shared.decision_signature import get_scope_decision_signature
from src.inline_domain.composition import build_aoi_rs_repository, refresh_aoi_rs_snapshots
from src.inline_domain.core.aoi_rs.aoi_rs_decoration import AOI_RS_OOS_DECORATION_FILE_NAME
from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range
from src.inline_domain.core.shared.sheet_oos_decoration import SheetOosDecorationReadError
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

AOI_RS_PAGE_CACHE_SIGNATURE = "aoi_rs_report_v1"


st.set_page_config(page_title="AOI_RS监控报表", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

active_config = SessionManager.get_active_config()
current_product = active_config.data_source.product_code
product_cache_signature = build_product_cache_signature(
    AOI_RS_PAGE_CACHE_SIGNATURE,
    current_product,
)
db_manager = DatabaseManager()
step_desc_map = get_cached_step_description_map(db_manager)

# 固定时间窗：上一自然月 1 日 ~ 当前日期（含当天），不提供时间筛选框
_, default_end_dt = MonitorAnalysisService.get_time_window()
default_start_dt = get_default_aoi_rs_start_date(default_end_dt.date())
query_config = AoiRsQueryConfig(
    prod_code=current_product,
    start_date=default_start_dt.strftime("%Y-%m-%d"),
    end_date=default_end_dt.strftime("%Y-%m-%d"),
)
aoi_rs_data_port = build_aoi_rs_repository(db_manager, current_product)

render_page_header(
    title="AOI_RS监控报表",
    config=active_config,
    cached_funcs=extract_cached_funcs(AoiRsReportService) + [get_cached_step_description_map],
    product_cache_scope=current_product,
    refresh_handlers=[
        lambda: refresh_aoi_rs_snapshots(db_manager, current_product, query_config.end_date)
    ],
)

# Phase 4 门控：共享产品 revision + 两阶段决策签名进入 L2 缓存键；
# 决策表读取失败时显式失败（不降级为空决策）。
product_revision = get_product_cache_revision(current_product)
try:
    decision_signature = get_scope_decision_signature("aoi_rs", current_product)
except SheetOosDecorationReadError:
    st.error(
        "AOI_RS 超规修饰表读取失败。请确认 Excel 文件可正常打开且未被锁定，"
        "然后点击页头“刷新缓存”重试。"
    )
    st.stop()

with st.spinner("正在加载 AOI RS 数据..."):
    view_model = AoiRsReportService.get_aoi_rs_report_data(
        _data_port=aoi_rs_data_port,
        query_config_json=query_config.model_dump_json(),
        snapshot_signature=product_cache_signature,
        product_revision=product_revision,
        decision_signature=decision_signature,
    )

rs_details_df = view_model.rs_details_df
pass_through_df = view_model.pass_through_df
spec_df = view_model.spec_df
indicator_df = view_model.indicators_df

if rs_details_df.empty or indicator_df.empty:
    st.info("当前产品暂无可展示的 AOI RS 数据。")
    st.stop()

selected_factory, selected_codes, selected_steps, should_render_report = render_aoi_rs_filters(
    indicator_df=indicator_df,
    step_desc_map=step_desc_map,
)

# 单片异常预警：只读加载工作簿（企业加密读取失败降级为 None，不阻断报表）
product_resource_dir = resolve_product_resource_dir(current_product)
decoration_path = product_resource_dir / AOI_RS_OOS_DECORATION_FILE_NAME
if decoration_path.exists():
    decoration_stat = decoration_path.stat()
    sheet_oos_decoration_df = load_cached_aoi_rs_sheet_oos_decoration(
        decoration_stat.st_mtime_ns,
        decoration_stat.st_size,
        current_product,
        str(product_resource_dir),
    )
else:
    sheet_oos_decoration_df = None

sheet_oos_alerts_df = build_aoi_rs_sheet_oos_alerts(
    sheet_oos_decoration_df,
    reference_date=default_end_dt.date(),
)
iso_week_start, _ = previous_iso_week_range(default_end_dt.date())
iso_week = iso_week_start.isocalendar()
render_sheet_oos_alert_center(
    sheet_oos_alerts_df,
    title=f"单片异常预警中心（上一周 {iso_week.year}-W{iso_week.week:02d}）",
    has_source_data=sheet_oos_decoration_df is not None,
    step_desc_map=step_desc_map,
)
render_aoi_rs_sheet_oos_alert_indicator_sections(
    alerts_df=sheet_oos_alerts_df,
    rs_details_df=rs_details_df,
    pass_through_df=pass_through_df,
    spec_df=spec_df,
    indicators_df=indicator_df,
    lot_points_df=view_model.lot_points_df,
    sheet_points_df=view_model.sheet_points_df,
    end_date=default_end_dt.date(),
    step_desc_map=step_desc_map,
)

if not should_render_report:
    st.info("当前筛选条件尚未查询。")
    st.stop()

render_aoi_rs_indicator_sections(
    rs_details_df=filter_aoi_rs_report(rs_details_df, selected_factory, selected_codes, selected_steps),
    pass_through_df=filter_aoi_rs_report(pass_through_df, selected_factory, [], selected_steps),
    spec_df=spec_df,
    indicators_df=filter_aoi_rs_report(indicator_df, selected_factory, selected_codes, selected_steps),
    lot_points_df=filter_aoi_rs_report(view_model.lot_points_df, selected_factory, selected_codes, selected_steps),
    sheet_points_df=filter_aoi_rs_report(view_model.sheet_points_df, selected_factory, selected_codes, selected_steps),
    end_date=default_end_dt.date(),
    step_desc_map=step_desc_map,
)
