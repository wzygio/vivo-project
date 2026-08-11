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
from app.sections.aoi_tt.aoi_tt_dashboard import (
    filter_aoi_tt_report,
    get_default_aoi_tt_start_date,
    render_aoi_tt_filters,
    render_aoi_tt_indicator_sections,
)
from app.utils.app_setup import AppSetup
from app.manager.session_manager import SessionManager
from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportService
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.infrastructure.aoi_tt.data_loader import AoiTtQueryConfig
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

AOI_TT_PAGE_CACHE_SIGNATURE = "aoi_tt_report_v1"


st.set_page_config(page_title="AOI_TT监控报表", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

active_config = SessionManager.get_active_config()
current_product = active_config.data_source.product_code
product_cache_signature = build_product_cache_signature(
    AOI_TT_PAGE_CACHE_SIGNATURE,
    current_product,
)
db_manager = DatabaseManager()

# 固定时间窗：上一自然月 1 日 ~ 当前日期（含当天），不提供时间筛选框
_, default_end_dt = MonitorAnalysisService.get_time_window()
default_start_dt = get_default_aoi_tt_start_date(default_end_dt.date())
query_config = AoiTtQueryConfig(
    prod_code=current_product,
    start_date=default_start_dt.strftime("%Y-%m-%d"),
    end_date=default_end_dt.strftime("%Y-%m-%d"),
)

render_page_header(
    title="AOI_TT监控报表",
    config=active_config,
    cached_funcs=extract_cached_funcs(AoiTtReportService),
    product_cache_scope=current_product,
)

with st.spinner("正在加载 AOI TT 数据..."):
    view_model = AoiTtReportService.get_aoi_tt_report_data(
        _db_manager=db_manager,
        query_config_json=query_config.model_dump_json(),
        snapshot_signature=product_cache_signature,
    )

tt_details_df = view_model.tt_details_df
spec_df = view_model.spec_df
indicator_df = view_model.indicators_df

if tt_details_df.empty or indicator_df.empty:
    st.info("当前产品暂无可展示的 AOI TT 数据。")
    st.stop()

selected_factory, selected_codes, selected_steps, should_render_report = render_aoi_tt_filters(
    indicator_df=indicator_df
)
if not should_render_report:
    st.info("当前筛选条件尚未查询。")
    st.stop()

render_aoi_tt_indicator_sections(
    tt_details_df=filter_aoi_tt_report(tt_details_df, selected_factory, selected_codes, selected_steps),
    spec_df=spec_df,
    indicators_df=filter_aoi_tt_report(indicator_df, selected_factory, selected_codes, selected_steps),
    end_date=default_end_dt.date(),
)
