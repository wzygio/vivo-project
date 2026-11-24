import logging
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

from src.shared_kernel.config import ConfigLoader

from app.components.indicator_cache import (
    build_indicator_product_cache_signature,
    get_indicator_product_revision,
)

PRODUCT_CACHE_INDICATORS = ("aoi_rs_sheet_oos",)

from app.components.page_header import (
    extract_cached_funcs,
    render_page_header,
)
from app.sections.inline_domain.aoi_rs.aoi_rs_dashboard import (
    render_saved_aoi_rs_alerts,
    render_aoi_rs_report_interaction,
    publish_aoi_rs_report_alerts,
    get_default_aoi_rs_start_date,
)
from app.sections.inline_domain.aoi_rs.aoi_rs_export import render_aoi_rs_export_interaction as render_aoi_rs_export
from app.sections.inline_domain.aoi_rs.aoi_rs_weekly_export import render_aoi_rs_weekly_export_interaction as render_aoi_rs_weekly_export
from app.utils.app_setup import AppSetup
from app.utils.step_labels import get_cached_step_description_map
from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.application.aoi_rs.aoi_rs_service import (
    AoiRsReportBuildError,
    AoiRsReportService,
)
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.shared.decision_signature import get_scope_decision_signature
from src.inline_domain.composition import build_aoi_rs_repository, refresh_aoi_rs_snapshots
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    SheetOosDecorationReadError,
)
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

AOI_RS_PAGE_CACHE_SIGNATURE = "aoi_rs_report_v1"


def _render_saved_page_alerts(report=None) -> None:
    render_saved_aoi_rs_alerts(
        current_product, reference_date=default_end_dt.date(), report=report,
        step_desc_map=step_desc_map,
    )


st.set_page_config(page_title="AOI_RS监控报表", layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

_, default_end_dt = MonitorAnalysisService.get_time_window()
current_product = render_page_header(
    title="AOI_RS监控报表",
    cached_funcs=extract_cached_funcs(AoiRsReportService) + [get_cached_step_description_map],
    product_cache_indicators=PRODUCT_CACHE_INDICATORS,
    product_refresh_handler=lambda product: refresh_aoi_rs_snapshots(
        DatabaseManager(), product, default_end_dt.date().isoformat(),
    ),
)
product_cache_signature = build_indicator_product_cache_signature(
    AOI_RS_PAGE_CACHE_SIGNATURE,
    current_product,
    PRODUCT_CACHE_INDICATORS,
)
step_desc_map = {}
try:
    db_manager = DatabaseManager()
    step_desc_map = get_cached_step_description_map(db_manager)
except Exception:
    logging.getLogger(__name__).exception("AOI_RS report setup failed")
    _render_saved_page_alerts()
    st.error("AOI_RS 报表数据加载失败，请稍后重试。")
    st.stop()

# 固定时间窗：上一自然月 1 日 ~ 当前日期（含当天），不提供时间筛选框
default_start_dt = get_default_aoi_rs_start_date(default_end_dt.date())
query_config = AoiRsQueryConfig(
    prod_code=current_product,
    start_date=default_start_dt.strftime("%Y-%m-%d"),
    end_date=default_end_dt.strftime("%Y-%m-%d"),
)

render_aoi_rs_export(
    current_product=current_product,
    start_date=default_start_dt,
    end_date=default_end_dt.date(),
    db_manager=db_manager,
    step_desc_map=step_desc_map,
)

render_aoi_rs_weekly_export(
    current_product=current_product,
    start_date=default_start_dt,
    end_date=default_end_dt.date(),
    db_manager=db_manager,
    step_desc_map=step_desc_map,
)

# Phase 4 门控：共享产品 revision + 两阶段决策签名进入 L2 缓存键；
# 决策表读取失败时显式失败（不降级为空决策）。
product_revision = get_indicator_product_revision("aoi_rs_sheet_oos", current_product)
try:
    decision_signature = get_scope_decision_signature("aoi_rs", current_product)
except SheetOosDecorationReadError:
    logging.getLogger(__name__).exception("AOI RS report inputs could not be read")
    st.error(
        "AOI_RS 数据读取失败，请稍后重试。"
    )
    _render_saved_page_alerts()
    st.stop()
except Exception:
    logging.getLogger(__name__).exception("AOI_RS decision inputs could not be read")
    _render_saved_page_alerts()
    st.error("AOI_RS 报表数据加载失败，请稍后重试。")
    st.stop()

try:
    aoi_rs_data_port = build_aoi_rs_repository(db_manager, current_product)
    with st.spinner("正在加载 AOI RS 数据..."):
        view_model = AoiRsReportService.get_aoi_rs_report_data(
            _data_port=aoi_rs_data_port,
            query_config_json=query_config.model_dump_json(),
            snapshot_signature=product_cache_signature,
            product_revision=product_revision,
            decision_signature=decision_signature,
        )
except AoiRsReportBuildError:
    st.error("AOI RS 报表加载失败，请稍后重试或点击页头“刷新缓存”。")
    _render_saved_page_alerts()
    st.stop()
except Exception:
    logging.getLogger(__name__).exception("AOI_RS report generation failed")
    _render_saved_page_alerts()
    st.error("AOI_RS 报表加载失败，请稍后重试。")
    st.stop()

rs_details_df = view_model.rs_details_df
indicator_df = view_model.indicators_df

if rs_details_df.empty or indicator_df.empty:
    st.info("当前产品暂无可展示的 AOI RS 数据。")
    _render_saved_page_alerts(view_model)
    st.stop()

publication_failed = not publish_aoi_rs_report_alerts(
    current_product, default_end_dt.date(), view_model,
)
render_aoi_rs_report_interaction(
    view_model, product=current_product, reference_date=default_end_dt.date(),
    step_desc_map=step_desc_map, publication_failed=publication_failed,
)
