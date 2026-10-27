from app.sections.yield_domain.data_health import render_yield_data_health
import streamlit as st
import pandas as pd
import numpy as np
import logging

# ==============================================================================
#  配置与初始化
# ==============================================================================
from app.manager.session_manager import SessionManager
from app.utils.app_setup import AppSetup

from yield_domain.application.yield_service import (
    YieldAnalysisService,
    YieldDataLoadError,
    YieldDataModificationError,
    YieldWarningLinesReadError,
)
from src.yield_domain.infrastructure.rate_override_repository import RateOverrideReadError
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

# 引入图表组件
from app.components.page_header import (
    extract_cached_funcs,
    render_page_header,
)
from app.components.indicator_cache import build_indicator_product_cache_signature
from src.yield_domain.application.alert_publication import generate_yield_alert_publications
from src.yield_domain.composition import build_yield_alert_results
from app.components.file_uploader import render_yield_config_uploader
from app.charts.yield_domain.mwd_chart import (
    prepare_union_data_for_filter
)
# [新增引入区块渲染组件]
from app.sections.yield_domain.yield_dashboard import (
    render_macro_trend_section,
    render_alert_code_expanders,
)
from app.sections.yield_domain.resource_config import get_dashboard_resource_config, get_code_monthly_rate_threshold
from app.sections.yield_domain.group_trend_data import YIELD_SNAPSHOT_SIGNATURE_BASE
from app.sections.yield_domain.code_analysis import render_code_analysis_section
from app.sections.yield_domain.published_alert_center import render_saved_yield_alerts

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

YIELD_DASHBOARD_CACHE_SIGNATURE = YIELD_SNAPSHOT_SIGNATURE_BASE

# [Refactor] 2. 获取上下文 (配置 & 路径)
active_config = SessionManager.get_active_config()
product_dir = SessionManager.get_product_dir()
product_cache_signature = build_indicator_product_cache_signature(
    YIELD_DASHBOARD_CACHE_SIGNATURE,
    active_config.data_source.product_code,
    ("yield_trend_fluctuation",),
)
lot_cache_signature = build_indicator_product_cache_signature(
    YIELD_DASHBOARD_CACHE_SIGNATURE, active_config.data_source.product_code,
    ("yield_lot_oos",),
)
sheet_cache_signature = build_indicator_product_cache_signature(
    YIELD_DASHBOARD_CACHE_SIGNATURE, active_config.data_source.product_code,
    ("yield_sheet_oos",),
)

# 依赖注入：初始化数据库连接
db_manager = DatabaseManager()

refresh_handlers = [
    lambda: YieldAnalysisService.safe_refresh_snapshots(
        db_manager,
        active_config,
    )
]

# [Refactor] 3. 渲染页头
funcs_to_clear = extract_cached_funcs(YieldAnalysisService)
render_page_header(
    title="📊 入库不良率分析看板",
    config=active_config,
    cached_funcs=funcs_to_clear,
    refresh_handlers=refresh_handlers,
    product_cache_scope=active_config.data_source.product_code,
    product_cache_indicators=("yield_trend_fluctuation", "yield_lot_oos", "yield_sheet_oos"),
)

# [Refactor] 4. 渲染 Yield 配置文件上传组件
query_params = st.query_params
if query_params.get("admin") == "true":
    render_yield_config_uploader(active_config, product_dir)
try:
    active_config = get_dashboard_resource_config(active_config, product_cache_signature)
    # Resource edits follow the original explicit-refresh policy.
    yield_cache_context = YieldAnalysisService.build_cache_context(
        active_config,
        product_dir,
        resource_revision=YIELD_DASHBOARD_CACHE_SIGNATURE,
    )
except Exception:
    logging.exception("Yield report inputs could not be prepared")
    render_saved_yield_alerts(active_config.data_source.product_code, pd.Timestamp.today().date())
    st.error("Yield 报表配置或资源读取失败，请稍后重试。")
    st.stop()

# ==============================================================================
#  数据加载
# ==============================================================================
# Publish prepared alerts before any Sheet/Mapping or chart-only gates.
try:
    with st.spinner("正在加载预警分析数据..."):
        alert_generation = generate_yield_alert_publications(
            analysis=YieldAnalysisService,
            publisher=build_yield_alert_results(),
            config=active_config,
            product_dir=product_dir,
            cache_context=yield_cache_context,
            trend_signature=product_cache_signature,
            lot_signature=lot_cache_signature,
            db_manager=db_manager,
        )
except Exception:
    logging.exception("Yield alert results could not be updated")
    render_saved_yield_alerts(active_config.data_source.product_code, pd.Timestamp.today().date())
    st.error("Yield 预警结果暂时无法更新，请稍后重试。")
    st.stop()
mwd_group_data = alert_generation["mwd_group_data"]
mwd_code_data = alert_generation["mwd_code_data"]
lot_data = alert_generation["lot_data"]
warning_lines = alert_generation["warning_lines"]
displayed_health = []
for health in alert_generation["health"].values():
    if health not in displayed_health:
        render_yield_data_health(health)
        displayed_health.append(health)
if alert_generation["publication_errors"]:
    st.warning("预警结果保存失败，请稍后重试。")
# Saved results are independent of Sheet/Mapping, chart settings and report gates.
saved_alerts = render_saved_yield_alerts(
    active_config.data_source.product_code,
    pd.Timestamp(yield_cache_context['analysis_end_date']).date(),
)
if alert_generation["errors"]:
    st.error("Yield 报表数据加载失败，请稍后重试。")
    st.stop()

try:
    code_rate_threshold = get_code_monthly_rate_threshold()
except (ValueError, TypeError, AttributeError):
    logging.exception("Invalid Yield code image threshold configuration")
    st.error("图像筛选设置不可用，请检查报表设置后重试。")
    st.stop()

try:
    health = YieldAnalysisService.get_data_health(
        active_config, _db_manager=db_manager, snapshot_signature=sheet_cache_signature,
        analysis_start_date=yield_cache_context["analysis_start_date"],
        analysis_end_date=yield_cache_context["analysis_end_date"],
    )
    if health not in displayed_health:
        render_yield_data_health(health)
        displayed_health.append(health)
    with st.spinner("正在加载全维度分析数据..."):
        sheet_data = YieldAnalysisService.get_sheet_defect_rates(
            active_config,
            product_dir,
            _db_manager=db_manager,
            snapshot_signature=sheet_cache_signature,
            analysis_start_date=yield_cache_context["analysis_start_date"],
            analysis_end_date=yield_cache_context["analysis_end_date"],
            modifier_signature=yield_cache_context["modifier_signature"],
            warning_signature=yield_cache_context["warning_signature"],
            rate_override_signature=yield_cache_context["rate_override_signature"],
        )
        mapping_data = YieldAnalysisService.get_mapping_data(
            active_config,
            _db_manager=db_manager,
            snapshot_signature=product_cache_signature,
            product_dir=product_dir,
            analysis_start_date=yield_cache_context["analysis_start_date"],
            analysis_end_date=yield_cache_context["analysis_end_date"],
            modifier_signature=yield_cache_context["modifier_signature"],
        )
except (
    YieldDataLoadError,
    YieldDataModificationError,
    YieldWarningLinesReadError,
    RateOverrideReadError,
):
    st.error("Yield 报表配置或资源读取失败，请检查相关文件后点击页头“刷新缓存”重试。")
    st.stop()

# 基础校验
if not all([mwd_group_data, mwd_code_data, lot_data, sheet_data]):
    st.info("该产品暂无足够数据进行分析，请检查数据源或稍后再试。")
    st.stop()

# ==============================================================================
#  🚨 智能预警中心 (Intelligent Alert Center)
# ==============================================================================
with st.spinner("正在准备预警指标图像..."):
    # Published alert keys select images; images never determine alert status.
    oos_records = saved_alerts['lot_records']
    alert_records = saved_alerts['trend_records']
    render_alert_code_expanders(
        trend_records=alert_records,
        lot_oos_records=oos_records,
        warning_lines=warning_lines,
        mwd_code_data=mwd_code_data,
        lot_data=lot_data,
        sheet_data=sheet_data,
        mapping_data=mapping_data,
        hotspot_scripts=active_config.processing.get('mapping_hotspot_script', []),
        product_code=active_config.data_source.product_code,
        mapping_layout=active_config.processing.get('mapping_layout'),
        data_signature=repr(sorted(yield_cache_context.items())),
        rate_threshold=code_rate_threshold,
    )

# ==============================================================================
#  第一部分: 宏观监控 (Group级趋势)
# ==============================================================================
st.subheader("1️⃣ 入库不良率分析 (Group Level)")
render_macro_trend_section(mwd_group_data)

st.divider()

# ==============================================================================
#  第二部分: 独立交互区（筛选与详细图表只触发 fragment rerun）
# ==============================================================================
# 候选池仅在整页加载时准备；筛选操作复用同一份全维度结果。
master_df = prepare_union_data_for_filter(mwd_code_data, lot_data, mapping_data)
render_code_analysis_section(
    master_df,
    rate_threshold=code_rate_threshold,
    warning_lines=warning_lines,
    mwd_code_data=mwd_code_data,
    lot_data=lot_data,
    sheet_data=sheet_data,
    mapping_data=mapping_data,
    hotspot_scripts=active_config.processing.get('mapping_hotspot_script', []),
    product_code=active_config.data_source.product_code,
    mapping_layout=active_config.processing.get('mapping_layout'),
)
