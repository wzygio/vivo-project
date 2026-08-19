import streamlit as st
import pandas as pd
import numpy as np

# ==============================================================================
#  配置与初始化
# ==============================================================================
from app.manager.session_manager import SessionManager
from app.utils.app_setup import AppSetup

from yield_domain.application.alert_service import AlertService
from yield_domain.application.yield_service import YieldAnalysisService
from yield_domain.application.excel_service import ExcelService
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

# 引入图表组件
from app.components.page_header import (
    build_product_cache_signature,
    extract_cached_funcs,
    render_page_header,
)
from app.components.code_selector import create_group_batch_selection_ui
from app.components.alert_center import compute_lot_oos_records, render_alert_center, build_trend_context
from app.components.file_uploader import render_trend_override_uploader
from app.charts.mwd_chart import (
    prepare_union_data_for_filter
)
# [新增引入区块渲染组件]
from app.sections.yield_domain.yield_dashboard import (
    render_macro_trend_section,
    render_code_compact_expanders,
)

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

YIELD_DASHBOARD_CACHE_SIGNATURE = "yield_dashboard_manual_refresh_v1"

# [Refactor] 2. 获取上下文 (配置 & 路径)
active_config = SessionManager.get_active_config()
product_dir = SessionManager.get_product_dir()
product_cache_signature = build_product_cache_signature(
    YIELD_DASHBOARD_CACHE_SIGNATURE,
    active_config.data_source.product_code,
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
)

# [Refactor] 4. 渲染趋势图覆盖文件上传组件
query_params = st.query_params
if query_params.get("admin") == "true":
    render_trend_override_uploader(active_config, product_dir)
ExcelService.inject_excel_overrides_to_config(active_config, product_dir)

# ==============================================================================
#  数据加载
# ==============================================================================
with st.spinner("正在加载全维度分析数据..."):
    mwd_group_data = YieldAnalysisService.get_mwd_trend_data(
        active_config,
        product_dir,
        _db_manager=db_manager,
        snapshot_signature=product_cache_signature
    )
    mwd_code_data = YieldAnalysisService.get_code_level_trend_data(
        active_config,
        product_dir,
        _db_manager=db_manager,
        snapshot_signature=product_cache_signature
    )
    lot_data = YieldAnalysisService.get_lot_defect_rates(
        active_config,
        product_dir,
        _db_manager=db_manager,
        snapshot_signature=product_cache_signature,
    )
    sheet_data = YieldAnalysisService.get_sheet_defect_rates(
        active_config,
        product_dir,
        _db_manager=db_manager,
        snapshot_signature=product_cache_signature,
    )
    mapping_data = YieldAnalysisService.get_mapping_data(
        active_config,
        _db_manager=db_manager,
        snapshot_signature=product_cache_signature
    )
    warning_lines = YieldAnalysisService.load_static_warning_lines(
        active_config,
        product_dir,
        product_cache_signature,
    )

# 基础校验
if not all([mwd_group_data, mwd_code_data, lot_data, sheet_data]):
    st.info("该产品暂无足够数据进行分析，请检查数据源或稍后再试。")
    st.stop()

# ==============================================================================
#  🚨 智能预警中心 (Intelligent Alert Center)
# ==============================================================================
with st.spinner("正在执行全维度智能预警扫描 (趋势监测 + Spec拦截)..."):
    # 1. 趋势预警
    trend_alerts = AlertService.get_dashboard_alerts(
        mwd_group_data=mwd_group_data,
        mwd_code_data=mwd_code_data,
        product_dir=product_dir,
        benchmark_report_config=active_config.processing.get(
            'benchmark_report_config', {}
        ),
    )
    
    # 2. 构建趋势监控上下文
    trend_context = build_trend_context(
        alert_service_result=trend_alerts,
        mwd_code_data=mwd_code_data,
        mwd_group_data=mwd_group_data
    )

    # 3. Lot 超规扫描
    oos_records, total_recent_lots = compute_lot_oos_records(
        lot_data=lot_data,
        warning_lines=warning_lines,
        time_period=30
    )

    # 4. 统一渲染
    render_alert_center(
        trend_alerts=trend_alerts,
        trend_context=trend_context,
        oos_records=oos_records,
        total_recent_lots=total_recent_lots,
        time_period=30
    )

# ==============================================================================
#  第一部分: 宏观监控 (Group级趋势)
# ==============================================================================
st.subheader("1️⃣ 入库不良率分析 (Group Level)")
render_macro_trend_section(mwd_group_data)

st.divider()

# ==============================================================================
#  第二部分: 核心筛选器 (统一控制下方所有图表)
# ==============================================================================
st.subheader("2️⃣ 入库不良率分析 (Code Level)")

# 1. 准备“全能候选池”
master_df = prepare_union_data_for_filter(mwd_code_data, lot_data, mapping_data)

# 2. 渲染 Group 批量筛选器
selection = create_group_batch_selection_ui(
    source_data=master_df,
    key_prefix="unified_focus"
)

selected_groups = selection.get("groups", [])
codes_by_group = selection.get("codes_by_group", {})
if not selected_groups or not codes_by_group:
    st.info("请选择至少一个包含有效 Code 的 Defect Group。")
    st.stop()

if not selection.get("should_render", False):
    st.info("当前筛选条件尚未查询。")
    st.stop()

# ==============================================================================
#  第三部分: 微观分析 (Group 下所有 Code 批量展示)
# ==============================================================================
hotspot_scripts = active_config.processing.get('mapping_hotspot_script', [])

# 查询门控通过后才进入两阶段渲染：RenderGate 先在统一 spinner 下构建
# 全部 Code payload，再按原顺序集中渲染，避免图表逐张出现。
render_code_compact_expanders(
    selected_groups=selected_groups,
    codes_by_group=codes_by_group,
    warning_lines=warning_lines,
    mwd_code_data=mwd_code_data,
    lot_data=lot_data,
    sheet_data=sheet_data,
    mapping_data=mapping_data,
    hotspot_scripts=hotspot_scripts,
    product_code=active_config.data_source.product_code,
    mapping_layout=active_config.processing.get('mapping_layout'),
)
