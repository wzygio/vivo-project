import streamlit as st
import pandas as pd
import numpy as np

# ==============================================================================
#  配置与初始化
# ==============================================================================
from app.utils.session_manager import SessionManager
from app.utils.app_setup import AppSetup

from yield_domain.application.alert_service import AlertService
from yield_domain.application.yield_service import YieldAnalysisService
from yield_domain.application.excel_service import ExcelService
from yield_domain.application.dtos import YieldQueryConfig
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

# 引入图表组件
from app.components.page_header import render_page_header, extract_cached_funcs
from app.components.code_selector import create_code_selection_ui
from app.components.alert_center import compute_lot_oos_records, render_alert_center, build_trend_context
from app.components.file_uploader import render_trend_override_uploader
from app.charts.mwd_chart import (
    prepare_union_data_for_filter
)
# [新增引入区块渲染组件]
from app.sections.yield_dashboard import (
    render_macro_trend_section,
    render_micro_trend_section,
    render_lot_distribution_section,
    render_sheet_distribution_section,
    render_mapping_section
)

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
AppSetup.initialize_app()

YIELD_DASHBOARD_CACHE_SIGNATURE = "yield_dashboard_manual_refresh_v1"

# [Refactor] 2. 获取上下文 (配置 & 路径)
active_config = SessionManager.get_active_config()
product_dir = SessionManager.get_product_dir()

# 依赖注入：初始化数据库连接
db_manager = DatabaseManager()

# L1 快照刷新参数：仅由页头“刷新数据”按钮触发。
start_dt, end_dt = YieldAnalysisService.get_time_window()
yield_query_config = YieldQueryConfig(
    product_code=active_config.data_source.product_code,
    start_date=start_dt.strftime("%Y-%m-%d"),
    end_date=end_dt.strftime("%Y-%m-%d"),
    work_order_types=getattr(active_config.data_source, "work_order_types", []),
    target_defect_groups=getattr(active_config.data_source, "target_defect_groups", []),
)
refresh_handlers = [
    lambda: YieldAnalysisService.safe_refresh_snapshots(
        db_manager,
        yield_query_config.model_dump_json(),
    )
]

# [Refactor] 3. 渲染页头
funcs_to_clear = extract_cached_funcs(YieldAnalysisService)
render_page_header(
    title="📊 入库不良率分析看板",
    config=active_config,
    cached_funcs=funcs_to_clear,
    refresh_handlers=refresh_handlers,
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
        snapshot_signature=YIELD_DASHBOARD_CACHE_SIGNATURE
    )
    mwd_code_data = YieldAnalysisService.get_code_level_trend_data(
        active_config,
        product_dir,
        _db_manager=db_manager,
        snapshot_signature=YIELD_DASHBOARD_CACHE_SIGNATURE
    )
    lot_data = YieldAnalysisService.get_lot_defect_rates(
        active_config,
        product_dir,
        _db_manager=db_manager,
        snapshot_signature=YIELD_DASHBOARD_CACHE_SIGNATURE,
    )
    sheet_data = YieldAnalysisService.get_sheet_defect_rates(
        active_config,
        product_dir,
        _db_manager=db_manager,
        snapshot_signature=YIELD_DASHBOARD_CACHE_SIGNATURE,
    )
    mapping_data = YieldAnalysisService.get_mapping_data(
        active_config,
        _db_manager=db_manager,
        snapshot_signature=YIELD_DASHBOARD_CACHE_SIGNATURE
    )
    warning_lines = YieldAnalysisService.load_static_warning_lines(
        active_config, product_dir
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
        config=active_config,
        product_dir=product_dir
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
render_macro_trend_section(
    mwd_group_data,
    group_order=active_config.data_source.target_defect_groups
)

st.divider()

# ==============================================================================
#  第二部分: 核心筛选器 (统一控制下方所有图表)
# ==============================================================================
st.subheader("2️⃣ 入库不良率分析 (Code Level)")

# 1. 准备“全能候选池”
master_df = prepare_union_data_for_filter(mwd_code_data, lot_data, mapping_data)

# 2. 渲染筛选器
selection = create_code_selection_ui(
    source_data=master_df,
    key_prefix="unified_focus"
)

# 如果没选 Code，下方不显示
if not selection.get("code"):
    st.info("👈 请在上方选择一个 Defect Code 以查看详细分析。")
    st.stop()

# 获取当前上下文
curr_code = selection["code"]
curr_group = selection["group"]
curr_warning = warning_lines.get(curr_code)

# [防御] 如果当前 Code 未在警戒线配置中找到，使用默认值防止 None['upper'] 报错
if curr_warning is None:
    curr_warning = {'upper': 0.002, 'lower': 0.0}

st.markdown(f"### 🎯 当前分析: **{curr_code}**")

# ==============================================================================
#  第三部分: 微观分析 (Code 级积木式拼装)
# ==============================================================================

# Row A: 时间趋势
render_micro_trend_section(mwd_code_data, curr_code, curr_warning['upper'])

# Row B: 批次分布 (返回被点击选中的 Lot)
target_lot = render_lot_distribution_section(lot_data, curr_code, curr_warning['upper'])

# Row C: 单片分布 (监听 Lot 点击状态)
render_sheet_distribution_section(sheet_data, target_lot, curr_group, curr_code)

# Row D: 空间热力图
hotspot_scripts = active_config.processing.get('mapping_hotspot_script', [])
render_mapping_section(
    mapping_data,
    curr_group,
    curr_code,
    hotspot_scripts,
    product_code=active_config.data_source.product_code,
)
