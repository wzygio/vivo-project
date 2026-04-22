import streamlit as st
import pandas as pd
import numpy as np

# ==============================================================================
#  配置与初始化
# ==============================================================================
from app.utils.session_manager import SessionManager
from src.shared_kernel.config import ConfigLoader
from app.utils.app_setup import AppSetup
from app.utils.reloader import get_project_revision

from yield_domain.application.alert_service import AlertService
from yield_domain.application.yield_service import YieldAnalysisService
from yield_domain.application.excel_service import ExcelService
from yield_domain.application.dtos import YieldQueryConfig
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from pathlib import Path

# 引入图表组件
from app.components.components import (
    create_code_selection_ui, 
    render_page_header, 
    render_lot_spec_alert,
    render_trend_override_uploader,
    extract_cached_funcs,
    setup_hot_reload
)
from app.charts.mwd_chart import (
    prepare_union_data_for_filter
)
# [新增引入区块渲染组件]
from app.components.yield_sections import (
    render_macro_trend_section,
    render_micro_trend_section,
    render_lot_distribution_section,
    render_sheet_distribution_section,
    render_mapping_section
)

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
setup_hot_reload(enable=True)
AppSetup.initialize_app()

# [Refactor] 2. 获取3上下文 (配置 & 路径)
active_config = SessionManager.get_active_config()
project_root = ConfigLoader.get_project_root()
product_dir = SessionManager.get_product_dir()

# ==============================================================================
# [核心修复] 构造后端需要的 Query JSON 并使用 Lambda 包裹 (完全对齐业务参数)
# ==============================================================================
current_product = active_config.data_source.product_code
start_dt, end_dt = YieldAnalysisService.get_time_window()

# 1. [新增] 安全提取底层的业务过滤参数 (兼容字典和对象模式)
# 提取工单类型
wo_types = getattr(active_config.data_source, 'work_order_types', [])
# 提取目标缺陷组
if isinstance(active_config.processing, dict):
    defect_groups = active_config.processing.get('target_defect_groups', [])
else:
    defect_groups = getattr(active_config.processing, 'target_defect_groups', [])

# 2. 实例化后端的 DTO，补齐核心过滤参数，确保刷新拉取的数据与常规查询 100% 严格一致
yield_query_config = YieldQueryConfig(
    product_code=current_product,
    start_date=start_dt.strftime("%Y-%m-%d"),
    end_date=end_dt.strftime("%Y-%m-%d"),
    work_order_types=wo_types,
    target_defect_groups=defect_groups
)

# [核心修复] 初始化数据库连接与快照签名
# 依赖注入：由 Service 层外部初始化 db_manager，避免 Repository 内部构造失败
db_manager = DatabaseManager()

snapshot_path = Path("data") / current_product / f"yield_snapshot_{current_product}.parquet"

# [核心修复] 使用 session_state 固定签名，防止 Service 写入快照后 mtime 变化导致 cache miss
# 签名只在浏览器刷新（新 Session）时重新计算，st.rerun() 期间保持稳定
sig_session_key = f"yield_snapshot_sig_{current_product}"
if sig_session_key not in st.session_state:
    st.session_state[sig_session_key] = YieldAnalysisService.compute_snapshot_signature(snapshot_path)
snapshot_sig = st.session_state[sig_session_key]

composite_key = f"{get_project_revision(project_root)}:{snapshot_sig}"

# 3. 利用闭包，安全地将带有参数的函数传给 Header
handlers = [
    lambda: YieldAnalysisService.safe_refresh_snapshots(db_manager, yield_query_config.model_dump_json())
]

# [Refactor] 3. 渲染页头
funcs_to_clear = extract_cached_funcs(YieldAnalysisService)
render_page_header(
    title="📊 入库不良率分析看板", 
    config=active_config,
    cached_funcs=funcs_to_clear,
    refresh_handlers=handlers  # 注入携带了全量参数的闭包
)

# [Refactor] 4. 渲染趋势图覆盖文件上传组件 (注入 config 用于刷新逻辑)
query_params = st.query_params
if query_params.get("admin") == "true":
    render_trend_override_uploader(active_config, product_dir)
ExcelService.inject_excel_overrides_to_config(active_config, product_dir)

# ==============================================================================
#  数据加载
# ==============================================================================
with st.spinner("正在加载全维度分析数据..."):
    # [Refactor] 5. 并行加载所有服务数据
    # [核心修复] 传入 snapshot_signature 与 db_manager，实现文件签名感知的缓存失效
    mwd_group_data = YieldAnalysisService.get_mwd_trend_data(
        active_config, 
        product_dir, 
        _db_manager=db_manager,
        snapshot_signature=composite_key
    )
    mwd_code_data = YieldAnalysisService.get_code_level_trend_data(
        active_config, 
        product_dir, 
        _db_manager=db_manager,
        snapshot_signature=composite_key
    )
    lot_data = YieldAnalysisService.get_lot_defect_rates(
        active_config, 
        product_dir, 
        _db_manager=db_manager,
        snapshot_signature=composite_key, 
    )
    sheet_data = YieldAnalysisService.get_sheet_defect_rates(
        active_config, 
        product_dir, 
        _db_manager=db_manager,
        snapshot_signature=composite_key, 
    )
    mapping_data = YieldAnalysisService.get_mapping_data(
        active_config, 
        _db_manager=db_manager,
        snapshot_signature=composite_key
    )
    warning_lines = YieldAnalysisService.load_static_warning_lines(
        active_config, product_dir
    )

# 基础校验
if not all([mwd_group_data, mwd_code_data, lot_data, sheet_data]):
    st.error("部分核心数据加载失败 (数据为空或数据库连接异常)，请检查后台日志。")
    st.stop()

# ==============================================================================
#  🚨 智能预警中心 (Intelligent Alert Center)
# ==============================================================================
if query_params.get("admin") == "true":
    with st.spinner("正在执行全维度智能预警扫描 (趋势监测 + Spec拦截)..."):
        # 1. 趋势预警
        trend_alerts = AlertService.get_dashboard_alerts(
            mwd_group_data=mwd_group_data,
            mwd_code_data=mwd_code_data,
            config=active_config,
            product_dir=product_dir
        )
        
        has_trend_alerts = len(trend_alerts) > 0
        
        # [A] 趋势异常通报区
        with st.expander("🛡️ 月周天数据异常预警", expanded=not has_trend_alerts):
            if has_trend_alerts:
                with st.container(border=True):
                    st.error(f"🚨 趋势监测发现 {len(trend_alerts)} 项异常波动 (需关注)")
                    for msg in trend_alerts:
                        st.markdown(msg)
            else:
                st.success("✅ 系统监测正常：未发现月周天良率异常。")

# ==============================================================================
#  第一部分: 宏观监控 (Group级趋势)
# ==============================================================================
render_lot_spec_alert(lot_data=lot_data, warning_lines=warning_lines)
st.subheader("1️⃣ 入库不良率分析 (Group Level)")
render_macro_trend_section(mwd_group_data)

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
render_mapping_section(mapping_data, curr_group, curr_code, hotspot_scripts)