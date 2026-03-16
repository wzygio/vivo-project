import streamlit as st
import pandas as pd
import numpy as np

# --- [新增] 热重载机制 ---
ENABLE_HOT_RELOAD = True

if ENABLE_HOT_RELOAD:
    try:
        from vivo_project.utils.reloader import deep_reload_modules, get_project_revision
        from vivo_project.config import ConfigLoader
        
        # 1. 计算当前代码目录的真实哈希指纹
        project_root = ConfigLoader.get_project_root()
        current_rev = get_project_revision(project_root)
        
        # 2. 从 session_state 获取上一次的指纹
        last_rev = st.session_state.get('last_code_revision')
        
        # 3. 只有当代码指纹发生变化时，才执行暴力的模块卸载
        if last_rev is not None and last_rev != current_rev:
            deep_reload_modules()
            
        # 4. 更新指纹
        st.session_state['last_code_revision'] = current_rev
        
    except ImportError:
        pass

# --- 1. 配置与初始化 ---
from vivo_project.utils.session_manager import SessionManager
from vivo_project.config import ConfigLoader
from vivo_project.utils.app_setup import AppSetup
from vivo_project.utils.reloader import get_project_revision

from vivo_project.application.alert_service import AlertService
from vivo_project.application.yield_service import YieldAnalysisService
from vivo_project.application.excel_service import ExcelService

# 引入图表组件
from vivo_project.app.components.components import (
    create_code_selection_ui, 
    render_page_header, 
    render_lot_spec_alert,
    render_trend_override_uploader
)
from vivo_project.app.charts.mwd_chart import (
    prepare_union_data_for_filter
)
# [新增引入区块渲染组件]
from vivo_project.app.components.view_sections import (
    render_macro_trend_section,
    render_micro_trend_section,
    render_lot_distribution_section,
    render_sheet_distribution_section,
    render_mapping_section
)

# ==============================================================================
#  数据加载
# ==============================================================================
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

AppSetup.initialize_app()

# [Refactor] 2. 获取3上下文 (配置 & 路径)
active_config = SessionManager.get_active_config()
project_root = ConfigLoader.get_project_root()
product_dir = SessionManager.get_product_dir()

# [Refactor] 3. 渲染页头 (注入 config 用于刷新逻辑)
render_page_header("📊 入库不良率分析看板", active_config)

# [Refactor] 4. 渲染趋势图覆盖文件上传组件 (注入 config 用于刷新逻辑)
query_params = st.query_params
if query_params.get("admin") == "true":
    render_trend_override_uploader(active_config, product_dir)
ExcelService.inject_excel_overrides_to_config(active_config, product_dir)

# --- 2 全局数据加载 ---
with st.spinner("正在加载全维度分析数据..."):
    # [Refactor] 4. 获取核心版本号 (依赖注入 project_root)
    current_revision = get_project_revision(project_root)
    
    # 1. 获取当前产品代号
    current_product = st.session_state.get(SessionManager.KEY_PRODUCT, "Unknown")
    
    # 2. 定义默认参数 (兜底)

    # [Refactor] 5. 并行加载所有服务数据
    mwd_group_data = YieldAnalysisService.get_mwd_trend_data(
        active_config, 
        product_dir, 
        _core_revision=current_revision
    )
    mwd_code_data = YieldAnalysisService.get_code_level_trend_data(
        active_config, 
        product_dir, 
        _core_revision=current_revision
    )
    lot_data = YieldAnalysisService.get_lot_defect_rates(
        active_config, 
        product_dir, 
        _core_revision=current_revision, 
    )
    sheet_data = YieldAnalysisService.get_sheet_defect_rates(
        active_config, 
        product_dir, 
        _core_revision=current_revision, 
    )
    mapping_data = YieldAnalysisService.get_mapping_data(
        active_config, 
        _core_revision=current_revision
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
with st.spinner("正在执行全维度智能预警扫描 (趋势监测 + Spec拦截)..."):
    # 1. 趋势预警
    trend_alerts = AlertService.get_dashboard_alerts(
        mwd_group_data=mwd_group_data,
        mwd_code_data=mwd_code_data,
        config=active_config,
        product_dir=product_dir
    )
    
    has_trend_alerts = len(trend_alerts) > 0
    
    if query_params.get("admin") == "true":
        # [A] 趋势异常通报区
        with st.expander("🛡️ 月周天数据异常预警", expanded=not has_trend_alerts):
            if has_trend_alerts:
                with st.container(border=True):
                    st.error(f"🚨 趋势监测发现 {len(trend_alerts)} 项异常波动 (需关注)")
                    for msg in trend_alerts:
                        st.markdown(msg)
            else:
                st.success("✅ 系统监测正常：未发现月周天良率异常。")

    # [B] Lot 级良损(Spec)监控区 (只需一行调用！)
    render_lot_spec_alert(lot_data=lot_data, warning_lines=warning_lines)

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
render_micro_trend_section(mwd_code_data, curr_code, curr_warning)

# Row B: 批次分布 (返回被点击选中的 Lot)
target_lot = render_lot_distribution_section(lot_data, curr_code, curr_warning)

# Row C: 单片分布 (监听 Lot 点击状态)
render_sheet_distribution_section(sheet_data, target_lot, curr_group, curr_code)

# Row D: 空间热力图
hotspot_scripts = active_config.processing.get('mapping_hotspot_script', [])
render_mapping_section(mapping_data, curr_group, curr_code, hotspot_scripts)