# src/vivo_project/app/pages/入库不良率BySheet明细表.py
import streamlit as st

# --- 1. 初始化与配置 ---
from yield_domain.utils.session_manager import SessionManager
from config import ConfigLoader
from yield_domain.utils.reloader import get_project_revision

from yield_domain.application.yield_service import YieldAnalysisService
from yield_domain.app.components.components import render_page_header

# 引入区块渲染组件
from yield_domain.app.components.table_sections import (
    render_sheet_group_summary_section,
    render_sheet_code_details_section,
    render_sheet_top20_section
)

# --- 2. UI 界面布局 ---
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

# [关键修复]：必须先拿到 Config，再获取 product_dir
active_config = SessionManager.get_active_config()
project_root = ConfigLoader.get_project_root()
product_dir = SessionManager.get_product_dir()

render_page_header("📈 入库不良率BySheet明细表", active_config)

# --- 3. 加载数据 ---
core_rev = get_project_revision(project_root)
all_data = YieldAnalysisService.get_sheet_defect_rates(
    config=active_config, 
    product_dir=product_dir,
    _core_revision=core_rev
)

# --- 4. 页面积木式调度 ---
if all_data:
    # 模块 1：渲染总表并获得在时间/选项范围内的有效 Sheet ID 集合
    valid_sheet_ids = render_sheet_group_summary_section(all_data)
    
    if valid_sheet_ids:
        # 模块 2：手动查询指定 Sheet 的 Code 明细
        render_sheet_code_details_section(all_data, valid_sheet_ids)
        
        # 模块 3：选择 Code，反查 Top 20 严重 Sheet
        render_sheet_top20_section(all_data, valid_sheet_ids)
else:
    st.error("未能从后台加载Sheet数据，请检查后台日志或刷新重试。")