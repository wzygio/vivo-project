# Home.py (旧称 app.py)
import streamlit as st
from pathlib import Path
import sys
import logging

# --- 1. 初始化与配置 ---
from vivo_project.app.setup import AppSetup
AppSetup.initialize_app()

from vivo_project.config import CONFIG
from vivo_project.services.workflow_handler import WorkflowHandler

# --- 提取配置 ---
ui_config = CONFIG.get('ui', {}) # 使用.get()更安全，避免KeyError
icons = ui_config.get('icons', {})

# --- UI 界面布局 ---
st.set_page_config(
    page_title="天柱不良分析平台",
    page_icon=icons.get('dashboard', '📊'), # 从配置调用图标
    layout="wide"
)

# --- 1. 欢迎与简介 ---
st.title(f"{icons.get('home', '🏠')} 天柱不良分析平台") # 从配置调用图标
st.markdown("本平台旨在提供一个自动化的数据分析与可视化工具，帮助工艺工程师快速定位和分析不良问题。")


# --- 2. 关键指标速览 (KPI Snapshot) ---
st.header(f"{icons.get('chart_up', '📈')} 关键指标速览 (最近60天)") # 从配置调用图标

# (这里的KPI数据是占位符，未来可以替换为真实查询)
col1, col2, col3 = st.columns(3)
col1.metric(label="总分析Sheet数", value="7,890", delta="1.2% (较上期)")
col2.metric(label="平均Panel不良率", value="0.87%", delta="-0.05%", delta_color="inverse")
col3.metric(label="最高风险缺陷", value="Array_Line", help="这是近期出现次数最多的缺陷类型")

st.divider()

# --- 3. 页面导航/目录 ---
st.header(f"{icons.get('report', '📚')} 导航") # 从配置调用图标
st.write("请从下方选择您需要分析的报表模块，或使用左侧侧边栏进行导航。")

with st.container(border=True):
    st.page_link(
        "pages/入库不良率ByLot报表.py", # 假设你的文件名是这个
        label=f"{icons.get('arrow_right', '➡️')} 入库不良率ByLot报表",
        icon=icons.get('report', '📋') # 使用不同的图标
    )

with st.container(border=True):
    st.page_link(
        "pages/入库不良率BySheet报表.py", # 假设你的文件名是这个
        label=f"{icons.get('arrow_right', '➡️')} 入库不良率BySheet报表",
        icon=icons.get('datasheet', '📄') # 使用不同的图标
    )

with st.container(border=True):
    st.page_link(
        "pages/入库不良率月周天趋势图.py", # 假设你的文件名是这个
        label=f"{icons.get('arrow_right', '➡️')} 入库不良率月周天趋势图",
        icon=icons.get('chart_up', '📈') # 使用不同的图标
    )

with st.container(border=True):
    st.page_link(
        "pages/入库不良率集中性分析图.py", # 假设你的文件名是这个
        label=f"{icons.get('arrow_right', '➡️')} 入库不良率集中性分析图",
        icon=icons.get('chart_up', '📈') # 使用不同的图标
    )

# --- 4. 辅助信息 (侧边栏) ---
st.sidebar.divider()
st.sidebar.info(
    f"""
    **{icons.get('info', 'ℹ️')} 项目说明**
    
    本应用用于查询指定产品的入库不良率。
    - **数据源**: 生产数据库
    - **缓存**: {CONFIG['application']['cache_ttl_hours']}小时更新
    - **技术支持**: [你的名字/部门]
    """
)
