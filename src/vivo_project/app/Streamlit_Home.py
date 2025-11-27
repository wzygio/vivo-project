import streamlit as st
import sys
from pathlib import Path

# --- 1. 初始化系统环境 (资源缓存) ---
from vivo_project.config import CONFIG
from vivo_project.utils.app_setup import AppSetup

# 使用 cache_resource 避免每次刷新页面都重新运行初始化逻辑
@st.cache_resource
def init_global_resources():
    # 初始化日志和路径
    AppSetup.initialize_app()
    return True

init_global_resources()

# --- 提取配置 (用于首页仪表盘) ---
ui_config = CONFIG.get('ui', {})
icons = ui_config.get('icons', {})

# ==============================================================================
#                               页面内容定义
# ==============================================================================

def dashboard_page():
    """
    [保留原有功能] 首页仪表盘内容
    将原本的主页内容封装在此函数中，作为默认页面渲染
    """
    # 1. 欢迎与简介
    st.title(f"天柱不良分析平台") 

    # 2. 关键指标速览 (KPI Snapshot)
    st.header(f"{icons.get('chart_up', '📈')} 关键指标速览 (最近60天)") 

    # (这里的KPI数据是占位符，未来可以替换为真实查询)
    col1, col2, col3 = st.columns(3)
    col1.metric(label="总分析Sheet数", value="7,890", delta="1.2% (较上期)")
    col2.metric(label="平均Panel不良率", value="0.87%", delta="-0.05%", delta_color="inverse")
    col3.metric(label="最高风险缺陷", value="Array_Line", help="这是近期出现次数最多的缺陷类型")

    st.divider()
    
    # 注意：原有的 st.page_link 导航块已被移除，因为现在由左侧侧边栏统一管理
    st.info("👈 请点击左侧侧边栏选择具体的分析报表。")

# ==============================================================================
#                               路由配置 (Router)
# ==============================================================================

# 1. 定义首页 (指向本文件内的函数)
home_page = st.Page(
    dashboard_page,         # 直接调用上面的函数
    title="平台总览",        # 侧边栏名称
    icon=icons.get('dashboard', '📊'),
    default=True            # 设置为默认打开的页面
)

# 2. 定义功能页面 (指向具体的物理文件)
# 请确保这些路径与你实际的文件结构完全一致
page_lot = st.Page(
    "pages/入库不良率ByLot明细表.py", 
    title="ByLot 明细报表", 
    icon="📋"
)

page_sheet = st.Page(
    "pages/入库不良率BySheet明细表.py", 
    title="BySheet 明细报表", 
    icon="📦"
)

page_trend = st.Page(
    "pages/入库不良率月周天趋势图.py", 
    title="月周天趋势分析", 
    icon="📈"
)

page_focus = st.Page(
    "pages/入库不良率集中性分析图.py", 
    title="集中性分析", 
    icon="🔬"
)

page_parse = st.Page(
    "pages/解析资料.py", 
    title="解析资料库", 
    icon="📚"
)

# --- 3. 构建导航结构 ---
# 这里实现了分组显示，让侧边栏更条理
pg = st.navigation({
    "核心监控": [home_page],
    "报表查询": [page_lot, page_sheet],
    "深度分析": [page_trend, page_focus],
    "知识库": [page_parse]
})

# --- 4. 设置页面通用配置 ---
# 注意：set_page_config 必须在 st.navigation 之后，但在 pg.run() 之前(或内部)逻辑生效
st.set_page_config(
    page_title="天柱不良分析平台",
    page_icon=icons.get('dashboard', '📊'),
    layout="wide"
)

# --- 5. 启动应用 ---
pg.run()