import sys
import os
from pathlib import Path

# 仅在 "入口文件" (被直接运行的文件) 中写这段代码，其他被 import 的业务文件里一行都不要写
current_file = Path(__file__).resolve()
src_root = current_file.parent.parent.parent 

if str(src_root) not in sys.path:
    sys.path.insert(0, str(src_root))
# ------------------------------------

import streamlit as st
import streamlit.components.v1 as components
from pathlib import Path

from vivo_project.utils.app_setup import AppSetup
@st.cache_resource
def init_portal_resources():
    AppSetup.initialize_app(log_name="portal_main.log")

# 1. 基础配置
st.set_page_config(
    page_title="Visionox M3 Portal",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ==============================================================================
#  核心魔法：CSS 样式注入 (去除 Streamlit 所有原生 UI，实现伪全屏)
# ==============================================================================
FULL_SCREEN_CSS = """
<style>
    /* 1. 隐藏顶部 Header (包括 Deploy 按钮、汉堡菜单、Running 动画) */
    header[data-testid="stHeader"] {
        display: none;
    }
    
    /* 2. 隐藏底部 Footer */
    footer {
        display: none;
    }
    
    /* 3. 移除主内容区域的默认内边距 (Padding) */
    .block-container {
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
        padding-left: 0rem !important;
        padding-right: 0rem !important;
        max-width: 100% !important;
    }
    
    /* 4. 移除顶部空白间隙 */
    .main > div {
        padding-top: 0rem !important;
    }
    
    /* 5. 强制 iframe 填满视口高度 (防止双滚动条) */
    iframe {
        height: 100vh !important; /* 强制占满 100% 视口高度 */
        display: block;
    }
    
    /* 6. 隐藏侧边栏本身 (防止鼠标误触边缘滑出) */
    [data-testid="stSidebar"] {
        display: none;
    }
</style>
"""
st.markdown(FULL_SCREEN_CSS, unsafe_allow_html=True)

# ==============================================================================
#  资源加载逻辑
# ==============================================================================
def load_resource(filename):
    """读取 resources/static 下的文件内容"""
    current_dir = Path(__file__).parent.resolve()
    # 路径回溯：src/vivo_project/app -> src/vivo_project -> src -> (Root) -> resources
    project_root = current_dir.parent.parent.parent
    static_dir = project_root / "resources" / "static"
    
    file_path = static_dir / filename
    
    if not file_path.exists():
        st.error(f"Error: 找不到文件 {file_path}")
        return ""
        
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def render_portal():
    # 1. 加载源码
    html_content = load_resource("index.html")
    css_content = load_resource("style.css")
    js_config = load_resource("config.js")
    js_logic = load_resource("script.js")

    # 2. 内联替换 (Inlining)
    # 将 CSS/JS 内容直接注入 HTML，确保 Streamlit iframe 能正确解析
    final_html = html_content.replace(
        '<link rel="stylesheet" href="style.css">',
        f"<style>{css_content}</style>"
    )
    final_html = final_html.replace(
        '<script src="config.js"></script>',
        f"<script>{js_config}</script>"
    )
    final_html = final_html.replace(
        '<script src="script.js"></script>',
        f"<script>{js_logic}</script>"
    )

    # 3. 渲染组件
    # height=1080 只是保底值，CSS 中的 100vh !important 会覆盖它实现全屏
    components.html(final_html, height=1080, scrolling=False)

if __name__ == "__main__":
    render_portal()