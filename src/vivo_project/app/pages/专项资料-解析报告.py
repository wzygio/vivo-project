import streamlit as st
import os
from pathlib import Path

# --- 1. 初始化与配置 ---
from vivo_project.utils.session_manager import SessionManager
from vivo_project.config import ConfigLoader
# 移除 AppSetup，由 SessionManager 接管配置上下文

from vivo_project.application.ppt_service import PPTService
from vivo_project.application.pdf_service import PDFService
from vivo_project.app.components.components import render_page_header

# 页面基础设置
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

# [Refactor] 2. 获取上下文
active_config = SessionManager.get_active_config()
project_root = ConfigLoader.get_project_root()
# resource_dir = SessionManager.get_resource_dir() # 本页暂未使用 resource_dir，主要用 project_root

# [Refactor] 3. 渲染页头 (注入 config)
render_page_header("📋 解析资料", active_config)

# --- 2. 定义常量与路径 ---
# 定义相对于根目录的路径
DOC_SOURCE_REL_DIR = "resources/analysis_files"
IMG_OUTPUT_REL_DIR = "data/doc_cache"

# 构建绝对路径用于文件操作
ABS_DOC_SOURCE_DIR = project_root / DOC_SOURCE_REL_DIR

if not ABS_DOC_SOURCE_DIR.exists():
    ABS_DOC_SOURCE_DIR.mkdir(parents=True, exist_ok=True)

# --- 3. 状态管理 ---
if 'viewing_file' not in st.session_state:
    st.session_state.viewing_file = None

# --- 辅助函数 ---
def get_service_by_filename(filename, output_dir_name):
    """
    工厂函数：根据文件名后缀返回对应的 Service 实例
    [Refactor] 注入 project_root
    """
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.ppt', '.pptx']:
        return PPTService(output_dir_name, project_root), "PPT"
    elif ext in ['.pdf']:
        return PDFService(output_dir_name, project_root), "PDF"
    return None, None

# ==============================================================================
#  界面区域 A: 列表
# ==============================================================================
st.caption("下载或在线预览服务器上的分析报告 (支持 PPTX 和 PDF)。")

# [Refactor] 使用绝对路径读取目录
if ABS_DOC_SOURCE_DIR.exists():
    all_files = [f for f in os.listdir(ABS_DOC_SOURCE_DIR) if f.lower().endswith(('.pptx', '.ppt', '.pdf'))]
else:
    all_files = []

if not all_files:
    st.warning(f"文件夹 `{DOC_SOURCE_REL_DIR}` 为空，请上传文件。")
else:
    st.markdown("### 资料列表")
    for doc_file in all_files:
        # 绝对路径用于文件读取
        abs_file_path = ABS_DOC_SOURCE_DIR / doc_file
        ext = os.path.splitext(doc_file)[1].lower()
        
        if ext == '.pdf':
            icon, mime_type = "📕", "application/pdf"
        else:
            icon, mime_type = "📊", "application/vnd.openxmlformats-officedocument.presentationml.presentation"

        with st.container(border=True):
            col_name, col_dl, col_view = st.columns([7, 1.5, 1.5])
            with col_name:
                st.markdown(f"**{icon} {doc_file}**")
            
            with col_dl:
                with open(abs_file_path, "rb") as f:
                    st.download_button("⬇️ 下载", f, file_name=doc_file, mime=mime_type, key=f"dl_{doc_file}", use_container_width=True)
            
            with col_view:
                # 按钮点击逻辑
                if st.button("👁️ 查看", key=f"view_{doc_file}", use_container_width=True):
                    st.session_state.viewing_file = doc_file
                    
                    # 实例化 Service (注入相对输出路径)
                    service, doc_type = get_service_by_filename(doc_file, IMG_OUTPUT_REL_DIR)
                    
                    if service:
                        # Service 内部使用的是相对于 project_root 的路径
                        rel_path_str = os.path.join(DOC_SOURCE_REL_DIR, doc_file)
                        
                        success = False
                        with st.spinner(f"正在启动 {doc_type} 引擎解析，请稍候..."):
                            # 传入相对路径字符串
                            success = service.convert_to_images(rel_path_str)
                        
                        if success:
                            st.rerun()
                        else:
                            st.error("解析失败，请检查日志。")
                            st.session_state.viewing_file = None
                    else:
                        st.error("不支持的文件类型。")

# ==============================================================================
#  界面区域 B: 预览
# ==============================================================================
if st.session_state.viewing_file:
    current_file = st.session_state.viewing_file
    st.markdown("---") 
    
    col_title, col_close = st.columns([9, 1])
    with col_title:
        st.subheader(f"📖 正在预览: {current_file}")
    with col_close:
        if st.button("❌ 关闭", type="primary"):
            st.session_state.viewing_file = None
            st.rerun()

    # 获取 Service 实例来读取图片
    service, _ = get_service_by_filename(current_file, IMG_OUTPUT_REL_DIR)
    
    if service:
        images = service.get_images()
        if images:
            st.info(f"共加载 {len(images)} 页内容")
            for idx, img_path in enumerate(images):
                # [Refactor] 使用 project_root 构建绝对路径进行显示
                # img_path 是相对于 output_dir 的或者包含了部分路径，get_images通常返回相对路径
                # 假设 get_images 返回的是相对于 project_root 或者 output_dir 的部分
                # 查看 service 实现：它返回的是 glob 结果。
                # 如果 glob 使用的是绝对路径 (project_root / output_dir)，则返回绝对路径。
                # 如果 Service 内部 output_dir 是 absolute Path，则 glob 返回 absolute strings。
                # 直接使用即可。
                # 但为了保险 (st.image 有时对路径敏感)，确保它是字符串。
                st.image(str(img_path), caption=f"Page {idx+1}", use_container_width=True)
        else:
            st.warning("缓存目录为空，请尝试重新点击“查看”。")