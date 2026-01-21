import streamlit as st
import os
from pathlib import Path

# --- 1. 初始化与配置 ---
from vivo_project.config import CONFIG, PROJECT_ROOT # 引入 PROJECT_ROOT 用于绝对路径
from vivo_project.utils.app_setup import AppSetup
# 使用 cache_resource 避免重复初始化
@st.cache_resource
def init_global_resources():
    AppSetup.initialize_app()
init_global_resources()

from vivo_project.application.ppt_service import PPTService
from vivo_project.application.pdf_service import PDFService
from vivo_project.app.components.components import render_page_header

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
render_page_header("📋 解析资料")

# --- 2. 定义常量与路径 ---
DOC_SOURCE_DIR = "resources/analysis_files"
IMG_OUTPUT_DIR = "data/doc_cache"

if not os.path.exists(DOC_SOURCE_DIR):
    os.makedirs(DOC_SOURCE_DIR)

# --- 3. 状态管理 ---
if 'viewing_file' not in st.session_state:
    st.session_state.viewing_file = None

# --- 辅助函数 ---
def get_service_by_filename(filename, output_dir):
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.ppt', '.pptx']:
        return PPTService(output_dir), "PPT"
    elif ext in ['.pdf']:
        return PDFService(output_dir), "PDF"
    return None, None

# ==============================================================================
#  界面区域 A: 列表
# ==============================================================================
st.caption("下载或在线预览服务器上的分析报告 (支持 PPTX 和 PDF)。")

all_files = [f for f in os.listdir(DOC_SOURCE_DIR) if f.lower().endswith(('.pptx', '.ppt', '.pdf'))]

if not all_files:
    st.warning(f"文件夹 `{DOC_SOURCE_DIR}` 为空，请上传文件。")
else:
    st.markdown("### 资料列表")
    for doc_file in all_files:
        file_path = os.path.join(DOC_SOURCE_DIR, doc_file)
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
                with open(file_path, "rb") as f:
                    st.download_button("⬇️ 下载", f, file_name=doc_file, mime=mime_type, key=f"dl_{doc_file}", use_container_width=True)
            
            with col_view:
                # 按钮点击逻辑
                if st.button("👁️ 查看", key=f"view_{doc_file}", use_container_width=True):
                    st.session_state.viewing_file = doc_file
                    
                    service, doc_type = get_service_by_filename(doc_file, IMG_OUTPUT_DIR)
                    if service:
                        rel_path = os.path.join(DOC_SOURCE_DIR, doc_file)
                        
                        # [核心修复] Spinner 块独立
                        success = False
                        with st.spinner(f"正在启动 {doc_type} 引擎解析，请稍候..."):
                            success = service.convert_to_images(rel_path)
                        
                        # [核心修复] Rerun 必须在 Spinner 上下文之外调用
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

    service, _ = get_service_by_filename(current_file, IMG_OUTPUT_DIR)
    
    if service:
        images = service.get_images()
        if images:
            st.info(f"共加载 {len(images)} 页内容")
            for idx, img_path in enumerate(images):
                # [核心优化] 使用绝对路径显示图片，防止相对路径在不同环境下解析错误
                # 确保路径是字符串格式
                abs_img_path = str(Path(PROJECT_ROOT) / img_path)
                st.image(abs_img_path, caption=f"Page {idx+1}", use_container_width=True)
        else:
            st.warning("缓存目录为空，请尝试重新点击“查看”。")