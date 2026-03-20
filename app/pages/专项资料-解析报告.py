# src/vivo_project/app/pages/专项资料-解析报告.py
import streamlit as st
import os
import time
from pathlib import Path

# --- 1. 初始化与配置 ---
from app.utils.session_manager import SessionManager
from config import ConfigLoader
from yield_domain.application.ppt_service import PPTService
from yield_domain.application.pdf_service import PDFService
from app.components.components import render_page_header

# 页面基础设置
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

# --- 2. 获取动态上下文与路径 ---
active_config = SessionManager.get_active_config()
project_root = ConfigLoader.get_project_root()
product_dir = SessionManager.get_product_dir()  # 动态获取当前产品目录

# 渲染页头
render_page_header("📋 解析资料", active_config)

# 动态构建绝对路径 (取代全局变量)
doc_source_dir = product_dir / "analysis_files"
doc_source_dir.mkdir(parents=True, exist_ok=True)
img_cache_rel_dir = "data/doc_cache" # 缓存目录依然放在外层共用

# --- 3. 状态管理 ---
if 'viewing_file' not in st.session_state:
    st.session_state.viewing_file = None

# [防护机制]：如果切换了产品，导致当前正在查看的文件在当前产品目录下不存在了，则自动关闭预览
if st.session_state.viewing_file and not (doc_source_dir / st.session_state.viewing_file).exists():
    st.session_state.viewing_file = None

# --- 辅助函数 ---
def get_service_by_filename(filename, output_dir_name):
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.ppt', '.pptx']:
        return PPTService(output_dir_name, project_root), "PPT"
    elif ext in ['.pdf']:
        return PDFService(output_dir_name, project_root), "PDF"
    return None, None

# ==============================================================================
#  界面区域 A: 统一上传接口
# ==============================================================================
st.caption(f"下载或在线预览当前产品 ({active_config.data_source.product_code}) 的分析报告 (支持 PPTX 和 PDF)。")

with st.expander("📤 上传新解析报告", expanded=False):
    uploaded_files = st.file_uploader(
        "选择要上传的分析报告 (支持批量上传)", 
        type=['ppt', 'pptx', 'pdf'], 
        accept_multiple_files=True,
        key="report_uploader_top"
    )
    
    if uploaded_files:
        if st.button("🚀 确认上传并覆盖同名文件", type="primary", use_container_width=True):
            for uf in uploaded_files:
                target_path = doc_source_dir / uf.name
                
                # [核心逻辑] 先删后写，防止文件系统层面的损坏
                if target_path.exists():
                    try:
                        target_path.unlink()
                    except Exception as e:
                        st.error(f"❌ 无法覆盖旧文件 {uf.name}，可能正被占用: {e}")
                        continue
                
                with open(target_path, "wb") as f:
                    f.write(uf.getbuffer())
                    
            st.success(f"✅ {len(uploaded_files)} 个文件上传成功！")
            time.sleep(1)
            st.rerun()

st.divider()

# ==============================================================================
#  界面区域 B: 列表
# ==============================================================================
# 读取当前产品的文件列表
if doc_source_dir.exists():
    all_files = [f for f in os.listdir(doc_source_dir) if f.lower().endswith(('.pptx', '.ppt', '.pdf'))]
else:
    all_files = []

if not all_files:
    rel_display_path = doc_source_dir.relative_to(project_root)
    st.warning(f"文件夹 `{rel_display_path}` 为空，请在上方上传文件。")
else:
    st.markdown("### 资料列表")
    for doc_file in all_files:
        abs_file_path = doc_source_dir / doc_file
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
                if st.button("👁️ 查看", key=f"view_{doc_file}", use_container_width=True):
                    st.session_state.viewing_file = doc_file
                    
                    service, doc_type = get_service_by_filename(doc_file, img_cache_rel_dir)
                    if service:
                        rel_path_str = str(doc_source_dir.relative_to(project_root) / doc_file)
                        success = False
                        with st.spinner(f"正在启动 {doc_type} 引擎解析，请稍候..."):
                            success = service.convert_to_images(rel_path_str)
                        
                        if success:
                            st.rerun()
                        else:
                            st.error("解析失败，请检查日志。")
                            st.session_state.viewing_file = None
                    else:
                        st.error("不支持的文件类型。")

# ==============================================================================
#  界面区域 C: 预览
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

    service, _ = get_service_by_filename(current_file, img_cache_rel_dir)
    if service:
        images = service.get_images()
        if images:
            st.info(f"共加载 {len(images)} 页内容")
            for idx, img_path in enumerate(images):
                st.image(str(img_path), caption=f"Page {idx+1}", use_container_width=True)
        else:
            st.warning("缓存目录为空，请尝试重新点击“查看”。")