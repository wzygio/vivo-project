import streamlit as st
import os
from pathlib import Path

# --- 1. 初始化与配置 ---
from vivo_project.config import CONFIG
from vivo_project.utils.app_setup import AppSetup
AppSetup.initialize_app()

# [新增] 引入 PDF 服务
from vivo_project.services.ppt_service import PPTService
from vivo_project.services.pdf_service import PDFService
from vivo_project.app.components.components import render_page_header

# 删除 set_page_config (因为 Home.py 已经设置了全局宽屏)
# st.set_page_config(layout="wide", initial_sidebar_state="collapsed") 

render_page_header("📋 解析资料")

# --- 2. 定义常量与路径 ---
# [修改] 改个更通用的名字，建议把 PPT 和 PDF 都放在这个文件夹里
DOC_SOURCE_DIR = "resources/analysis_files" 
IMG_OUTPUT_DIR = "data/doc_cache"      # 图片缓存目录

# 确保目录存在
if not os.path.exists(DOC_SOURCE_DIR):
    os.makedirs(DOC_SOURCE_DIR)

# --- 3. 状态管理 (Session State) ---
if 'viewing_file' not in st.session_state:
    st.session_state.viewing_file = None # 存储当前正在查看的文件名

# --- 辅助函数：获取对应的 Service ---
def get_service_by_filename(filename, output_dir):
    """根据文件后缀返回对应的 Service 实例"""
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.ppt', '.pptx']:
        return PPTService(output_dir), "PPT"
    elif ext in ['.pdf']:
        return PDFService(output_dir), "PDF"
    return None, None

# ==============================================================================
#  界面区域 A: 标题与文件列表
# ==============================================================================
st.caption("下载或在线预览服务器上的分析报告 (支持 PPTX 和 PDF)。")

# [修改] 扩展文件扫描范围
all_files = [f for f in os.listdir(DOC_SOURCE_DIR) if f.lower().endswith(('.pptx', '.ppt', '.pdf'))]

if not all_files:
    st.warning(f"文件夹 `{DOC_SOURCE_DIR}` 为空，请上传 PPT 或 PDF 文件。")
else:
    st.markdown("### 资料列表")
    
    # 遍历文件列表
    for doc_file in all_files:
        file_path = os.path.join(DOC_SOURCE_DIR, doc_file)
        ext = os.path.splitext(doc_file)[1].lower()
        
        # 根据类型设置图标和 MIME
        if ext == '.pdf':
            icon = "📕"
            mime_type = "application/pdf"
        else:
            icon = "📊"
            mime_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation"

        # --- 容器化设计 ---
        with st.container(border=True):
            # [图标+名称 (70%)]  [下载 (15%)]  [查看 (15%)]
            col_name, col_dl, col_view = st.columns([7, 1.5, 1.5])
            
            with col_name:
                st.markdown(f"**{icon} {doc_file}**")
            
            with col_dl:
                with open(file_path, "rb") as f:
                    st.download_button(
                        label="⬇️ 下载",
                        data=f,
                        file_name=doc_file,
                        mime=mime_type,
                        key=f"dl_{doc_file}",
                        use_container_width=True
                    )
            
            with col_view:
                # 查看按钮
                if st.button("👁️ 查看", key=f"view_{doc_file}", use_container_width=True):
                    # 1. 更新状态
                    st.session_state.viewing_file = doc_file
                    
                    # 2. 实例化对应的 Service
                    service, doc_type = get_service_by_filename(doc_file, IMG_OUTPUT_DIR)
                    
                    if service:
                        # 3. 立即触发转换
                        rel_path = os.path.join(DOC_SOURCE_DIR, doc_file)
                        with st.spinner(f"正在启动 {doc_type} 引擎解析 {doc_file}，请稍候..."):
                            success = service.convert_to_images(rel_path)
                            if not success:
                                st.error("解析失败，请检查后台日志。")
                                st.session_state.viewing_file = None
                            else:
                                st.rerun() # 刷新页面显示预览
                    else:
                        st.error("不支持的文件类型。")

# ==============================================================================
#  界面区域 B: 预览窗口 (仅当选择了文件时显示)
# ==============================================================================
if st.session_state.viewing_file:
    current_file = st.session_state.viewing_file
    st.markdown("---") 
    
    # 标题栏
    col_title, col_close = st.columns([9, 1])
    with col_title:
        st.subheader(f"📖 正在预览: {current_file}")
    with col_close:
        if st.button("❌ 关闭", type="primary"):
            st.session_state.viewing_file = None
            st.rerun()

    # [修改] 重新获取 Service 以读取图片
    # 因为 PPTService 和 PDFService 都往同一个 IMG_OUTPUT_DIR 写图片，
    # 且文件名格式都是 slide_xx.jpg 或 page_xx.png，
    # 所以只要实例化对应的 Service 调用 get_images() 即可。
    service, _ = get_service_by_filename(current_file, IMG_OUTPUT_DIR)
    
    if service:
        images = service.get_images()
        
        if images:
            st.info(f"共加载 {len(images)} 页内容")
            # 垂直滚动展示
            for idx, img_path in enumerate(images):
                st.image(img_path, caption=f"Page {idx+1}", use_container_width=True)
        else:
            st.warning("缓存目录为空，请尝试重新点击“查看”。")