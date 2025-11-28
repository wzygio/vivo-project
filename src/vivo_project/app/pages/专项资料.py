import streamlit as st
import os
from pathlib import Path

# --- 1. 基础配置与导入 ---
from vivo_project.config import CONFIG, PROJECT_ROOT
from vivo_project.utils.app_setup import AppSetup
from vivo_project.app.components.components import render_page_header

# 引入所有 Service
from vivo_project.services.file_manager_service import FileManagerService
from vivo_project.services.excel_service import ExcelService
from vivo_project.services.ppt_service import PPTService
from vivo_project.services.pdf_service import PDFService

AppSetup.initialize_app()

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
render_page_header("📋 解析资料阅览室")

# --- 2. 路径定义 ---
DOC_SOURCE_DIR = "resources/project_files"
IMG_CACHE_DIR = "data/doc_cache"

if not os.path.exists(DOC_SOURCE_DIR):
    os.makedirs(DOC_SOURCE_DIR)

# --- 3. 状态管理 ---
# viewing_file: 当前选中的文件名
# viewing_type: 'PPT', 'PDF', 'EXCEL'
if 'viewing_file' not in st.session_state:
    st.session_state.viewing_file = None
if 'viewing_type' not in st.session_state:
    st.session_state.viewing_type = None

# --- 4. 获取并分类文件 ---
classified_files = FileManagerService.get_classified_files(DOC_SOURCE_DIR)

# 定义 UI 上显示的分类映射
category_map = [
    ("📂 北极星台账", classified_files['ledger'], True),   # 标题, 文件列表, 默认展开
    ("📅 北极星周报", classified_files['weekly'], False),
    ("🗃️ 其他归档",     classified_files['others'], False)
]

# ==============================================================================
#  界面区域 A: 分类折叠列表
# ==============================================================================
st.caption("浏览、下载或在线预览各类分析报告与台账。")

# 遍历三个分类，生成 UI
for title, files, default_expanded in category_map:
    if not files: continue # 如果该分类没文件，就不显示
    
    with st.expander(f"{title} ({len(files)})", expanded=default_expanded):
        for doc_file in files:
            # 获取类型
            f_type = FileManagerService.get_file_type(doc_file)
            file_path = os.path.join(DOC_SOURCE_DIR, doc_file)
            
            # 设定图标和 MIME
            if f_type == 'EXCEL':
                icon, mime = "📗", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif f_type == 'PDF':
                icon, mime = "📕", "application/pdf"
            else: # PPT
                icon, mime = "📊", "application/vnd.openxmlformats-officedocument.presentationml.presentation"

            # 渲染单行卡片
            with st.container(border=True):
                c_name, c_dl, c_view = st.columns([8, 1, 1])
                
                with c_name:
                    st.markdown(f"**{icon} {doc_file}**")
                
                with c_dl:
                    with open(file_path, "rb") as f:
                        st.download_button("⬇️ 下载", f, file_name=doc_file, mime=mime, key=f"dl_{doc_file}", use_container_width=True)
                
                with c_view:
                    # 查看按钮逻辑
                    if st.button("👁️ 查看", key=f"view_{doc_file}", use_container_width=True):
                        st.session_state.viewing_file = doc_file
                        st.session_state.viewing_type = f_type
                        
                        # --- 特殊处理: PDF/PPT 需要预处理转图片 ---
                        if f_type in ['PDF', 'PPT']:
                            # 根据类型选择服务
                            service = PDFService(IMG_CACHE_DIR) if f_type == 'PDF' else PPTService(IMG_CACHE_DIR)
                            
                            with st.spinner(f"正在启动 {f_type} 引擎解析，请稍候..."):
                                success = service.convert_to_images(os.path.join(DOC_SOURCE_DIR, doc_file))
                            
                            if success:
                                st.rerun() # 成功后刷新以显示图片
                            else:
                                st.error("解析失败，请检查日志。")
                                st.session_state.viewing_file = None
                        
                        else:
                            # Excel 不需要预处理，直接刷新显示
                            st.rerun()

# ==============================================================================
#  界面区域 B: 统一预览窗口
# ==============================================================================
if st.session_state.viewing_file:
    curr_file = st.session_state.viewing_file
    curr_type = st.session_state.viewing_type
    
    st.markdown("---")
    
    # 预览头
    c_head, c_close = st.columns([9, 1])
    with c_head:
        st.subheader(f"📖 正在预览: {curr_file}")
    with c_close:
        if st.button("❌ 关闭", type="primary"):
            st.session_state.viewing_file = None
            st.rerun()

    # --- 多态渲染逻辑 ---
    try:
        # Case 1: Excel 渲染
        if curr_type == 'EXCEL':
            df = ExcelService.load_and_clean_data(os.path.join(DOC_SOURCE_DIR, curr_file))
            if not df.empty:
                # 应用样式
                if '状态' in df.columns:
                    styled_df = df.style.map(ExcelService.highlight_status, subset=['状态'])
                else:
                    styled_df = df
                
                # 渲染表格
                st.dataframe(
                    styled_df,
                    height=600,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "No.": st.column_config.NumberColumn("No.", format="%d", width="small"),
                        "Issue描述": st.column_config.TextColumn(width="large"),
                        "原因分析": st.column_config.TextColumn(width="large"),
                        "状态": st.column_config.TextColumn(width="small")
                    }
                )
            else:
                st.warning("Excel 内容为空或读取失败。")

        # Case 2: PDF / PPT 渲染 (读取缓存图片)
        elif curr_type in ['PDF', 'PPT']:
            # 重新实例化服务以获取图片列表
            service = PDFService(IMG_CACHE_DIR) if curr_type == 'PDF' else PPTService(IMG_CACHE_DIR)
            images = service.get_images()
            
            if images:
                st.info(f"共加载 {len(images)} 页内容")
                for idx, img_path in enumerate(images):
                    abs_path = str(Path(PROJECT_ROOT) / img_path)
                    st.image(abs_path, caption=f"Page {idx+1}", use_container_width=True)
            else:
                st.warning("缓存图片丢失，请重新点击“查看”按钮。")
                
    except Exception as e:
        st.error(f"预览渲染出错: {e}")