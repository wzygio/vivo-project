import streamlit as st
import os
from pathlib import Path
import logging
import time
import pandas as pd
import io

# 修改后：
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode, ColumnsAutoSizeMode

# --- 1. 基础配置与导入 ---
from vivo_project.config import CONFIG, PROJECT_ROOT
from vivo_project.utils.app_setup import AppSetup
from vivo_project.app.components.components import render_page_header

# 引入所有 Service
from vivo_project.services.file_manager_service import FileManagerService
from vivo_project.services.excel_service import ExcelService
from vivo_project.services.ppt_service import PPTService
from vivo_project.services.pdf_service import PDFService

# 使用 cache_resource 避免重复初始化
@st.cache_resource
def init_global_resources():
    AppSetup.initialize_app()
init_global_resources()

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
render_page_header("📋 专项资料")

# --- 2. 路径定义 ---
DOC_SOURCE_DIR = "resources/project_files"
IMG_CACHE_DIR = "data/doc_cache"

if not os.path.exists(DOC_SOURCE_DIR):
    os.makedirs(DOC_SOURCE_DIR)

# --- 3. 状态管理初始化 ---
if 'viewing_file' not in st.session_state:
    st.session_state.viewing_file = None
if 'viewing_type' not in st.session_state:
    st.session_state.viewing_type = None

# 初始化用于 Excel 编辑的专用状态
if 'editor_data' not in st.session_state:
    st.session_state.editor_data = None
if 'editor_timestamp' not in st.session_state:
    st.session_state.editor_timestamp = 0.0
if 'editor_file_ref' not in st.session_state:
    st.session_state.editor_file_ref = None

# --- 4. 获取并分类文件 ---
classified_files = FileManagerService.get_classified_files(DOC_SOURCE_DIR)

# 定义 UI 上显示的分类映射
category_map = [
    ("📂 北极星台账", classified_files['ledger'], True),
    ("📅 北极星周报", classified_files['weekly'], False),
    ("🗃️ 其他归档",     classified_files['others'], False)
]

# ==============================================================================
#  界面区域 A: 分类折叠列表
# ==============================================================================
st.caption("浏览、下载或在线预览各类分析报告与台账。")

for title, files, default_expanded in category_map:
    if not files: continue
    
    with st.expander(f"{title} ({len(files)})", expanded=default_expanded):
        for doc_file in files:
            f_type = FileManagerService.get_file_type(doc_file)
            file_path = os.path.join(DOC_SOURCE_DIR, doc_file)
            
            if f_type == 'EXCEL':
                icon, mime = "📗", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif f_type == 'PDF':
                icon, mime = "📕", "application/pdf"
            else: 
                icon, mime = "📊", "application/vnd.openxmlformats-officedocument.presentationml.presentation"

            with st.container(border=True):
                c_name, c_dl, c_view = st.columns([8, 1, 1])
                
                with c_name:
                    st.markdown(f"**{icon} {doc_file}**")
                
                with c_dl:
                    with open(file_path, "rb") as f:
                        st.download_button("⬇️ 下载", f, file_name=doc_file, mime=mime, key=f"dl_{doc_file}", use_container_width=True)
                
                with c_view:
                    if st.button("👁️ 查看", key=f"view_{doc_file}", use_container_width=True):
                        st.session_state.viewing_file = doc_file
                        st.session_state.viewing_type = f_type
                        
                        # 重置编辑器状态
                        st.session_state.editor_data = None
                        st.session_state.editor_timestamp = 0.0
                        st.session_state.editor_file_ref = None
                        
                        if f_type in ['PDF', 'PPT']:
                            service = PDFService(IMG_CACHE_DIR) if f_type == 'PDF' else PPTService(IMG_CACHE_DIR)
                            with st.spinner(f"正在启动 {f_type} 引擎解析..."):
                                success = service.convert_to_images(os.path.join(DOC_SOURCE_DIR, doc_file))
                            if success:
                                st.rerun()
                            else:
                                st.error("解析失败。")
                                st.session_state.viewing_file = None
                        else:
                            st.rerun()

# ==============================================================================
#  界面区域 B: 统一预览/编辑窗口
# ==============================================================================
if st.session_state.viewing_file:
    curr_file = st.session_state.viewing_file
    curr_type = st.session_state.viewing_type
    
    st.markdown("---")
    
    c_head, c_close = st.columns([9, 1])
    with c_head:
        if curr_type == 'EXCEL':
            st.subheader(f"📝 正在编辑: {curr_file}")
        else:
            st.subheader(f"📖 正在预览: {curr_file}")
            
    with c_close:
        if st.button("❌ 关闭", type="primary"):    
            st.session_state.viewing_file = None
            st.rerun()

    try:
        # Case 1: Excel 编辑器 (Ag-Grid 重构版)
        if curr_type == 'EXCEL':
            file_path = os.path.join(DOC_SOURCE_DIR, curr_file)
            
            # 1. 首次加载逻辑
            if st.session_state.editor_data is None or st.session_state.editor_file_ref != curr_file:
                with st.spinner("正在加载最新数据并校验版本..."):
                    df = ExcelService.load_and_clean_data(file_path)
                    ts = ExcelService.get_file_timestamp(file_path)
                    
                    st.session_state.editor_data = df
                    st.session_state.editor_timestamp = ts
                    st.session_state.editor_file_ref = curr_file
            
            # 2. Ag-Grid 配置构建
            if st.session_state.editor_data is not None:
                # 注入 JS 样式 (保持不变)
                status_cell_style = JsCode("""
                function(params) {
                    if (params.value == 'Open') {
                        return {'backgroundColor': '#ffebee', 'color': '#c62828', 'fontWeight': 'bold', 'borderRadius': '4px', 'textAlign': 'center'};
                    } else if (params.value == 'Close') {
                        return {'backgroundColor': '#e8f5e9', 'color': '#2e7d32', 'fontWeight': 'bold', 'borderRadius': '4px', 'textAlign': 'center'};
                    } else if (params.value == 'Monitor') {
                        return {'backgroundColor': '#fff3e0', 'color': '#ef6c00', 'fontWeight': 'bold', 'borderRadius': '4px', 'textAlign': 'center'};
                    }
                    return {'textAlign': 'center'};
                }
                """)

                # 初始化构建器
                gb = GridOptionsBuilder.from_dataframe(st.session_state.editor_data)
                
                # --- A. 全局默认配置 ---
                # resizable=True: 关键！允许用户像 Excel 一样鼠标拖动列宽
                gb.configure_default_column(
                    resizable=True, 
                    filterable=True, 
                    sortable=True,
                    editable=True,
                    wrapText=True,      
                    autoHeight=False,
                    cellStyle={'whiteSpace': 'normal'}
                )

                large_text_editor_params = {
                    "maxLength": 1000,  # 允许最大字符数
                    "rows": 10,         # 输入框默认行数
                    "cols": 50          # 输入框默认列宽
                }

                # --- B. 精确列宽配置 (Manual Width Control) ---
                # 这里是你最关心的部分，我根据截图中的数据类型预设了最佳宽度
                
                # 1. 极窄列 (No., 型号, 状态)
                gb.configure_column("No.", minWidth=80, pinned='left', editable=False, suppressMenu=True) # 序号锁定在左侧
                # gb.configure_column("Issue类型", minWidth=90, wrapText=True, autoHeight=False)
                gb.configure_column("Issue名称",  minWidth=90, wrapText=True, autoHeight=False)
                gb.configure_column("工艺段",  minWidth=90, wrapText=True, autoHeight=False)
                gb.configure_column("发现方",  minWidth=90, wrapText=True, autoHeight=False)
                gb.configure_column("型号",  minWidth=90, wrapText=True, autoHeight=False)
                gb.configure_column("北极星指标",  minWidth=90, wrapText=True, autoHeight=False)
                gb.configure_column("影响物料",  minWidth=90, wrapText=True, autoHeight=False)
                gb.configure_column("风险品处理结果",  minWidth=90, wrapText=True, autoHeight=False)
                gb.configure_column("是否优化指标",  minWidth=90, wrapText=True, autoHeight=False)
                gb.configure_column("产品处置共识结论",  minWidth=90, wrapText=True, autoHeight=False)
                


                # 4. 关键指标 (中等宽度)
                gb.configure_column("北极星指标",  minWidth=130, wrapText=True, autoHeight=False,
                    cellEditor="agLargeTextCellEditor",   # 指定使用多行编辑器
                    cellEditorParams=large_text_editor_params,
                    cellEditorPopup=True                  # 强制弹窗显示，体验更好
                )
                gb.configure_column("管控规格",  minWidth=200, wrapText=True, autoHeight=False,
                    cellEditor="agLargeTextCellEditor",   # 指定使用多行编辑器
                    cellEditorParams=large_text_editor_params,
                    cellEditorPopup=True                  # 强制弹窗显示，体验更好
                )
                gb.configure_column("Issue描述",  minWidth=200, wrapText=True, autoHeight=False,
                    cellEditor="agLargeTextCellEditor",   # 指定使用多行编辑器
                    cellEditorParams=large_text_editor_params,
                    cellEditorPopup=True                  # 强制弹窗显示，体验更好
                )
                gb.configure_column("风险品处理方案",  minWidth=200, wrapText=True, autoHeight=False,
                    cellEditor="agLargeTextCellEditor",   # 指定使用多行编辑器
                    cellEditorParams=large_text_editor_params,
                    cellEditorPopup=True                  # 强制弹窗显示，体验更好
                )
                gb.configure_column("风险品处理结果",  minWidth=200, wrapText=True, autoHeight=False,
                    cellEditor="agLargeTextCellEditor",   # 指定使用多行编辑器
                    cellEditorParams=large_text_editor_params,
                    cellEditorPopup=True                  # 强制弹窗显示，体验更好
                )
                gb.configure_column("优化项/成果输出", minWidth=200, wrapText=True, autoHeight=False,
                    cellEditor="agLargeTextCellEditor",   # 指定使用多行编辑器
                    cellEditorParams=large_text_editor_params,
                    cellEditorPopup=True                  # 强制弹窗显示，体验更好
                )
                
                
                # 5. 超宽长文本列 (重点解决对象！)
                gb.configure_column("Issue描述",  minWidth=300, wrapText=True, autoHeight=False,
                    cellEditor="agLargeTextCellEditor",   # 指定使用多行编辑器
                    cellEditorParams=large_text_editor_params,
                    cellEditorPopup=True                  # 强制弹窗显示，体验更好
                )
                gb.configure_column("原因分析",  minWidth=300, wrapText=True, autoHeight=False,
                    cellEditor="agLargeTextCellEditor",   # 指定使用多行编辑器
                    cellEditorParams=large_text_editor_params,
                    cellEditorPopup=True                  # 强制弹窗显示，体验更好
                )
                gb.configure_column("改善措施", minWidth=300, wrapText=True, autoHeight=False,
                    cellEditor="agLargeTextCellEditor",   # 指定使用多行编辑器
                    cellEditorParams=large_text_editor_params,
                    cellEditorPopup=True                  # 强制弹窗显示，体验更好
                )
                gb.configure_column("改善效果", minWidth=300, wrapText=True, autoHeight=False,
                    cellEditor="agLargeTextCellEditor",   # 指定使用多行编辑器
                    cellEditorParams=large_text_editor_params,
                    cellEditorPopup=True                  # 强制弹窗显示，体验更好
                )
                

                
                # 2. 状态列 (带颜色胶囊)
                gb.configure_column(
                    "状态", 
                    minWidth=90,
                    cellEditor='agSelectCellEditor',
                    cellEditorParams={'values': ['Open', 'Close', 'Monitor']},
                    cellStyle=status_cell_style
                )

                # 3. 日期列 (固定宽度)
                gb.configure_column(
                    "发生日期",
                    minWidth=120,
                    type=["dateColumnFilter", "customDateTimeFormat"],
                    custom_format_string='yyyy-MM-dd',
                    datePicker=True
                )

                # 3. 日期列 (固定宽度)
                gb.configure_column(
                    "关闭日期",
                    minWidth=120,
                    type=["dateColumnFilter", "customDateTimeFormat"],
                    custom_format_string='yyyy-MM-dd',
                    datePicker=True
                )

                # 构建配置对象
                gridOptions = gb.build()

                # 渲染表格
                st.info("💡 提示：表格已启用横向滚动，请向右滑动查看完整内容。")
                
                grid_response = AgGrid(
                    st.session_state.editor_data,
                    gridOptions=gridOptions,
                    height=900, 
                    width='90%',
                    data_return_mode=DataReturnMode.AS_INPUT, 
                    update_mode=GridUpdateMode.MODEL_CHANGED,
                    
                    # [修复点 1] 显式禁止自动调整，强制使用设定的 width
                    # 您之前的代码里缺少这一行，导致 AgGrid 默认还是尝试去适应屏幕
                    columns_auto_size_mode=ColumnsAutoSizeMode.NO_AUTOSIZE, 
                    
                    # [修复点 2] 保持 False，允许横向滚动条出现
                    fit_columns_on_grid_load=False, 

                    theme='alpine',
                    allow_unsafe_jscode=True,
                    enable_enterprise_modules=False
                )

                # 获取编辑后的数据
                edited_df = grid_response['data']

                # 3. 保存逻辑 (保持之前的乐观锁逻辑不变)
                col_save, col_info = st.columns([1, 5])
                with col_save:
                    save_status_container = st.empty()
                    
                    if st.button("💾 保存修改", type="primary", use_container_width=True):
                        with st.spinner("正在安全写入..."):
                            success, msg = ExcelService.save_data_with_lock(
                                file_path, 
                                edited_df, 
                                st.session_state.editor_timestamp
                            )
                            
                            if success:
                                st.success(msg)
                                logging.info(f"User saved file: {curr_file}")
                                st.session_state.editor_data = edited_df
                                st.session_state.editor_timestamp = ExcelService.get_file_timestamp(file_path)
                                time.sleep(1)
                                st.rerun()
                            else:
                                save_status_container.error(f"⚠️ {msg}")
                                # 逃生舱：备份下载
                                buffer = io.BytesIO()
                                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                                    edited_df.to_excel(writer, index=False)
                                
                                st.warning("您的修改尚未保存！请先下载您的修改版本作为备份。")
                                st.download_button(
                                    label="📥 下载我的修改备份",
                                    data=buffer,
                                    file_name=f"冲突备份_{curr_file}",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )

            else:
                st.warning("Excel 内容为空或读取失败。")

        # Case 2: PDF / PPT
        elif curr_type in ['PDF', 'PPT']:
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
        st.error(f"系统错误: {e}")
        logging.error(f"Render error: {e}", exc_info=True)