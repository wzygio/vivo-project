import streamlit as st
import os
from pathlib import Path
import logging
import pandas as pd
import time


# --- 1. 基础配置与导入 ---
from vivo_project.config import CONFIG, PROJECT_ROOT  # 导入项目配置和根路径
from vivo_project.utils.app_setup import AppSetup  # 导入应用初始化工具
from vivo_project.app.components.components import render_page_header  # 导入页头渲染组件

# 引入所有 Service
from vivo_project.application.file_manager_service import FileManagerService  # 文件管理服务
from vivo_project.application.excel_service import ExcelService  # Excel 服务
from vivo_project.application.ppt_service import PPTService  # PPT 服务
from vivo_project.application.pdf_service import PDFService  # PDF 服务

# 使用 cache_resource 避免重复初始化
@st.cache_resource
def init_global_resources():
    AppSetup.initialize_app()  # 初始化应用配置
init_global_resources()  # 执行初始化

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")  # 设置页面布局为宽屏
render_page_header("📋 专项资料")  # 渲染统一页头

# --- 2. 路径定义 ---
DOC_SOURCE_DIR = "resources/project_files"  # 文档源目录
IMG_CACHE_DIR = "data/doc_cache"  # 图片缓存目录

if not os.path.exists(DOC_SOURCE_DIR):  # 如果源目录不存在
    os.makedirs(DOC_SOURCE_DIR)  # 创建源目录

# --- 3. 状态管理初始化 ---
# 初始化用于文件浏览的状态
if 'viewing_file' not in st.session_state:
    st.session_state.viewing_file = None  # 当前查看的文件名
if 'viewing_type' not in st.session_state:
    st.session_state.viewing_type = None  # 当前查看的文件类型

# 初始化用于 Excel 编辑的专用状态
# 我们需要把数据和时间戳存在 Session 中，防止每次交互都被重置
if 'editor_data' not in st.session_state:
    st.session_state.editor_data = None  # 缓存的 DataFrame 数据
if 'editor_timestamp' not in st.session_state:
    st.session_state.editor_timestamp = 0.0  # 缓存的文件加载时间戳（用于乐观锁）
if 'editor_file_ref' not in st.session_state:
    st.session_state.editor_file_ref = None  # 记录当前缓存的是哪个文件，防止切文件时数据错乱

# --- 4. 获取并分类文件 ---
classified_files = FileManagerService.get_classified_files(DOC_SOURCE_DIR)  # 获取分类文件列表

# 定义 UI 上显示的分类映射
category_map = [
    ("📂 北极星台账", classified_files['ledger'], True),   # 标题, 文件列表, 默认展开
    ("📅 北极星周报", classified_files['weekly'], False),
    ("🗃️ 其他归档",     classified_files['others'], False)
]

# ==============================================================================
#  界面区域 A: 分类折叠列表
# ==============================================================================
st.caption("浏览、下载或在线预览各类分析报告与台账。")  # 页面说明

# 遍历三个分类，生成 UI
for title, files, default_expanded in category_map:
    if not files: continue # 如果该分类没文件，就不显示
    
    # --- 在循环外初始化一个状态，用于记录当前哪个文件的上传框是打开的 ---
if 'active_upload_file' not in st.session_state:
    st.session_state.active_upload_file = None

# 遍历三个分类，生成 UI
for title, files, default_expanded in category_map:
    if not files: continue 
    
    with st.expander(f"{title} ({len(files)})", expanded=default_expanded):
        for doc_file in files:
            # 获取类型
            f_type = FileManagerService.get_file_type(doc_file)
            file_path = os.path.join(DOC_SOURCE_DIR, doc_file)
            
            if f_type == 'EXCEL':
                icon, mime = "📗", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif f_type == 'PDF':
                icon, mime = "📕", "application/pdf"
            else: 
                icon, mime = "📊", "application/vnd.openxmlformats-officedocument.presentationml.presentation"

            # 渲染单行卡片
            with st.container(border=True):
                # [修改点] 调整列宽比例，增加一列给更新按钮: [7, 1, 1, 1]
                c_name, c_dl, c_up, c_view = st.columns([7, 1, 1, 1])
                
                with c_name:
                    st.markdown(f"**{icon} {doc_file}**")
                
                with c_dl:
                    with open(file_path, "rb") as f:
                        st.download_button("⬇️ 下载", f, file_name=doc_file, mime=mime, key=f"dl_{doc_file}", use_container_width=True)
                
                with c_up:
                    # [新增] 更新按钮 (作为开关)
                    # 点击时，如果当前已经打开就是关闭(设为None)，否则设为当前文件
                    is_active = (st.session_state.active_upload_file == doc_file)
                    btn_label = "📤 收起" if is_active else "📤 上传"
                    btn_type = "secondary" if not is_active else "primary"
                    
                    if st.button(btn_label, key=f"btn_up_{doc_file}", type=btn_type, use_container_width=True):
                        if is_active:
                            st.session_state.active_upload_file = None
                        else:
                            st.session_state.active_upload_file = doc_file
                        st.rerun()

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
                        else:
                            st.rerun()

            # ==========================================================
            # [新增功能] 动态展开的上传区域 (仅当状态匹配时显示)
            # ==========================================================
            if st.session_state.active_upload_file == doc_file:
                with st.container():
                    # 使用 info/warning 样式框突出显示区域
                    st.info(f"💡 建议先下载原文件，在原文件基础上进行修改，再进行上传。")

                    file_ext = os.path.splitext(doc_file)[1].replace(".", "")
                    uploaded_new_file = st.file_uploader(
                        f"选择新的 .{file_ext} 文件进行覆盖", 
                        type=[file_ext], 
                        key=f"uploader_{doc_file}",
                        label_visibility="collapsed" # 隐藏 label 使布局更紧凑
                    )
                    
                    if uploaded_new_file is not None:
                        # 1. 执行覆盖
                        with open(file_path, "wb") as f:
                            f.write(uploaded_new_file.getbuffer())
                        
                        # 2. 清除缓存
                        if st.session_state.viewing_file == doc_file:
                            st.session_state.editor_data = None
                            st.session_state.editor_timestamp = 0.0
                            st.session_state.editor_file_ref = None
                        
                        # 3. 关闭上传框并刷新
                        st.session_state.active_upload_file = None
                        st.success(f"✅ {doc_file} 更新成功！")
                        time.sleep(1)
                        st.rerun()
                
                # 加个分割线让视觉更清晰
                st.divider()


# ==============================================================================
#  界面区域 B: 统一预览/编辑窗口
# ==============================================================================
if st.session_state.viewing_file:  # 如果有正在查看的文件
    curr_file = st.session_state.viewing_file  # 获取文件名
    curr_type = st.session_state.viewing_type  # 获取文件类型
    
    st.markdown("---")  # 分割线
    
    # 预览头
    c_head, c_close = st.columns([9, 1])
    with c_head:
        if curr_type == 'EXCEL':
            st.subheader(f"📝 正在编辑: {curr_file}")  # Excel 显示为编辑模式
        else:
            st.subheader(f"📖 正在预览: {curr_file}")  # 其他文件显示为预览模式
            
    with c_close:
        if st.button("❌ 关闭", type="primary"):  # 关闭按钮
            st.session_state.viewing_file = None
            st.rerun()  # 刷新页面

    # --- 多态渲染逻辑 ---
    try:
        # Case 1: Excel 编辑器 (新增行修复版)
        if curr_type == 'EXCEL':
            file_path = os.path.join(DOC_SOURCE_DIR, curr_file)
            
            # 1. 首次加载或重新加载逻辑
            if st.session_state.editor_data is None or st.session_state.editor_file_ref != curr_file:
                with st.spinner("正在加载最新数据并校验版本..."):
                    df = ExcelService.load_and_clean_data(file_path, sheet_name="Sheet1")
                    
                    status_mapping = {"Open": "🔴 Open", "Close": "🟢 Close", "Monitor": "🟡 Monitor"}
                    if "状态" in df.columns:
                        df["状态"] = df["状态"].replace(status_mapping)
                    
                    # ======================================================
                    # [新增] 注入临时辅助列 "移除"，用于控制删除
                    # 插入到第0列，默认值为 False
                    # ======================================================
                    if "移除" not in df.columns:
                        df.insert(0, "移除", False)
                    
                    ts = ExcelService.get_file_timestamp(file_path)
                    
                    st.session_state.editor_data = df
                    st.session_state.editor_timestamp = ts
                    st.session_state.editor_file_ref = curr_file

            # 2. 渲染区域
            if st.session_state.editor_data is not None:
                
                # 定义列配置 (保持不变)
                column_cfg = {
                    "移除": st.column_config.CheckboxColumn("🗑️", width="small", help="勾选后点击上方删除按钮"),
                    "No.": st.column_config.NumberColumn("序号", format="%d", width="small"),
                    "Issue描述": st.column_config.TextColumn("Issue 描述", width="large", required=True),
                    "原因分析": st.column_config.TextColumn("原因分析", width="large"),
                    "发生日期": st.column_config.DateColumn("发生日期", format="YYYY-MM-DD", width="medium"),
                    "状态": st.column_config.SelectboxColumn(
                        "当前状态", width="medium",
                        options=["🔴 Open", "🟢 Close", "🟡 Monitor", "⚪ Pending"],
                        required=True
                    ),
                }

                # ==========================================================
                # [核心修复] 开启大表单模式：所有按钮都放入 Form
                # ==========================================================
                with st.form(key=f"form_{curr_file}"):
                    
                    # --- A. 顶部功能按钮区 (都在 Form 内部) ---
                    col_btn_add, col_btn_del, col_hint = st.columns([1, 1, 8])
                    
                    with col_btn_add:
                        # [修改] 变为 form_submit_button
                        btn_add = st.form_submit_button("➕ New", type="secondary", use_container_width=True)
                    
                    with col_btn_del:
                        # [修改] 变为 form_submit_button
                        btn_del = st.form_submit_button("🗑️ Delete", type="secondary", use_container_width=True)
                        
                    with col_hint:
                        st.caption("💡 提示：勾选左侧复选框可删除。修改后请点击底部保存。")

                    # --- B. 编辑器区域 ---
                    # 此时 edited_df 会包含您刚刚勾选的状态，即使还没有点击保存
                    edited_df = st.data_editor(
                        st.session_state.editor_data,
                        height=600,
                        use_container_width=True,
                        hide_index=True,
                        column_config=column_cfg,
                        key=f"editor_{curr_file}"
                    )
                    
                    # --- C. 底部保存区 ---
                    col_save, col_space = st.columns([1, 5])
                    with col_save:
                        btn_save = st.form_submit_button("💾 Save", type="primary", use_container_width=True)

                # ==========================================================
                # [逻辑处理] 根据点击的按钮分发逻辑
                # ==========================================================
                
                # 1. 处理新增
                if btn_add:
                    # 使用 edited_df (最新的前端数据) 而不是 st.session_state.editor_data (旧数据)
                    # 这样可以保留您在新增前填写的未保存内容！
                    current_df = edited_df 
                    
                    # 智能序号
                    new_no = 1
                    if not current_df.empty and "No." in current_df.columns:
                        try:
                            max_val = pd.to_numeric(current_df["No."], errors='coerce').max()
                            if not pd.isna(max_val): new_no = int(max_val) + 1
                        except: pass
                    
                    new_row = {col: "" for col in current_df.columns}
                    new_row["No."] =str(new_no)
                    new_row["状态"] = "🔴 Open"
                    new_row["发生日期"] = pd.Timestamp.now().normalize().strftime('%Y-%m-%d')
                    new_row["Issue描述"] = "(请在此处填写描述)"
                    new_row["移除"] = "False"

                    st.session_state.editor_data = pd.concat(
                        [current_df, pd.DataFrame([new_row])], 
                        ignore_index=True
                    )
                    st.success(f"已追加新行 No.{new_no}")
                    st.rerun()

                # 2. 处理删除 (这是您最关心的修复)
                elif btn_del:
                    # edited_df 里包含了您刚刚勾选的 ✅
                    rows_to_delete = edited_df["移除"].sum()
                    
                    if rows_to_delete == 0:
                        st.warning("⚠️ 请先勾选表格第一列的复选框，再点击删除。")
                    else:
                        # 执行删除过滤
                        st.session_state.editor_data = edited_df[~edited_df["移除"]].reset_index(drop=True)
                        st.success(f"🗑️ 已移除 {rows_to_delete} 行。")
                        st.rerun()

                # 3. 处理保存
                elif btn_save:
                    logging.info(f"[Form] 用户提交保存: {curr_file}")
                    
                    # 剔除辅助列
                    df_to_save = edited_df.drop(columns=["移除"], errors='ignore')

                    with st.spinner("正在安全写入..."):
                        success, msg = ExcelService.save_data_with_lock(
                            file_path, 
                            df_to_save, 
                            st.session_state.editor_timestamp,
                            sheet_name="Sheet1"
                        )
                        
                        if success:
                            st.success(msg)
                            # 保存成功后，更新 Session
                            # 注意：这里我们得把“移除”列加回去，否则刷新后复选框会消失
                            df_saved_display = df_to_save.copy()
                            df_saved_display.insert(0, "移除", False)
                            
                            st.session_state.editor_data = df_saved_display
                            st.session_state.editor_timestamp = ExcelService.get_file_timestamp(file_path)
                            time.sleep(1) 
                            st.rerun() 
                        else:
                            st.error(f"⚠️ {msg}")
                            # 备份下载
                            import io
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                                df_to_save.to_excel(writer, index=False)
                            st.download_button("📥 下载备份", data=buffer, file_name=f"冲突备份_{curr_file}")
            else:
                st.warning("数据加载异常，请尝试刷新页面。")

        # Case 2: PDF / PPT 渲染 (读取缓存图片)
        elif curr_type in ['PDF', 'PPT']:
            # 重新实例化服务以获取图片列表
            service = PDFService(IMG_CACHE_DIR) if curr_type == 'PDF' else PPTService(IMG_CACHE_DIR)
            images = service.get_images()  # 获取转换后的图片列表
            
            if images:
                st.info(f"共加载 {len(images)} 页内容")
                for idx, img_path in enumerate(images):
                    abs_path = str(Path(PROJECT_ROOT) / img_path)  # 转换为绝对路径
                    st.image(abs_path, caption=f"Page {idx+1}", use_container_width=True)  # 渲染图片
            else:
                st.warning("缓存图片丢失，请重新点击“查看”按钮。")
                
    except Exception as e:
        st.error(f"预览渲染出错: {e}")  # 捕获并显示全局异常
        logging.error(f"Render error: {e}")