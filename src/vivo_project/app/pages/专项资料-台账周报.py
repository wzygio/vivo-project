import streamlit as st
import os
from pathlib import Path
import logging
import pandas as pd
import time

# --- 1. 基础配置与导入 ---
from vivo_project.utils.session_manager import SessionManager
from vivo_project.config import ConfigLoader
from vivo_project.app.components.components import render_page_header

# 引入所有 Service
from vivo_project.application.file_manager_service import FileManagerService
from vivo_project.application.excel_service import ExcelService
from vivo_project.application.ppt_service import PPTService
from vivo_project.application.pdf_service import PDFService

# 设置页面
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

# [Refactor] 1. 渲染侧边栏
SessionManager.render_product_selector_sidebar()

# [Refactor] 2. 获取上下文
active_config = SessionManager.get_active_config()
project_root = ConfigLoader.get_project_root()

# [Refactor] 3. 渲染页头 (注入 config)
render_page_header("📋 专项资料", active_config)

# --- 2. 路径定义 ---
# 定义相对路径
DOC_SOURCE_REL_DIR = "resources/project_files"
IMG_CACHE_REL_DIR = "data/doc_cache"

# 定义绝对路径 (用于文件系统操作)
ABS_DOC_SOURCE_DIR = project_root / DOC_SOURCE_REL_DIR
# IMG_CACHE_DIR 传给 Service 时一般传相对路径名称，Service 内部结合 project_root 使用

if not ABS_DOC_SOURCE_DIR.exists():
    ABS_DOC_SOURCE_DIR.mkdir(parents=True, exist_ok=True)

# --- 3. 状态管理初始化 ---
if 'viewing_file' not in st.session_state:
    st.session_state.viewing_file = None
if 'viewing_type' not in st.session_state:
    st.session_state.viewing_type = None

if 'editor_data' not in st.session_state:
    st.session_state.editor_data = None
if 'editor_timestamp' not in st.session_state:
    st.session_state.editor_timestamp = 0.0
if 'editor_file_ref' not in st.session_state:
    st.session_state.editor_file_ref = None

if 'active_upload_file' not in st.session_state:
    st.session_state.active_upload_file = None

# --- 4. 获取并分类文件 ---
# [Note] 假设 FileManagerService 支持传入绝对路径或处理逻辑与环境无关
# 为了安全，传入绝对路径字符串
classified_files = FileManagerService.get_classified_files(str(ABS_DOC_SOURCE_DIR))

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
            # 获取类型
            f_type = FileManagerService.get_file_type(doc_file)
            
            # 构建绝对路径用于下载/读取
            abs_file_path = ABS_DOC_SOURCE_DIR / doc_file
            
            if f_type == 'EXCEL':
                icon, mime = "📗", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif f_type == 'PDF':
                icon, mime = "📕", "application/pdf"
            else: 
                icon, mime = "📊", "application/vnd.openxmlformats-officedocument.presentationml.presentation"

            # 渲染单行卡片
            with st.container(border=True):
                c_name, c_dl, c_up, c_view = st.columns([7, 1, 1, 1])
                
                with c_name:
                    st.markdown(f"**{icon} {doc_file}**")
                
                with c_dl:
                    with open(abs_file_path, "rb") as f:
                        st.download_button("⬇️ 下载", f, file_name=doc_file, mime=mime, key=f"dl_{doc_file}", use_container_width=True)
                
                with c_up:
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
                            # [Refactor] 实例化 Service 并注入 project_root
                            service = PDFService(IMG_CACHE_REL_DIR, project_root) if f_type == 'PDF' else PPTService(IMG_CACHE_REL_DIR, project_root)
                            
                            # 相对路径字符串用于 Service 调用
                            rel_file_path_str = os.path.join(DOC_SOURCE_REL_DIR, doc_file)
                            
                            with st.spinner(f"正在启动 {f_type} 引擎解析..."):
                                success = service.convert_to_images(rel_file_path_str)
                            if success:
                                st.rerun()
                            else:
                                st.error("解析失败。")
                        else:
                            st.rerun()

            # 动态展开的上传区域
            if st.session_state.active_upload_file == doc_file:
                with st.container():
                    st.info(f"💡 建议先下载原文件，在原文件基础上进行修改，再进行上传。")

                    file_ext = os.path.splitext(doc_file)[1].replace(".", "")
                    uploaded_new_file = st.file_uploader(
                        f"选择新的 .{file_ext} 文件进行覆盖", 
                        type=[file_ext], 
                        key=f"uploader_{doc_file}",
                        label_visibility="collapsed"
                    )
                    
                    if uploaded_new_file is not None:
                        # 1. 执行覆盖 (使用绝对路径)
                        with open(abs_file_path, "wb") as f:
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
                st.divider()


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
        # Case 1: Excel 编辑器
        if curr_type == 'EXCEL':
            # 绝对路径
            abs_curr_file_path = ABS_DOC_SOURCE_DIR / curr_file
            
            if st.session_state.editor_data is None or st.session_state.editor_file_ref != curr_file:
                with st.spinner("正在加载最新数据并校验版本..."):
                    df = ExcelService.load_and_clean_data(str(abs_curr_file_path), sheet_name="Sheet1")
                    
                    status_mapping = {"Open": "🔴 Open", "Close": "🟢 Close", "Monitor": "🟡 Monitor"}
                    if "状态" in df.columns:
                        df["状态"] = df["状态"].replace(status_mapping)
                    
                    if "移除" not in df.columns:
                        df.insert(0, "移除", False)
                    
                    ts = ExcelService.get_file_timestamp(str(abs_curr_file_path))
                    
                    st.session_state.editor_data = df
                    st.session_state.editor_timestamp = ts
                    st.session_state.editor_file_ref = curr_file

            if st.session_state.editor_data is not None:
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

                with st.form(key=f"form_{curr_file}"):
                    col_btn_add, col_btn_del, col_hint = st.columns([1, 1, 8])
                    
                    with col_btn_add:
                        btn_add = st.form_submit_button("➕ New", type="secondary", use_container_width=True)
                    with col_btn_del:
                        btn_del = st.form_submit_button("🗑️ Delete", type="secondary", use_container_width=True)
                    with col_hint:
                        st.caption("💡 提示：勾选左侧复选框可删除。修改后请点击底部保存。")

                    edited_df = st.data_editor(
                        st.session_state.editor_data,
                        height=600,
                        use_container_width=True,
                        hide_index=True,
                        column_config=column_cfg,
                        key=f"editor_{curr_file}"
                    )
                    
                    col_save, col_space = st.columns([1, 5])
                    with col_save:
                        btn_save = st.form_submit_button("💾 Save", type="primary", use_container_width=True)

                if btn_add:
                    current_df = edited_df 
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

                elif btn_del:
                    rows_to_delete = edited_df["移除"].sum()
                    if rows_to_delete == 0:
                        st.warning("⚠️ 请先勾选表格第一列的复选框，再点击删除。")
                    else:
                        st.session_state.editor_data = edited_df[~edited_df["移除"]].reset_index(drop=True)
                        st.success(f"🗑️ 已移除 {rows_to_delete} 行。")
                        st.rerun()

                elif btn_save:
                    logging.info(f"[Form] 用户提交保存: {curr_file}")
                    df_to_save = edited_df.drop(columns=["移除"], errors='ignore')

                    with st.spinner("正在安全写入..."):
                        success, msg = ExcelService.save_data_with_lock(
                            str(abs_curr_file_path), 
                            df_to_save, 
                            st.session_state.editor_timestamp,
                            sheet_name="Sheet1"
                        )
                        
                        if success:
                            st.success(msg)
                            df_saved_display = df_to_save.copy()
                            df_saved_display.insert(0, "移除", False)
                            st.session_state.editor_data = df_saved_display
                            st.session_state.editor_timestamp = ExcelService.get_file_timestamp(str(abs_curr_file_path))
                            time.sleep(1) 
                            st.rerun() 
                        else:
                            st.error(f"⚠️ {msg}")
                            import io
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                                df_to_save.to_excel(writer, index=False)
                            st.download_button("📥 下载备份", data=buffer, file_name=f"冲突备份_{curr_file}")
            else:
                st.warning("数据加载异常，请尝试刷新页面。")

        # Case 2: PDF / PPT 渲染
        elif curr_type in ['PDF', 'PPT']:
            # [Refactor] 实例化 Service 注入 project_root
            service = PDFService(IMG_CACHE_REL_DIR, project_root) if curr_type == 'PDF' else PPTService(IMG_CACHE_REL_DIR, project_root)
            images = service.get_images()
            
            if images:
                st.info(f"共加载 {len(images)} 页内容")
                for idx, img_path in enumerate(images):
                    # service.get_images() 返回绝对路径字符串 (在 Service 内部已处理为 absolute glob)
                    # 直接显示即可
                    st.image(img_path, caption=f"Page {idx+1}", use_container_width=True)
            else:
                st.warning("缓存图片丢失，请重新点击“查看”按钮。")
                
    except Exception as e:
        st.error(f"预览渲染出错: {e}")
        logging.error(f"Render error: {e}", exc_info=True)