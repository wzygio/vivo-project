# src/vivo_project/app/pages/专项资料-台账周报.py
import streamlit as st
import os
from pathlib import Path
import logging
import pandas as pd
import time

# --- 1. 基础配置与导入 ---
from app.utils.session_manager import SessionManager
from src.shared_kernel.config import ConfigLoader
from app.components.components import render_page_header
from yield_domain.application.file_manager_service import FileManagerService
from yield_domain.application.excel_service import ExcelService
from yield_domain.application.ppt_service import PPTService
from yield_domain.application.pdf_service import PDFService

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

# --- 2. 获取动态上下文与路径 ---
active_config = SessionManager.get_active_config()
project_root = ConfigLoader.get_project_root()
resource_dir = SessionManager.get_resource_dir() # 动态获取当前产品目录

# 渲染页头
render_page_header("📋 专项资料", active_config)

# 动态构建绝对路径 (取代全局变量)
doc_source_dir = resource_dir / "project_files"
doc_source_dir.mkdir(parents=True, exist_ok=True)
img_cache_rel_dir = "data/doc_cache"

# --- 3. 状态管理初始化 (已清理掉不需要的 active_upload_file 状态) ---
if 'viewing_file' not in st.session_state: st.session_state.viewing_file = None
if 'viewing_type' not in st.session_state: st.session_state.viewing_type = None
if 'editor_data' not in st.session_state: st.session_state.editor_data = None
if 'editor_timestamp' not in st.session_state: st.session_state.editor_timestamp = 0.0
if 'editor_file_ref' not in st.session_state: st.session_state.editor_file_ref = None

# [防护机制]：如果切换了产品导致正在预览的文件不存在，则清空状态
if st.session_state.viewing_file and not (doc_source_dir / st.session_state.viewing_file).exists():
    st.session_state.viewing_file = None
    st.session_state.editor_data = None
    st.session_state.editor_file_ref = None

# --- 4. 获取并分类文件 ---
classified_files = FileManagerService.get_classified_files(str(doc_source_dir))

category_map = [
    ("📂 北极星台账", classified_files['ledger'], True),
    ("📅 北极星周报", classified_files['weekly'], False),
    ("🗃️ 其他归档",     classified_files['others'], False)
]

# ==============================================================================
#  界面区域 A: 统一上传接口
# ==============================================================================
st.caption(f"浏览、下载或在线预览当前产品 ({active_config.data_source.product_code}) 的各类分析报告与台账。")

with st.expander("📤 上传新台账/周报/专项资料", expanded=False):
    uploaded_files = st.file_uploader(
        "选择要上传的文件 (支持 xlsx, ppt, pptx, pdf，支持批量上传)", 
        type=['xlsx', 'ppt', 'pptx', 'pdf'], 
        accept_multiple_files=True,
        key="project_file_uploader_top"
    )
    
    if uploaded_files:
        if st.button("🚀 确认上传并覆盖同名文件", type="primary", use_container_width=True):
            for uf in uploaded_files:
                target_path = doc_source_dir / uf.name
                
                # [核心逻辑] 先删后写，防文件损坏
                if target_path.exists():
                    try:
                        target_path.unlink()
                    except Exception as e:
                        st.error(f"❌ 无法覆盖旧文件 {uf.name}，可能正被占用: {e}")
                        continue
                
                with open(target_path, "wb") as f:
                    f.write(uf.getbuffer())
            
            # 如果上传的文件覆盖了当前正在预览的 Excel，清空它的内存缓存，让它重新拉取
            if st.session_state.viewing_file in [f.name for f in uploaded_files]:
                st.session_state.editor_data = None
                st.session_state.editor_timestamp = 0.0
                st.session_state.editor_file_ref = None

            st.success(f"✅ {len(uploaded_files)} 个文件上传成功！")
            time.sleep(1)
            st.rerun()

st.divider()

# ==============================================================================
#  界面区域 B: 分类折叠列表 (已移除每行单文件的上传按钮)
# ==============================================================================
for title, files, default_expanded in category_map:
    if not files: continue 
    
    with st.expander(f"{title} ({len(files)})", expanded=default_expanded):
        for doc_file in files:
            f_type = FileManagerService.get_file_type(doc_file)
            abs_file_path = doc_source_dir / doc_file
            
            if f_type == 'EXCEL':
                icon, mime = "📗", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif f_type == 'PDF':
                icon, mime = "📕", "application/pdf"
            else: 
                icon, mime = "📊", "application/vnd.openxmlformats-officedocument.presentationml.presentation"

            with st.container(border=True):
                # 重新分配列宽，去掉了上传按钮的占位
                c_name, c_dl, c_view = st.columns([7, 1.5, 1.5])
                
                with c_name:
                    st.markdown(f"**{icon} {doc_file}**")
                
                with c_dl:
                    with open(abs_file_path, "rb") as f:
                        st.download_button("⬇️ 下载", f, file_name=doc_file, mime=mime, key=f"dl_{doc_file}", use_container_width=True)

                with c_view:
                    if st.button("👁️ 查看", key=f"view_{doc_file}", use_container_width=True):
                        st.session_state.viewing_file = doc_file
                        st.session_state.viewing_type = f_type
                        st.session_state.editor_data = None
                        st.session_state.editor_timestamp = 0.0
                        st.session_state.editor_file_ref = None
                        
                        if f_type in ['PDF', 'PPT']:
                            service = PDFService(img_cache_rel_dir, project_root) if f_type == 'PDF' else PPTService(img_cache_rel_dir, project_root)
                            # 动态计算相对路径
                            rel_file_path_str = str(doc_source_dir.relative_to(project_root) / doc_file)
                            
                            with st.spinner(f"正在启动 {f_type} 引擎解析..."):
                                success = service.convert_to_images(rel_file_path_str)
                            if success:
                                st.rerun()
                            else:
                                st.error("解析失败。")
                        else:
                            st.rerun()

# ==============================================================================
#  界面区域 C: 统一预览/编辑窗口
# ==============================================================================
if st.session_state.viewing_file:
    curr_file = st.session_state.viewing_file
    curr_type = st.session_state.viewing_type
    
    st.markdown("---")
    c_head, c_close = st.columns([9, 1])
    with c_head:
        st.subheader(f"📝 正在编辑: {curr_file}" if curr_type == 'EXCEL' else f"📖 正在预览: {curr_file}")
    with c_close:
        if st.button("❌ 关闭", type="primary"):
            st.session_state.viewing_file = None
            st.rerun()

    try:
        if curr_type == 'EXCEL':
            abs_curr_file_path = doc_source_dir / curr_file
            
            if st.session_state.editor_data is None or st.session_state.editor_file_ref != curr_file:
                with st.spinner("正在加载最新数据并校验版本..."):
                    df = ExcelService.load_and_clean_data(str(abs_curr_file_path), sheet_name="Sheet1")
                    status_mapping = {"Open": "🔴 Open", "Close": "🟢 Close", "Monitor": "🟡 Monitor"}
                    if "状态" in df.columns:
                        df["状态"] = df["状态"].replace(status_mapping)
                    if "移除" not in df.columns:
                        df.insert(0, "移除", False)
                    
                    st.session_state.editor_data = df
                    st.session_state.editor_timestamp = ExcelService.get_file_timestamp(str(abs_curr_file_path))
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
                        options=["🔴 Open", "🟢 Close", "🟡 Monitor", "⚪ Pending"], required=True
                    ),
                }

                with st.form(key=f"form_{curr_file}"):
                    col_btn_add, col_btn_del, col_hint = st.columns([1, 1, 8])
                    with col_btn_add: btn_add = st.form_submit_button("➕ New", type="secondary", use_container_width=True)
                    with col_btn_del: btn_del = st.form_submit_button("🗑️ Delete", type="secondary", use_container_width=True)
                    with col_hint: st.caption("💡 提示：勾选左侧复选框可删除。修改后请点击底部保存。")

                    edited_df = st.data_editor(
                        st.session_state.editor_data, height=600, use_container_width=True,
                        hide_index=True, column_config=column_cfg, key=f"editor_{curr_file}"
                    )
                    
                    col_save, _ = st.columns([1, 5])
                    with col_save: btn_save = st.form_submit_button("💾 Save", type="primary", use_container_width=True)

                if btn_add:
                    current_df = edited_df 
                    new_no = 1
                    if not current_df.empty and "No." in current_df.columns:
                        try:
                            max_val = pd.to_numeric(current_df["No."], errors='coerce').max()
                            if not pd.isna(max_val): new_no = int(max_val) + 1
                        except: pass
                    
                    new_row = {col: "" for col in current_df.columns}
                    new_row["No."] = str(new_no)
                    new_row["状态"] = "🔴 Open"
                    new_row["发生日期"] = pd.Timestamp.now().normalize().strftime('%Y-%m-%d')
                    new_row["Issue描述"] = "(请在此处填写描述)"
                    new_row["移除"] = "False"

                    st.session_state.editor_data = pd.concat([current_df, pd.DataFrame([new_row])], ignore_index=True)
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
                            str(abs_curr_file_path), df_to_save, 
                            st.session_state.editor_timestamp, sheet_name="Sheet1"
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

        elif curr_type in ['PDF', 'PPT']:
            service = PDFService(img_cache_rel_dir, project_root) if curr_type == 'PDF' else PPTService(img_cache_rel_dir, project_root)
            images = service.get_images()
            
            if images:
                st.info(f"共加载 {len(images)} 页内容")
                for idx, img_path in enumerate(images):
                    st.image(str(img_path), caption=f"Page {idx+1}", use_container_width=True)
            else:
                st.warning("缓存图片丢失，请重新点击“查看”按钮。")
                
    except Exception as e:
        st.error(f"预览渲染出错: {e}")
        logging.error(f"Render error: {e}", exc_info=True)