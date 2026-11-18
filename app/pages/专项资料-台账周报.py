# src/vivo_project/app/pages/专项资料-台账周报.py
import streamlit as st
import logging
import pandas as pd
import time
from hashlib import sha256

# --- 1. 基础配置与导入 ---
from app.components.page_header import perform_hard_reset
from app.sections.shared.document_upload import render_document_upload
from src.shared_kernel.config import ConfigLoader
from yield_domain.application.file_manager_service import FileManagerService
from yield_domain.application.excel_service import ExcelService
from yield_domain.application.ppt_service import PPTService
from yield_domain.application.pdf_service import PDFService

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

# --- 2. 获取动态上下文与路径 ---
project_root = ConfigLoader.get_project_root()

document_categories = (
    ("ledger", "project_ledgers", "📂", "北极星台账"),
    ("weekly", "project_weekly_reports", "📅", "北极星周报"),
    ("analysis", "project_analysis_reports", "📊", "解析报告"),
    ("archive", "project_archives", "🗃️", "其他归档"),
)
document_sources = {
    source: ConfigLoader.get_domain_resource_directory("shared_kernel", directory_key)
    for source, directory_key, _, _ in document_categories
}
for directory in document_sources.values():
    directory.mkdir(parents=True, exist_ok=True)
img_cache_rel_dir = "output/screenshots/document-preview"


def get_preview_cache_dir(source: str, filename: str) -> str:
    """隔离不同目录中的同名文件预览。"""
    identity = str(document_sources[source] / filename)
    digest = sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"{img_cache_rel_dir}/{digest}"


# --- 3. 状态管理初始化 (已清理掉不需要的 active_upload_file 状态) ---
if 'viewing_file' not in st.session_state: st.session_state.viewing_file = None
if 'viewing_source' not in st.session_state: st.session_state.viewing_source = "ledger"
if 'viewing_type' not in st.session_state: st.session_state.viewing_type = None
if 'editor_data' not in st.session_state: st.session_state.editor_data = None
if 'editor_timestamp' not in st.session_state: st.session_state.editor_timestamp = 0.0
if 'editor_file_ref' not in st.session_state: st.session_state.editor_file_ref = None

# [防护机制]：如果正在预览的文件不存在，则清空状态
viewing_directory = document_sources.get(st.session_state.viewing_source)
viewing_file = st.session_state.viewing_file
if viewing_directory is None or (viewing_file and not (viewing_directory / viewing_file).is_file()):
    st.session_state.viewing_source = "ledger"
    st.session_state.viewing_file = None
    st.session_state.editor_data = None
    st.session_state.editor_timestamp = 0.0
    st.session_state.editor_file_ref = None

# ==============================================================================
#  界面区域 A: 统一上传接口
# ==============================================================================
# 管理员硬重置入口：与页头「刷新缓存」同一套逻辑，仅 ?admin=true 可见
if st.query_params.get("admin") == "true":
    st.button(
        "🔄 刷新缓存",
        key="btn_clear_专项资料台账周报",
        on_click=perform_hard_reset,
        help="清除当前报表缓存并重载代码与配置；普通浏览器刷新不会触发",
    )

st.caption("浏览、下载或在线预览各产品的分析报告与台账。")

render_document_upload(
    document_sources, {source: title for source, _, _, title in document_categories},
)

st.divider()

# ==============================================================================
#  界面区域 B: 分类折叠列表 (已移除每行单文件的上传按钮)
# ==============================================================================
for source, _, category_icon, title in document_categories:
    allowed_types = {"PPT", "PDF"} if source == "analysis" else {"EXCEL", "PPT", "PDF", "MARKDOWN"}
    files = sorted(
        (path.name for path in document_sources[source].iterdir()
         if path.is_file() and not path.name.startswith("~$")
         and FileManagerService.get_file_type(path.name) in allowed_types),
        reverse=True,
    )
    if not files and source != "analysis":
        continue

    with st.expander(f"{category_icon} {title} ({len(files)})", expanded=source == "ledger"):
        if not files:
            st.info("暂无解析报告，请在上方上传文件。")
        for doc_file in files:
            f_type = FileManagerService.get_file_type(doc_file)
            abs_file_path = document_sources[source] / doc_file
            
            if f_type == 'EXCEL':
                icon, mime = "📗", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif f_type == 'PDF':
                icon, mime = "📕", "application/pdf"
            elif f_type == 'MARKDOWN':
                icon, mime = "📄", "text/markdown"
            else: 
                icon, mime = "📊", "application/vnd.openxmlformats-officedocument.presentationml.presentation"

            with st.container(border=True):
                # 重新分配列宽，去掉了上传按钮的占位
                c_name, c_dl, c_view = st.columns([7, 1.5, 1.5])
                
                with c_name:
                    st.markdown(f"**{icon} {doc_file}**")
                
                with c_dl:
                    with open(abs_file_path, "rb") as f:
                        st.download_button("⬇️ 下载", f, file_name=doc_file, mime=mime, key=f"dl_{source}_{doc_file}", width="stretch", on_click="ignore")

                with c_view:
                    if st.button("👁️ 查看", key=f"view_{source}_{doc_file}", width="stretch"):
                        st.session_state.viewing_file = doc_file
                        st.session_state.viewing_source = source
                        st.session_state.viewing_type = f_type
                        st.session_state.editor_data = None
                        st.session_state.editor_timestamp = 0.0
                        st.session_state.editor_file_ref = None
                        
                        if f_type in ['PDF', 'PPT']:
                            preview_cache_dir = get_preview_cache_dir(source, doc_file)
                            service = PDFService(preview_cache_dir, project_root) if f_type == 'PDF' else PPTService(preview_cache_dir, project_root)
                            # 动态计算相对路径
                            rel_file_path_str = str(abs_file_path)
                            
                            with st.spinner(f"正在启动 {f_type} 引擎解析..."):
                                success = service.convert_to_images(rel_file_path_str)
                            if success:
                                st.rerun()
                            else:
                                st.session_state.viewing_file = None
                                st.error("解析失败，请确认文件可正常打开后重试。")
                        else:
                            st.rerun()

# ==============================================================================
#  界面区域 C: 统一预览/编辑窗口
# ==============================================================================
if st.session_state.viewing_file:
    curr_file = st.session_state.viewing_file
    curr_type = st.session_state.viewing_type
    curr_source = st.session_state.viewing_source
    current_source_dir = document_sources[curr_source]
    
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
            abs_curr_file_path = current_source_dir / curr_file
            editor_file_ref = str(abs_curr_file_path)
            
            if st.session_state.editor_data is None or st.session_state.editor_file_ref != editor_file_ref:
                with st.spinner("正在加载最新数据并校验版本..."):
                    df = ExcelService.load_and_clean_data(str(abs_curr_file_path), sheet_name="Sheet1")
                    status_mapping = {"Open": "🔴 Open", "Close": "🟢 Close", "Monitor": "🟡 Monitor"}
                    if "状态" in df.columns:
                        df["状态"] = df["状态"].replace(status_mapping)
                    if "移除" not in df.columns:
                        df.insert(0, "移除", False)
                    
                    st.session_state.editor_data = df
                    st.session_state.editor_timestamp = ExcelService.get_file_timestamp(str(abs_curr_file_path))
                    st.session_state.editor_file_ref = editor_file_ref

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

                with st.form(key=f"form_{curr_source}_{curr_file}"):
                    col_btn_add, col_btn_del, col_hint = st.columns([1, 1, 8])
                    with col_btn_add: btn_add = st.form_submit_button("➕ New", type="secondary", width="stretch")
                    with col_btn_del: btn_del = st.form_submit_button("🗑️ Delete", type="secondary", width="stretch")
                    with col_hint: st.caption("💡 提示：勾选左侧复选框可删除。修改后请点击底部保存。")

                    edited_df = st.data_editor(
                        st.session_state.editor_data, height=600, width="stretch",
                        hide_index=True, column_config=column_cfg, key=f"editor_{curr_source}_{curr_file}"
                    )
                    
                    col_save, _ = st.columns([1, 5])
                    with col_save: btn_save = st.form_submit_button("💾 Save", type="primary", width="stretch")

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
                            logging.error("Document save failed: %s", msg)
                            st.error("保存失败，文件可能已更新或正在被占用。请刷新后重试，或下载备份。")
                            import io
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                                df_to_save.to_excel(writer, index=False)
                            st.download_button("📥 下载备份", data=buffer, file_name=f"冲突备份_{curr_file}", on_click="ignore")
            else:
                st.warning("数据加载异常，请尝试刷新页面。")

        elif curr_type in ['PDF', 'PPT']:
            preview_cache_dir = get_preview_cache_dir(curr_source, curr_file)
            service = PDFService(preview_cache_dir, project_root) if curr_type == 'PDF' else PPTService(preview_cache_dir, project_root)
            images = service.get_images()

            if images:
                st.info(f"共加载 {len(images)} 页内容")
                for idx, img_path in enumerate(images):
                    st.image(str(img_path), caption=f"Page {idx+1}", width="stretch")
            else:
                st.warning("缓存图片丢失，请重新点击“查看”按钮。")

        elif curr_type == 'MARKDOWN':
            abs_curr_file_path = current_source_dir / curr_file
            md_content = abs_curr_file_path.read_text(encoding='utf-8')
            st.markdown(md_content)
                
    except Exception as e:
        st.error("预览暂时不可用，请稍后重试。")
        logging.error(f"Render error: {e}", exc_info=True)
