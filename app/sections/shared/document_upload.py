"""Isolate upload choices; successful uploads deliberately refresh the document list."""
import logging
import time
from pathlib import Path
import streamlit as st

@st.fragment
def render_document_upload(document_sources: dict[str, Path]) -> None:
    with st.expander("📤 上传新台账/周报/解析报告/专项资料", expanded=False):
        upload_source = st.selectbox(
            "资料类别", options=["project", "analysis"],
            format_func=lambda source: "解析报告" if source == "analysis" else "台账/周报/其他归档",
            key="document_upload_source",
        )
        uploaded_files = st.file_uploader(
            "选择要上传的文件（支持批量上传）",
            type=['ppt', 'pptx', 'pdf'] if upload_source == "analysis" else ['xlsx', 'ppt', 'pptx', 'pdf', 'md'],
            accept_multiple_files=True,
            key=f"document_uploader_{upload_source}",
        )
        
        if uploaded_files:
            if st.button("🚀 确认上传并覆盖同名文件", type="primary", width="stretch"):
                uploaded_names = []
                for uf in uploaded_files:
                    if Path(uf.name).name != uf.name or uf.name in {".", ".."}:
                        st.error("文件名无效，请重命名后重新上传。")
                        continue
                    target_path = document_sources[upload_source] / uf.name
                    
                    # [核心逻辑] 先删后写，防文件损坏
                    if target_path.exists():
                        try:
                            target_path.unlink()
                        except Exception as e:
                            logging.exception("Unable to replace uploaded document")
                            st.error(f"❌ 无法覆盖旧文件 {uf.name}，可能正被占用，请稍后重试。")
                            continue
                    
                    try:
                        with open(target_path, "wb") as f:
                            f.write(uf.getbuffer())
                        uploaded_names.append(uf.name)
                    except OSError:
                        logging.exception("Unable to upload document")
                        st.error(f"❌ 无法上传文件 {uf.name}，请稍后重试。")
                
                # 覆盖当前来源的预览文件后关闭预览，避免继续展示旧内容。
                if (st.session_state.viewing_source == upload_source
                        and st.session_state.viewing_file in uploaded_names):
                    st.session_state.viewing_file = None
                    st.session_state.editor_data = None
                    st.session_state.editor_timestamp = 0.0
                    st.session_state.editor_file_ref = None

                if uploaded_names:
                    st.success(f"✅ {len(uploaded_names)} 个文件上传成功！")
                    # 有失败时保留错误提示，避免自动刷新将其清除。
                    if len(uploaded_names) == len(uploaded_files):
                        time.sleep(1)
                        st.rerun()
