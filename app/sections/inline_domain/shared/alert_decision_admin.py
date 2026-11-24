"""Maintain the selected product's Inline warning decisions in its report."""
import logging

import streamlit as st

from app.sections.inline_domain.shared.decoration_admin import excel_bytes
from app.utils.admin_mode import is_admin_mode
from src.inline_domain.composition import build_alert_decision_workbook
from src.shared_kernel.core.decoration_revision import VERSION_SHEET, upload_revision
from src.shared_kernel.utils.excel_tools import read_uploaded_excel_sheet


def render_inline_alert_decisions(scope: str, product: str) -> None:
    if not (is_admin_mode() or scope in ("spc", "ctq") and "admin-true" in st.query_params):
        return
    kinds = ["oos", "ooc", "trend"] if scope.startswith("aoi_") else ["oos", "ooc"]
    if scope == "spc":
        kinds.extend(["cpk", "cpm"])
    with st.expander("预警中心 · 台账维护"):
        kind = st.selectbox("预警类型", kinds, format_func=lambda key: {
            "oos": "OOS", "ooc": "OOC", "trend": "密度趋势", "cpk": "CPK", "cpm": "CPM",
        }[key], key=f"{scope}_ledger_type")
        prefix = f"{scope}_{product}_{kind}_ledger"
        try:
            if kind in ("cpk", "cpm"):
                from app.sections.inline_domain.spc.spc_dashboard import render_capability_decoration_admin
                from src.inline_domain.composition import read_capability_admin_ledger
                render_capability_decoration_admin(read_capability_admin_ledger(product, kind),
                    metric_label=kind.upper(), key_prefix=prefix, show_expander=False)
                return
            store = build_alert_decision_workbook(scope, kind)
            payload = store.download(product)
        except Exception:
            logging.getLogger(__name__).exception("Failed to read Inline decisions")
            st.error("预警台账暂时无法读取。")
            return
        st.caption("编辑“决策台账”中的 flag。True 启用修饰，False 放行原值，Delete 从公开结果中隐藏。")
        st.download_button("下载预警台账", excel_bytes(payload), file_name=f"{product}_{kind}.xlsx", key=f"{prefix}_download")
        uploaded = st.file_uploader("上传预警台账", type=["xlsx"], key=f"{prefix}_upload")
        if uploaded is not None and st.button("确认保存预警决策", key=f"{prefix}_apply"):
            try:
                version = upload_revision(read_uploaded_excel_sheet(uploaded.getbuffer(), sheet_name=VERSION_SHEET))
                changes = read_uploaded_excel_sheet(uploaded.getbuffer(), sheet_name="决策台账")
                store.update(product, changes, revision=version)
            except ValueError as exc:
                st.error(str(exc))
            except Exception:
                logging.getLogger(__name__).exception("Failed to save Inline decisions")
                st.error("预警台账保存失败。")
            else:
                st.rerun()
