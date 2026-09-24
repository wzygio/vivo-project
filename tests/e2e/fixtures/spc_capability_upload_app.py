"""Exercise the SPC upload controls against a scratch copy, without database I/O."""

import hashlib
import shutil
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from app.sections.inline_domain.spc.spc_dashboard import render_capability_decoration_admin
from src.inline_domain.application.spc.capability_decoration_service import CpkDecorationResult
from src.shared_kernel.utils.excel_tools import read_workbook_sheet

st.set_page_config(page_title="SPC upload fixture", layout="wide")
if st.query_params.get("admin") != "true":
    st.info("当前产品暂无可展示的 SPC 数据。")
    st.stop()

target = ROOT / "output" / "test-results" / "spc-config-upload" / "decoration.xlsx"
if not target.exists():
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / "resources/inline_domain/spc/spc_cpk_cpm_decoration.xlsx", target)

st.caption(f"Saved workbook: {hashlib.sha256(target.read_bytes()).hexdigest()}")
render_capability_decoration_admin(
    CpkDecorationResult(
        period_capability_df=pd.DataFrame(),
        decoration_df=read_workbook_sheet(target, "M626"),
        decoration_path=target,
        decoration_sheet="M626",
    ),
    show_expander=False,
)
