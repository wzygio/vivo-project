"""Exercise the production fragment and selector without database/workbook I/O."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
for directory in (ROOT, ROOT / "src"):
    sys.path.insert(0, str(directory))

from app.sections.yield_domain import code_analysis

st.set_page_config(layout="wide")
product = st.selectbox("Product", ["M678", "M626"])
st.session_state.setdefault("revision", 0)
if st.button("Refresh cache"):
    st.session_state.revision += 1
st.session_state["full_runs"] = st.session_state.get("full_runs", 0) + 1
runs = st.session_state.full_runs
revision = st.session_state.revision
st.text(f"Data loads: {runs}; Alert scans: {runs}")

frame = pd.DataFrame([
    {"defect_group": group, "defect_desc": code, "monthly_avg_rate": .01}
    for group, code in (
        [("Group-A", "Code-A"), ("Group-B", "Code-B")]
        if product == "M678" else [("Group-C", "Code-C")]
    )
])


def render_details(**kwargs):
    st.text(f"Detail source: {kwargs['product_code']}/{kwargs['mwd_code_data']['revision']}")
    st.text(f"Detail codes: {','.join(code for codes in kwargs['codes_by_group'].values() for code in codes)}")
    st.selectbox("Detail interaction", ["Lot-1", "Lot-2"])


code_analysis.render_code_compact_expanders = render_details
code_analysis.render_code_analysis_section(
    frame,
    warning_lines={},
    mwd_code_data={"revision": revision},
    lot_data={},
    sheet_data={},
    mapping_data=None,
    hotspot_scripts=[],
    product_code=product,
)
