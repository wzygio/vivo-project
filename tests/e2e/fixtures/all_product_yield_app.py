"""Production all-product UI with synthetic data; no database or workbook writes."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
for directory in (ROOT, ROOT / "src"):
    sys.path.insert(0, str(directory))

from app.sections.yield_domain import all_product_board as board
sys.path.insert(0, str(Path(__file__).parent))
from period_trend_payload import period_payload

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
st.session_state["full_runs"] = st.session_state.get("full_runs", 0) + 1
st.subheader("全指标预警看板")
st.text(f"Full runs: {st.session_state.full_runs}")
mode = st.selectbox("Scenario", ["normal", "empty", "failed", "stale", "unavailable"])


def load(product_code, **kwargs):
    key = f"loads_{product_code}"
    st.session_state[key] = st.session_state.get(key, 0) + 1
    st.text(f"{product_code} loads: {st.session_state[key]}")
    if mode == "failed" and product_code == "M678":
        raise OSError("PRIVATE_CONFIG_PATH admin=true")
    if mode == 'unavailable' and product_code == 'M678':
        raise OSError('PRIVATE_CONFIG_PATH')
    return period_payload(mode if product_code == 'M678' else 'normal')


board.ConfigLoader.get_enabled_products = staticmethod(lambda: ["M678", "M626"])
board.load_product_period_trends = load
board.render_all_product_yield_board()
