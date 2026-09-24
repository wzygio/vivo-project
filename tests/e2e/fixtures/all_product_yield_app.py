"""Production all-product UI with synthetic data; no database or workbook writes."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
for directory in (ROOT, ROOT / "src"):
    sys.path.insert(0, str(directory))

from app.sections.yield_domain import all_product_board as board

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
    periods = {
        "monthly": ["2026-07", "2026-08", "2026-09"],
        "weekly": ["2026-W37", "2026-W38", "2026-W39"],
        "daily": [f"2026-09-{day}" for day in range(18, 25)],
    }
    data = {
        period: pd.DataFrame([
            {"time_period": label, "defect_group": group,
             "defect_rate": rate + index * .0001,
             "total_panels": (len(labels) - index) * 1000}
            for index, label in enumerate(labels)
            for group, rate in (("Array_Line", .01), ("Array_Mura", .004), ("OLED_Mura", .011))
        ])
        for period, labels in periods.items()
    }
    return {
        "trends": None if mode == "empty" and product_code == "M678" else data,
        "data_health": {"status": mode if mode in ("stale", "unavailable") and product_code == "M678" else "fresh"},
    }


board.ConfigLoader.get_enabled_products = staticmethod(lambda: ["M678", "M626"])
board.load_product_group_trends = load
board.render_all_product_yield_board()
