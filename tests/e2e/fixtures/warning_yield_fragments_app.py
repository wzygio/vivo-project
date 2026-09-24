"""Real warning page with a slow synthetic detail loader and no external I/O."""

import runpy
import sys
import time
from pathlib import Path
from uuid import uuid4

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
for directory in (ROOT, ROOT / "src"):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

from app.manager import compliance_manager
from app.manager.session_manager import SessionManager
from app.sections.inline_domain.monitor import alert_matrix, alert_matrix_detail, alert_matrix_snapshot
from app.sections.yield_domain import all_product_board
from app.utils import step_labels
from app.utils.app_setup import AppSetup
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure import db_handler


def initialize():
    st.session_state["fixture_full_runs"] = st.session_state.get("fixture_full_runs", 0) + 1
    st.session_state.setdefault("fixture_nonce", uuid4().hex)
    st.caption(f"Full runs: {st.session_state.fixture_full_runs}")


def matrix_payload():
    st.session_state["fixture_matrix_runs"] = st.session_state.get("fixture_matrix_runs", 0) + 1
    st.caption(f"Matrix runs: {st.session_state.fixture_matrix_runs}")
    return {
        "products": ["M678"],
        "rows": [{"row_key": "spc_cpk_trend", "display_name": "SPC 趋势波动（CPK）",
                  "module_group": "spc", "time_scope": "上一 ISO 周"}],
        "cells": {("spc_cpk_trend", "M678"): {
            "state": "alert", "detail_key": "spc_cpk_trend|M678",
            "cache_signature": st.session_state.fixture_nonce,
        }},
        "reference_week": {"label": "2026-W38", "start": "2026-09-14", "end": "2026-09-21"},
    }


def slow_detail(product, reference_date):
    st.caption("Detail loading")
    time.sleep(4)
    return {"kind": "spc_cpk", "alerts_df": pd.DataFrame(), "frames": {}}


def yield_data(product):
    st.session_state["fixture_yield_loads"] = st.session_state.get("fixture_yield_loads", 0) + 1
    st.caption(f"Yield loads: {st.session_state.fixture_yield_loads}")
    frame = pd.DataFrame({"time_period": ["2026-09"], "defect_group": ["Array_Line"],
                          "defect_rate": [.01], "total_panels": [1000]})
    return {"data_health": {"status": "fresh"},
            "trends": {period: frame for period in ("monthly", "weekly", "daily")}}


# This fixture runs in its own Streamlit process; patches must also persist for
# subsequent fragment reruns, when the fixture module itself does not execute.
AppSetup.initialize_app = staticmethod(initialize)
SessionManager.AVAILABLE_PRODUCTS = ["M678"]
ConfigLoader.get_enabled_products = staticmethod(lambda: ["M678"])
db_handler.DatabaseManager = lambda: None
step_labels.get_cached_step_description_map = lambda db: {}
alert_matrix_snapshot.has_daily_matrix_snapshot = lambda: False
alert_matrix.get_cached_alert_matrix = matrix_payload
compliance_manager.load_matrix_config = lambda: None
compliance_manager.apply_matrix_compliance = lambda payload, config: payload
alert_matrix_detail._spc_detail_signature = lambda product, signature: signature
alert_matrix_detail.build_default_detail_loaders = lambda db=None: {"spc_cpk_trend": slow_detail}
all_product_board.load_product_group_trends = yield_data

runpy.run_path(str(ROOT / "app/pages/自动预警看板.py"), run_name="__main__")
