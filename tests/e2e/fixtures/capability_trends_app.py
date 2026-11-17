"""Actual history page with synthetic capability summaries and forbidden DB I/O."""

from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
for directory in (ROOT, ROOT / "src"):
    sys.path.insert(0, str(directory))

from app.components import page_header
from app.manager.session_manager import SessionManager
from app.utils.app_setup import AppSetup
from src.inline_domain import composition
from src.inline_domain.application.monitor.cpk_monitor_service import CpkMonitorViewModel
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


def forbid_db(*args, **kwargs):
    raise AssertionError("Database access forbidden in capability trend fixture")


class Service:
    def __init__(self, metric):
        self.metric = metric

    def build_dashboard(self, *, products, factories):
        key = f"fixture_{self.metric}_calls"
        st.session_state[key] = st.session_state.get(key, 0) + 1
        st.caption(f"{self.metric.upper()} calls: {st.session_state[key]}")
        if st.session_state.fixture_scenario == "failure":
            raise OSError("PRIVATE_SOURCE_PATH admin=true flag")
        frame = {"指标": [f"{self.metric.upper()}总项目数", "达标项目数", "预警项目数", f"{self.metric.capitalize()}≥1.33达标率"]}
        for label, total in (("Y26", 100), ("Q4", 40), ("M9", 10), ("M10", 20), ("M11", 10), ("W44", 8), ("W45", 10), ("W46", 0), ("W47", 0)):
            if st.session_state.fixture_scenario == "empty":
                frame[label] = ["—", "—", "—", "—"]
            else:
                frame[label] = [total, int(total * .9), total - int(total * .9), f"{int(total * .9) / total if total else 1:.2%}"]
        summary = pd.DataFrame(frame)
        product_summaries = {}
        for product in products:
            own = summary.copy(deep=True)
            for label in own.columns[1:]:
                if st.session_state.fixture_scenario != "empty":
                    total = int(own.at[0, label] * (.4 if product == "M626" else .6))
                    passed = int(total * .9)
                    own[label] = [total, passed, total - passed, f"{passed / total if total else 1:.2%}"]
            product_summaries[product] = own
        return CpkMonitorViewModel(
            summary, pd.DataFrame([{"flag": "PRIVATE_DETAIL"}]),
            product_summary_dfs=product_summaries,
        )


AppSetup.initialize_app = staticmethod(lambda: None)
page_header.render_page_header = lambda **kwargs: st.title(kwargs["title"])
DatabaseManager.__init__ = forbid_db
ConfigLoader.get_enabled_products = staticmethod(lambda: ["M626", "M678"])
SessionManager.AVAILABLE_PRODUCTS = ["M626", "M678"]
composition.build_cpk_monitor_service = lambda: Service("cpk")
composition.build_cpm_monitor_service = lambda: Service("cpm")
composition.build_aoi_rs_monitor_service = lambda: SimpleNamespace(read_catalog=lambda *args: pd.DataFrame(columns=["step_id", "rs_code"]))

st.session_state.fixture_page_runs = st.session_state.get("fixture_page_runs", 0) + 1
st.caption(f"Page runs: {st.session_state.fixture_page_runs}")
st.selectbox("Fixture scenario", ["normal", "empty", "failure"], key="fixture_scenario")
st.button("Fixture full page reload")
runpy.run_path(str(ROOT / "app/pages/历史预警看板.py"), run_name="__main__")
