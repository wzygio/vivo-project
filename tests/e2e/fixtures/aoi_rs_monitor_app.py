"""Actual warning page with isolated AOI_RS facts; database access is forbidden."""

import os
from pathlib import Path
import runpy
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from app.components import page_header
from app.manager.session_manager import SessionManager
from app.utils.app_setup import AppSetup
from src.inline_domain import composition
from src.inline_domain.application.monitor.aoi_rs_monitor_service import AoiRsMonitorService
from src.inline_domain.infrastructure.monitor.aoi_rs_period_store import AoiRsPeriodStore
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


class FixtureSource:
    def policy_signature(self, product):
        return st.session_state.get("fixture_rs_policy", "fixture-rs-v1")

    def history_signature(self, product):
        return self.policy_signature(product)

    def read_projected(self, product, factory, start, end):
        mode = st.session_state.get("fixture_rs_mode", "normal")
        if mode == "failure":
            raise RuntimeError("FIXTURE_PRIVATE_FAILURE")
        common = dict(prod_code=product, factory=factory, step_id="11629", lot_id="L1")
        now = pd.Timestamp.today().normalize()
        details = pd.DataFrame([dict(common, sheet_id="S1", rs_code="A1", code_qty=6,
                                     start_time=now - pd.Timedelta(days=1))])
        passed = pd.DataFrame([dict(common, sheet_id=sheet, start_time=now - pd.Timedelta(days=1))
                               for sheet in ("S1", "S2")])
        specs = pd.DataFrame([dict(common, rs_code=code, code_desc=f"Fixture {code}") for code in ("A1", "A2")])
        if mode == "empty":
            details, passed, specs = details.iloc[:0], passed.iloc[:0], specs.iloc[:0]
        return dict(rs_details_df=details, pass_through_df=passed, indicators_df=specs,
                    special_factories=(), decoration_window=None)


def forbid_db(*args, **kwargs):
    raise AssertionError("Database access forbidden in AOI_RS fixture")


def render_header(**kwargs):
    st.title(kwargs["title"])


AppSetup.initialize_app = staticmethod(lambda: None)
page_header.render_page_header = render_header
DatabaseManager.__init__ = forbid_db
ConfigLoader.get_enabled_products = classmethod(lambda cls: ["M626", "M678"])
SessionManager.AVAILABLE_PRODUCTS = ["M626", "M678"]
fixture_root = Path(os.environ.get("VIVO_RS_MONITOR_FIXTURE_ROOT", str(ROOT / "output/tmp/aoi-rs-monitor-e2e")))


def build_service():
    return AoiRsMonitorService(FixtureSource(), AoiRsPeriodStore(fixture_root / "history.xlsx"),
                               enabled_products=("M626", "M678"))


composition.build_aoi_rs_monitor_service = build_service
runpy.run_path(str(ROOT / "app/pages/超规预警看板.py"), run_name="__main__")

mode = st.selectbox("Fixture scenario", ["normal", "empty", "failure"], key="fixture_rs_mode")
st.caption(f"Fixture mode: {mode}")
