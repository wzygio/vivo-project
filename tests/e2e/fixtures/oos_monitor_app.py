"""Controlled raw-input harness executing the real warning page and workbook stores."""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
for search_path in (PROJECT_ROOT, PROJECT_ROOT / "src"):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))

from app.components import page_header
from app.manager.session_manager import SessionManager
from app.sections.inline_domain.monitor import alert_matrix, alert_matrix_cache
from app.utils import app_setup, reloader, step_labels
from src.inline_domain.application.monitor.live_source import LiveMonitorSource, LiveThroughputReader
from src.inline_domain.application.monitor.live_history import LiveHistoryStore
from src.inline_domain.application.monitor.cpk_monitor_service import CpkMonitorService
from src.inline_domain.application.shared.oos_history_service import OosHistoryService
from src.inline_domain.application.shared.ooc_history_service import OocHistoryService
from src.inline_domain.infrastructure.shared.oos_decision_repository import OosDecisionWorkbookRepository
from src.inline_domain.application.monitor.summary_workbook_service import MonitorSummaryWorkbookService
from src.inline_domain.core.monitor.period_summary import MONITOR_SUMMARY_COLUMNS
from src.inline_domain.core.monitor.cpk_summary import CPK_SUMMARY_COLUMNS
from src.inline_domain.infrastructure.monitor.summary_workbook_store import MonitorSummaryWorkbookStore
from src.inline_domain.infrastructure.monitor.cpk_summary_workbook_store import CpkSummaryWorkbookStore
import src.inline_domain.composition as composition
import src.shared_kernel.infrastructure.db_handler as db_handler

fixture_root = PROJECT_ROOT / "output" / "tmp" / "monitor-live-production-e2e"
resources = fixture_root / "resources"
resources.mkdir(parents=True, exist_ok=True)
summary_workbook_path = fixture_root / "monitor-summary.xlsx"

if not st.session_state.get("fixture_initialized"):
    with pd.ExcelWriter(summary_workbook_path) as writer:
        pd.DataFrame(columns=MONITOR_SUMMARY_COLUMNS).to_excel(writer, sheet_name="报警率", index=False)
        pd.DataFrame(columns=CPK_SUMMARY_COLUMNS).to_excel(writer, sheet_name="CPK", index=False)
    for alarm, sheet_id in (("oos", "S1"), ("ooc", "S2")):
        pd.DataFrame([dict(prod_code="M626", step_id="1100", param_name="E2E_CPK", sheet_id=sheet_id, flag=False)]).to_excel(
            resources / f"spc_sheet_{alarm}_decoration.xlsx", sheet_name="M626__flags", index=False,
        )
    st.session_state["fixture_initialized"] = True
    st.session_state["fixture_compute_count"] = 0
    st.cache_data.clear()


class FixtureSource(LiveMonitorSource):
    def compute(self, product, scope, start_date, end_date):
        st.session_state["fixture_compute_count"] += 1
        today = pd.Timestamp.today().normalize()
        facts = pd.DataFrame([dict(
            factory="ARRAY", prod_code=product, step_id="1100", param_name="E2E_CPK",
            sheet_id="S1", sheet_start_time=today, sheet_max=3.0, sheet_min=1.0,
            sheet_mean=2.0, usl=2.5, lsl=1.5, oos_type="USL",
        )])
        ooc = facts.drop(columns=["usl", "lsl", "oos_type"]).assign(sheet_id="S2", ucl=2.2, lcl=1.8, ooc_type="UCL")
        throughput = pd.DataFrame([dict(scope=scope, factory="ARRAY", prod_code=product, event_date=today, item_id=f"S{i}") for i in range(1, 129)])
        features = pd.DataFrame([dict(factory="ARRAY", prod_code=product, step_id="1100", param_name="E2E_CPK", sheet_id=f"S{i}", sheet_start_time=today, sheet_mean=value, usl=2.5, lsl=1.5) for i, value in enumerate([1.8, 1.9, 2.0, 2.1, 2.2])])
        if scope != "spc":
            facts, ooc, throughput = facts.iloc[:0], ooc.iloc[:0], throughput.iloc[:0]
        return dict(oos=facts, ooc=ooc, throughput=throughput, sheet_features_df=features, raw_measurements_df=pd.DataFrame(), spec_empty=False)


source = FixtureSource(port_factory=lambda *_: None, signature_provider=lambda _: "e2e-live-v2", resource_dir_provider=lambda _: resources)
service = OosHistoryService(LiveHistoryStore(source, "oos"), OosDecisionWorkbookRepository(resources))
ooc_service = OocHistoryService(LiveHistoryStore(source, "ooc"), OosDecisionWorkbookRepository(resources))
summary_workbook_service = MonitorSummaryWorkbookService(MonitorSummaryWorkbookStore(summary_workbook_path))
cpk_store = CpkSummaryWorkbookStore(summary_workbook_path)
cpk_service = CpkMonitorService(source, cpk_store, product_resource_root=resources)
composition.build_oos_history_service = lambda resource_dir=None: service
composition.build_ooc_history_service = lambda resource_dir=None: ooc_service
composition.build_live_throughput_reader = lambda: LiveThroughputReader(source)
composition.build_monitor_summary_workbook_service = lambda resource_dir=None: summary_workbook_service
composition.build_cpk_monitor_service = lambda: cpk_service
app_setup.AppSetup.initialize_app = staticmethod(lambda: None)
reloader.deep_reload_modules = lambda: None


def fixture_header(**kwargs):
    st.title("自动预警看板")
    if st.button("刷新缓存", key="fixture_refresh_cache"):
        page_header.perform_hard_reset(kwargs.get("cached_funcs"))
        st.rerun()


page_header.render_page_header = fixture_header
step_labels.get_cached_step_description_map = lambda _db: {"1100": "涂布"}
alert_matrix.render_alert_matrix_filter_bar = lambda *args, **kwargs: None
alert_matrix.render_alert_matrix_board = lambda *args, **kwargs: None
alert_matrix_cache.get_alert_matrix_cached_funcs = lambda: []
SessionManager.AVAILABLE_PRODUCTS = ["M626"]
SessionManager.get_active_config = staticmethod(lambda: {})
db_handler.DatabaseManager = lambda: object()

runpy.run_path(str(PROJECT_ROOT / "app" / "pages" / "自动预警看板.py"), run_name="__main__")
st.caption(f"Fixture calculations: {st.session_state['fixture_compute_count']}")
cpk_records = cpk_store.read()
st.caption(f"Fixture CPK workbook rows: {len(cpk_records)}")
st.caption(f"Fixture CPK warning periods: {int(cpk_records['预警项目数'].fillna(0).sum())}")
