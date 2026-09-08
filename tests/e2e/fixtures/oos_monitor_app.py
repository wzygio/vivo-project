"""Isolated Excel inputs exercising the real page; raw/DB access is forbidden."""
from __future__ import annotations

import runpy
import sys
import uuid
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
from app.utils import app_setup, reloader
from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService
from src.inline_domain.application.monitor.live_source import LiveMonitorSource
from src.inline_domain.application.monitor.summary_workbook_service import MonitorSummaryWorkbookService
from src.inline_domain.core.monitor.period_summary import current_period_windows
from src.inline_domain.infrastructure.monitor.excel_alarm_store import ExcelAlarmStore
from src.inline_domain.infrastructure.monitor.summary_workbook_store import MonitorSummaryWorkbookStore
from src.inline_domain.infrastructure.monitor.cpk_summary_workbook_store import CpkSummaryWorkbookStore
from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore
import src.inline_domain.composition as composition
import src.shared_kernel.infrastructure.db_handler as db_handler

if "fixture_directory" not in st.session_state:
    st.session_state["fixture_directory"] = str(
        PROJECT_ROOT / "output" / "tmp" / "monitor-excel-e2e" / uuid.uuid4().hex
    )
fixture_root = Path(st.session_state["fixture_directory"])
fixture_root.mkdir(parents=True, exist_ok=True)
summary_path = fixture_root / "monitor-summary.xlsx"
cpk_latest_path = fixture_root / "spc_cpk_cpm_decoration.xlsx"
today = pd.Timestamp.today().normalize()
scopes = ("spc", "ctq", "aoi_tt", "aoi_rs")


def write_cpk_latest(decorated=False):
    previous_week = today - pd.Timedelta(days=today.weekday() + 7)
    iso = previous_week.isocalendar()
    rows = [dict(prod_code="M626", factory="ARRAY", step_id="1100", param_name="E2E_CPK",
                 period_type=kind, period_label=label, cpk_corrected=0.8, flag=decorated)
            for kind, label in (("month", today.strftime("%Y-%m")), ("week", f"{iso.year}-W{iso.week:02d}"))]
    pd.DataFrame(rows).to_excel(cpk_latest_path, sheet_name="M626", index=False)
paths = {alarm: {scope: fixture_root / f"{scope}_sheet_{alarm}_decoration.xlsx"
                 for scope in scopes} for alarm in ("oos", "ooc")}


def write_details(alarm: str, scope: str, *, decorated: bool = False) -> None:
    frame = pd.DataFrame()
    if scope == "spc":
        frame = pd.DataFrame([dict(
            factory="ARRAY", prod_code="M626", step_id="1100", param_name="E2E",
            sheet_id="S1" if alarm == "oos" else "S2", sheet_start_time=today,
            sheet_max=3., sheet_min=1., sheet_mean=2., usl=2.5, lsl=1.5,
            ucl=2.2, lcl=1.8, oos_type="USL", ooc_type="UCL", flag=decorated,
        )])
    with pd.ExcelWriter(paths[alarm][scope]) as writer:
        frame.to_excel(writer, sheet_name="M626", index=False)
        pd.DataFrame([dict(scope=scope, prod_code="M626", last_generated_at=today)]).to_excel(
            writer, sheet_name="__refresh_meta__", index=False,
        )


if not summary_path.exists():
    rows, cpk_rows = [], []
    for window in current_period_windows(today):
        count = 0 if window.period_type == "周度" else 100
        rows.append({"产品": "M626", "监控类型": "ALL", "厂别": "ALL",
                     "周期类型": window.period_type, "时间标签": window.time_label,
                     "显示标签": window.display_label, "过货量": 1000,
                     "OOC报警片数": count, "OOC报警率": count / 1000,
                     "SOOS报警片数": 0, "SOOS报警率": 0,
                     "OOS报警片数": count, "OOS报警率": count / 1000,
                     "Total报警片数": count, "Total报警率": count / 1000})
        cpk_rows.append({"产品": "M626", "监控类型": "SPC", "厂别": "ALL",
                         "周期类型": window.period_type, "时间标签": window.time_label,
                         "显示标签": window.display_label, "CPK总项目数": 10,
                         "Cpk≥1.33达标率": .8, "达标项目数": 8, "预警项目数": 2})
    closed = dict(rows[0])
    previous = today.replace(day=1) - pd.DateOffset(months=1)
    closed.update({"周期类型": "月度", "时间标签": previous.strftime("%Y-%m"),
                   "显示标签": f"M{previous.month}", "OOS报警片数": 777,
                   "OOS报警率": .777})
    rows.append(closed)
    previous_week = today - pd.Timedelta(days=today.weekday() + 7)
    iso = previous_week.isocalendar()
    cpk_previous = dict(cpk_rows[-1])
    cpk_previous.update({"时间标签": f"{iso.year}-W{iso.week:02d}", "显示标签": f"W{iso.week}"})
    cpk_rows.append(cpk_previous)
    with pd.ExcelWriter(summary_path) as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="报警率", index=False)
        pd.DataFrame(cpk_rows).to_excel(writer, sheet_name="CPK", index=False)
    for alarm in paths:
        for scope in scopes:
            write_details(alarm, scope)
    st.session_state["fixture_decorated"] = False
    write_cpk_latest()


def forbidden_raw(*args, **kwargs):
    raise AssertionError("Inline Excel-only E2E attempted raw computation or database access")


service = ExcelAlarmReader(ExcelAlarmStore(paths["oos"]), "oos")
ooc_service = ExcelAlarmReader(ExcelAlarmStore(paths["ooc"]), "ooc")
summary_store = MonitorSummaryWorkbookStore(summary_path)
cpk_store = CpkSummaryWorkbookStore(summary_path)
composition.build_oos_history_service = lambda resource_dir=None: service
composition.build_ooc_history_service = lambda resource_dir=None: ooc_service
composition.build_monitor_summary_workbook_service = lambda resource_dir=None: MonitorSummaryWorkbookService(summary_store)
composition.build_cpk_monitor_service = lambda: CpkWorkbookMonitorService(cpk_store, latest_reader=CpkLatestExcelStore(cpk_latest_path))
composition.build_live_throughput_reader = forbidden_raw
composition.build_live_monitor_source = forbidden_raw
composition.build_raw_measurement_repository = forbidden_raw
LiveMonitorSource.compute = forbidden_raw
db_handler.DatabaseManager = forbidden_raw
app_setup.AppSetup.initialize_app = staticmethod(lambda: None)
reloader.deep_reload_modules = lambda: None


def fixture_header(**kwargs):
    st.title("自动预警看板")
    if st.button("刷新缓存", key="fixture_refresh_cache"):
        page_header.perform_hard_reset(kwargs.get("cached_funcs"))
        st.rerun()
    if st.button("Fixture: decorate OOS Excel", key="fixture_edit_excel"):
        write_details("oos", "spc", decorated=True)
        st.session_state["fixture_decorated"] = True
    if st.button("Fixture: decorate CPK Excel", key="fixture_edit_cpk"):
        write_cpk_latest(decorated=True)
        st.session_state["fixture_cpk_decorated"] = True


page_header.render_page_header = fixture_header
alert_matrix.render_alert_matrix_filter_bar = lambda *args, **kwargs: None
alert_matrix.render_alert_matrix_board = lambda *args, **kwargs: None
alert_matrix_cache.get_alert_matrix_cached_funcs = lambda: []
SessionManager.AVAILABLE_PRODUCTS = ["M626"]
SessionManager.get_active_config = staticmethod(lambda: {})

runpy.run_path(str(PROJECT_ROOT / "app" / "pages" / "自动预警看板.py"), run_name="__main__")
records = summary_store.read()
week = records.loc[records["周期类型"].eq("周度")].iloc[0]
year = records.loc[records["周期类型"].eq("年度")].iloc[0]
closed = records.loc[records["周期类型"].eq("月度") & records["OOS报警片数"].eq(777)]
st.caption(f"Fixture week OOS: {int(week['OOS报警片数'])}")
st.caption(f"Fixture year OOS: {int(year['OOS报警片数'])}")
st.caption(f"Fixture closed preserved: {len(closed) == 1}")
st.caption(f"Fixture CPK warning total: {int(cpk_store.read()['预警项目数'].sum())}")
cpk_records = cpk_store.read()
st.caption(f"Fixture CPK history preserved: {cpk_records.loc[cpk_records['周期类型'].isin(['年度', '季度']), '预警项目数'].eq(2).all()}")
st.caption(f"Fixture Excel decorated: {st.session_state['fixture_decorated']}")
st.caption(f"Fixture CPK Excel decorated: {st.session_state.get('fixture_cpk_decorated', False)}")
