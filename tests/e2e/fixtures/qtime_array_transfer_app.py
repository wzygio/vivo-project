"""Production Q-Time input adapter/service/page with isolated SQL transports."""

from contextlib import contextmanager
from datetime import date, timedelta
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
for directory in (ROOT, ROOT / "src"):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

from app.sections.indicator_domain.qtime.dashboard import render_qtime_dashboard
from src.indicator_domain.application.qtime.alert_results import QTimeAlertResults
from src.indicator_domain.application.qtime.cached_monitoring import get_qtime_cached_funcs
from src.indicator_domain.application.qtime.service import QTimeReportService
from src.indicator_domain.infrastructure.qtime.array_transfer_source import ArrayTransferQTimeSource
from src.indicator_domain.infrastructure.qtime.repository import DETAIL_COLUMNS, QTimeRepository
from src.shared_kernel.config import ConfigLoader


class ReadOnlyEngine:
    @contextmanager
    def connect(self):
        yield self

    @contextmanager
    def begin(self):
        yield

    def exec_driver_sql(self, sql):
        assert sql.startswith("SET ")


class MemoryPublication:
    def read(self):
        return st.session_state.get("fixture_publication")

    def save(self, shop, alerts, products, metadata):
        st.session_state["fixture_publication"] = {
            "__meta__": pd.DataFrame([metadata]), f"alerts_{shop}": alerts.copy(),
            f"products_{shop}": products.copy(),
        }

    def source_signature(self):
        return "isolated-fixture-publication"


class Decisions:
    decoration_path = None

    def load_decisions(self):
        if decision not in ("保留预警", "删除记录") or mode != "合成样本":
            return pd.DataFrame()
        policy = ConfigLoader.get_data_forward_policy()
        timestamp = base_time + (pd.Timedelta(days=policy.offset_days) if policy.enabled else pd.Timedelta(0))
        return pd.DataFrame([{
            "prodcode": "M626", "step_desc": "1A200->21100", "lot_id": "FIXTURE_M626",
            "timekey": timestamp.strftime("%Y%m%d%H%M%S"),
            "flag": False if decision == "保留预警" else "Delete",
        }])


st.set_page_config(page_title="Q-Time fixed source E2E", layout="wide")
mode = st.selectbox("测试数据", ["合成样本", "真实只读样本", "空结果", "读取失败"])
decision = st.selectbox("测试规格与决策", ["不设置规格", "正式规格", "默认修饰", "保留预警", "删除记录"])
identity = (mode, decision)
if st.session_state.get("fixture_identity") != identity:
    for key in ("qtime_report_result", "qtime_report_signature", "fixture_publication"):
        st.session_state.pop(key, None)
    for cached in get_qtime_cached_funcs():
        cached.clear()
    st.session_state["fixture_identity"] = identity

fixture_root = Path(os.environ["VIVO_QTIME_FIXTURE_ROOT"])
base_day = (date.today().replace(day=1) - timedelta(days=1)).replace(day=10)
base_time = pd.Timestamp(base_day) + pd.Timedelta(hours=8)
event_rows, master_rows = [], []
for product, spec, hours in (("M626", "FIXTURE_SPEC_626", 2.5), ("M678", "FIXTURE_SPEC_678", 1.0)):
    for station, timestamp in (("1A200", base_time), ("21100", base_time + pd.Timedelta(hours=hours))):
        event_rows.append({
            "lot_id": f"FIXTURE_{product}", "oper_code": station, "event_time": timestamp,
            "event_timekey": timestamp.strftime("%Y%m%d%H%M%S%f"), "prod_id": spec,
            "prod_ver": "00001", "factory": "ARRAY" if station == "1A200" else "OLED",
            "prod_qty": 20,
        })
    master_rows.append({"productspecname": spec, "productspecversion": "00001", "productcode": product, "factoryname": "ARRAY"})
# The same Lot re-enters later; only its earliest entry may reach the report.
event_rows.append({
    **event_rows[0], "event_time": base_time + pd.Timedelta(microseconds=1),
    "event_timekey": (base_time + pd.Timedelta(microseconds=1)).strftime("%Y%m%d%H%M%S%f"),
})
events, master = pd.DataFrame(event_rows), pd.DataFrame(master_rows)
if mode == "真实只读样本":
    capture = Path(os.environ["VIVO_QTIME_CAPTURE_ROOT"])
    events = pd.read_parquet(capture / "bounded-events.parquet")
    master = pd.read_parquet(capture / "bounded-master.parquet")
elif mode == "空结果":
    events = events.iloc[:0].copy()


def read_sql(statement, connection, params=None):
    sql = str(statement)
    if "dwr_mes_lot_hist" in sql:
        st.session_state["fixture_source_reads"] = st.session_state.get("fixture_source_reads", 0) + 1
        if mode == "读取失败":
            raise OSError("FIXTURE_PRIVATE_TRANSPORT_ERROR")
        times = pd.to_datetime(events.event_time)
        return events.loc[times.ge(params["start_time"]) & times.lt(params["end_time"])].copy()
    if "dwr_mes_productspec" in sql:
        return master.loc[master.productspecname.isin(params["specs"])].copy()
    return pd.DataFrame(columns=DETAIL_COLUMNS)


engine = ReadOnlyEngine()
source = ArrayTransferQTimeSource(engine, enabled_products=tuple(ConfigLoader.get_enabled_products()))
repository = QTimeRepository(
    SimpleNamespace(engine=engine), snapshot_dir=fixture_root / mode / decision,
    supplemental_source=source,
)
publication = QTimeAlertResults(MemoryPublication(), ttl_seconds=ConfigLoader.get_cache_ttl_seconds())
service = QTimeReportService(
    repository, Decisions(), constrain_display=True, alert_results=publication,
    spec_overrides=(ConfigLoader.load_domain_config("indicator_domain")["qtime"]["spec_overrides"]
                    if decision == "正式规格" else
                    {"ARRAY/1A200/21100": 2.0} if decision != "不设置规格" else {}),
)


def read_alerts(row, product, reference, **selection):
    return publication.read(product, reference, **selection)


# Fragment reruns execute after the top-level script context has exited. Keep
# the fixture-only transports installed for this isolated process's lifetime.
import app.sections.shared.alert_results as shared_alerts

pd.read_sql = read_sql
shared_alerts.read_alert_result = read_alerts
render_qtime_dashboard(service)
st.caption(f"fixture_mode: {mode}")
st.caption(f"fixture_decision: {decision}")
st.caption(f"fixture_source_reads: {st.session_state.get('fixture_source_reads', 0)}")
