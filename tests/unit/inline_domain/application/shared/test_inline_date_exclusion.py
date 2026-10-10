"""One policy across measurements, feature caches, reports and monitor sources."""

from types import SimpleNamespace
import importlib

import pandas as pd
import pytest

from src.inline_domain.application.shared.decorated_features import fetch_decorated_features
from src.inline_domain.application.monitor.live_source import LiveMonitorSource, _cached_live_payload
from src.inline_domain.application.ctq.ctq_service import CtqReportService
from src.inline_domain.application.spc.spc_service import SpcReportService
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.shared_kernel.config import ConfigLoader
from src.inline_domain.infrastructure.shared.date_exclusion import apply_inline_date_exclusion
from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
from src.inline_domain.application.monitor.oos_monitor_service import OosMonitorService
from src.inline_domain.core.shared.sheet_oos_alerts import build_sheet_oos_alerts
from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService
from src.inline_domain.application.monitor.summary_workbook_service import MonitorSummaryWorkbookService
from src.inline_domain.core.monitor.cpk_summary import normalize_cpk_records


RULE = (("OLED",), "2026-10-01", "2026-10-06")


@pytest.fixture
def inputs(monkeypatch, tmp_path):
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "global.yaml").write_text("application:\n  cache_ttl_hours: 4\n", encoding="utf-8")
    policy = [RULE]
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: policy[0])
    monkeypatch.setattr(ConfigLoader, "get_spc_capability_param_exemptions", lambda: [])
    rows = pd.DataFrame({
        "factory": ["OLED", "OLED", "ARRAY"], "prod_code": ["M678"] * 3,
        "sheet_id": ["A", "B", "C"], "step_id": ["1"] * 3, "lot_id": ["L1"] * 3,
        "sheet_start_time": pd.to_datetime(["2026-09-30", "2026-10-01", "2026-10-01"]),
        "param_name": ["CD"] * 3, "site_name": ["P1"] * 3,
        "param_value": [10., 90., 20.], "data_type": ["SPC"] * 3,
    })
    specs = pd.DataFrame([dict(prod_code="M678", step_id="1", param_name="CD", usl=100., lsl=0.,
                               ucl=80., lcl=20., target=50.)])
    fetch_decorated_features.clear()
    _cached_live_payload.clear()
    yield policy, rows, specs, tmp_path
    fetch_decorated_features.clear()
    _cached_live_payload.clear()


@pytest.mark.parametrize("scope", ["spc", "ctq", "none"])
def test_shared_features_filter_before_deduplication_and_aggregation(inputs, scope):
    _policy, rows, specs, _root = inputs
    # Same point remeasured during the excluded dates: the older reading survives.
    raw = rows.copy()
    raw.loc[1, "sheet_id"] = "A"
    before = raw.copy(deep=True)
    port = SimpleNamespace(get_spc_measurements=lambda _q: apply_inline_date_exclusion(raw, time_column="sheet_start_time"), get_spc_spec_limits=lambda _p: specs)
    payload = fetch_decorated_features(port, "M678", scope, "2026-09-01", "2026-10-06", date_exclusion=RULE)
    assert payload["raw_measurements_df"].sheet_id.tolist() == ["A", "C"]
    assert payload["sheet_features_df"].set_index("sheet_id").loc["A", "sheet_mean"] == 10.
    pd.testing.assert_frame_equal(raw, before)


@pytest.mark.parametrize("kind", ["spc", "ctq"])
def test_report_facades_pass_policy_into_both_cache_layers(inputs, kind):
    policy, rows, specs, _root = inputs
    calls = []

    def read(_query):
        calls.append(1)
        return apply_inline_date_exclusion(rows, time_column="sheet_start_time")

    port = SimpleNamespace(get_spc_measurements=read, get_spc_spec_limits=lambda _p: specs)
    service = SpcReportService if kind == "spc" else CtqReportService
    fetch = getattr(service, f"fetch_{kind}_report_payload")
    run = getattr(service, f"get_{kind}_report_data")
    query = SpcQueryConfig(prod_code="M678", start_date="2026-09-01", end_date="2026-10-06")
    fetch.clear()
    try:
        first = run(port, query.model_dump_json(), snapshot_signature="shared-policy")
        assert first.raw_measurements_df.sheet_id.tolist() == ["A", "C"]
        run(port, query.model_dump_json(), snapshot_signature="shared-policy")
        assert len(calls) == 1
        policy[0] = None
        released = run(port, query.model_dump_json(), snapshot_signature="shared-policy")
        assert released.raw_measurements_df.sheet_id.tolist() == ["A", "B", "C"]
        assert len(calls) == 2
    finally:
        fetch.clear()


@pytest.mark.parametrize("scope", ["spc", "ctq", "aoi_tt", "aoi_rs"])
def test_live_monitor_filters_all_scopes_and_changes_signature(inputs, scope):
    policy, rows, specs, root = inputs
    details = rows.rename(columns={"sheet_start_time": "start_time"}).assign(
        tt_name="TT", tt_qty=[3., 9., 2.], rs_code="RS", code_qty=[3., 9., 2.],
    )
    port = SimpleNamespace(
        get_spc_measurements=lambda _q: apply_inline_date_exclusion(rows, time_column="sheet_start_time"), get_spc_spec_limits=lambda _p: specs,
        get_tt_details=lambda _q: apply_inline_date_exclusion(details), get_tt_spec_limits=lambda _p: pd.DataFrame([
            dict(step_id="1", tt_name="TT", usl=5., ucl=5.),
        ]),
        get_rs_details=lambda _q: apply_inline_date_exclusion(details), get_pass_through=lambda _q: apply_inline_date_exclusion(details),
        get_rs_spec_limits=lambda _p: pd.DataFrame([
            dict(factory=factory, step_id="1", rs_code="RS", type_flag=kind, spec=5.)
            for factory in ["OLED", "ARRAY"] for kind in ["SHEET_ID", "LOT_RATIO"]
        ]),
    )
    source = LiveMonitorSource(port_factory=lambda _s, _p: port,
                               signature_provider=lambda _p: "fixed",
                               resource_dir_provider=lambda _s: root / "resources")
    signature = source.source_signature(["M678"], [scope])
    payload = source.read_payload("M678", scope, "2026-09-01", "2026-10-06")
    passed = payload["throughput"]
    assert not ((passed.factory == "OLED") & (pd.to_datetime(passed.event_date) >= "2026-10-01")).any()
    assert payload["oos"].empty
    policy[0] = None
    assert source.source_signature(["M678"], [scope]) != signature
    unfiltered = source.read_payload("M678", scope, "2026-09-01", "2026-10-06")
    assert len(unfiltered["throughput"]) > len(passed)


@pytest.mark.parametrize("alarm_type", ["oos", "ooc"])
def test_workbook_alarm_reader_filters_projection_and_keeps_source(inputs, alarm_type):
    policy, rows, _specs, _root = inputs
    facts = rows.assign(flag=False, sheet_max=110., sheet_min=10., sheet_mean=60.,
                        usl=100., lsl=0., ucl=50., lcl=20., oos_type="USL", ooc_type="UCL")
    before = facts.copy(deep=True)
    store = SimpleNamespace(read=lambda _s, _p: {"frame": apply_inline_date_exclusion(facts, time_column="sheet_start_time"), "refresh_meta": pd.DataFrame()},
                            source_signature=lambda _p, _s: "unchanged-workbook")
    reader = ExcelAlarmReader(store, alarm_type)
    signature = reader.source_signature(["M678"], ["spc"])
    result = reader.read_product("spc", "M678")
    assert result.alerts_df.item_id.tolist() == ["A", "C"]
    policy[0] = None
    assert reader.source_signature(["M678"], ["spc"]) != signature
    assert reader.read_product("spc", "M678").alerts_df.item_id.tolist() == ["A", "B", "C"]
    pd.testing.assert_frame_equal(facts, before)


def test_shared_sheet_alerts_cannot_release_excluded_dates(inputs):
    _policy, rows, _specs, _root = inputs
    facts = rows.assign(flag=False)
    alerts = build_sheet_oos_alerts(apply_inline_date_exclusion(facts, time_column="sheet_start_time"), time_column="sheet_start_time",
                                   reference_date=pd.Timestamp("2026-10-06"))
    assert set(alerts.sheet_id) == {"A", "C"}


def test_oos_monitor_consumes_filtered_counts_and_denominators_from_readers(inputs):
    _policy, rows, _specs, _root = inputs
    facts = rows.rename(columns={"sheet_start_time": "event_time"}).assign(scope="spc", item_id=rows.sheet_id)
    throughput = facts.rename(columns={"event_time": "event_date"}).assign(sheet_qty=1)
    reader = SimpleNamespace(read_product=lambda _s, _p: SimpleNamespace(
        alerts_df=apply_inline_date_exclusion(facts, time_column="event_time"), source="excel", refreshed_at=None,
    ))
    service = OosMonitorService(reader, throughput_reader=SimpleNamespace(
        read_product=lambda _s, _p: apply_inline_date_exclusion(throughput, time_column="event_date"),
    ))
    result = service.build_dashboard(products=["M678"], scopes=["spc"], factories=["OLED", "ARRAY"],
                                     start_date="2026-09-01", end_date="2026-10-06")
    assert set(result.detail_df.sheet_id) == {"A", "C"}
    assert result.summary_df.oos_count.sum() == 2


@pytest.mark.parametrize("kind", ["spc", "ctq"])
def test_new_cache_policy_preserves_native_payload_during_service_reload(inputs, kind):
    _policy, rows, specs, _root = inputs
    module = importlib.import_module(f"src.inline_domain.application.{kind}.{kind}_service")
    original_symbols = module.__dict__.copy()
    service = getattr(module, "SpcReportService" if kind == "spc" else "CtqReportService")
    fetch = getattr(service, f"fetch_{kind}_report_payload")
    fetch.clear()

    def read(_query):
        importlib.reload(module)
        return apply_inline_date_exclusion(rows, time_column="sheet_start_time")

    port = SimpleNamespace(get_spc_measurements=read, get_spc_spec_limits=lambda _p: specs)
    query = SpcQueryConfig(prod_code="M678", start_date="2026-09-01", end_date="2026-10-06")
    try:
        result = getattr(service, f"get_{kind}_report_data")(
            port, query.model_dump_json(), snapshot_signature="reload-with-policy",
        )
        assert result.raw_measurements_df.sheet_id.tolist() == ["A", "C"]
    finally:
        fetch.clear()
        current_service = getattr(module, "SpcReportService" if kind == "spc" else "CtqReportService")
        getattr(current_service, f"fetch_{kind}_report_payload").clear()
        module.__dict__.update(original_symbols)


def test_maintained_cpk_months_ignore_detail_factory_filter_and_keep_excluded_month(inputs):
    _policy, _rows, _specs, _root = inputs
    records = normalize_cpk_records(pd.DataFrame([
        {"产品": "M678", "监控类型": "SPC", "厂别": "ALL", "周期类型": "月度",
         "时间标签": month, "显示标签": label, "CPK总项目数": 100, "Cpk≥1.33达标率": .8}
        for month, label in [("2026-09", "M9"), ("2026-10", "M10")]
    ]))
    before = records.copy(deep=True)
    result = CpkWorkbookMonitorService(SimpleNamespace(read=lambda: records)).build_dashboard(
        products=["M678"], factories=["OLED"], end_date="2026-10-06",
    )
    assert result.summary_df.M9.iloc[0] == 100
    assert result.summary_df.M10.iloc[0] == 100
    pd.testing.assert_frame_equal(records, before)


def test_maintained_alarm_summary_preserves_months_when_current_week_is_excluded(inputs):
    _policy, _rows, _specs, _root = inputs
    records = pd.DataFrame([
        {"产品": "M678", "监控类型": "SPC", "厂别": "OLED", "周期类型": "月度",
         "时间标签": month, "过货量": 100, "OOS报警片数": 5, "OOC报警片数": 0,
         "SOOS报警片数": 0, "Total报警片数": 5}
        for month in ["2026-09", "2026-10"]
    ])
    before = records.copy(deep=True)
    def reset_update(*_args, **kwargs):
        assert kwargs["reset_current_week"] is True
        return records.copy(deep=True), []

    store = SimpleNamespace(refresh_current_week=reset_update, read=lambda: records)
    result = MonitorSummaryWorkbookService(store).refresh_summary(
        products=["M678"], scopes=["spc"], factories=["OLED"],
        alerts_df=pd.DataFrame(), throughput_df=pd.DataFrame(), end_date=pd.Timestamp("2026-10-06"),
    )
    assert result.M9.iloc[0] == 100
    assert result.M10.tolist() == [100, 0, 0, 5, 5]
    pd.testing.assert_frame_equal(records, before)
