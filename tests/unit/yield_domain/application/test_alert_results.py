"""Published Yield results survive cold readers and never require analysis."""
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore

DAY = date(2026, 10, 20)
NOW = datetime(2026, 10, 20, 10, tzinfo=timezone(timedelta(hours=8)))
HEALTH = {"status": "fresh", "refreshed_at": NOW.isoformat(),
          "source_start": "2026-07-01", "source_end": "2026-10-20", "error_code": ""}
LOT = {"超规 Lot ID": "L1", "异常 Code": "D1", "管控规格线": "1.00%",
       "实际不良率": "2.00%", "不良panel数": 2, "入库时间": "2026/10/15", "阵列投入时间": "2026/10/01"}


def results(tmp_path, clock=lambda: NOW, policy="v1"):
    store = YieldAlertWorkbookStore({"lot": tmp_path / "lots.xlsx", "trend": tmp_path / "trends.xlsx"})
    return YieldAlertResults(store, policy_signature=lambda product: policy, ttl_seconds=43200, clock=clock)


def publish(service, product="P1", kind="lot", records=None, **overrides):
    return service.publish(product, kind, [LOT] if records is None else records,
        reference_date=DAY, coverage_start="2026-09-20", coverage_end="2026-10-20",
        has_data=True, health=HEALTH, calculation_signature="report-v1", **overrides)


def test_per_product_publication_is_atomic_and_unchanged_results_do_not_rewrite(tmp_path):
    service = results(tmp_path)
    assert publish(service)
    assert publish(service, "P2", records=[])
    path = tmp_path / "lots.xlsx"
    previous = path.read_bytes(), path.stat().st_mtime_ns
    assert not publish(service, "P1")
    assert (path.read_bytes(), path.stat().st_mtime_ns) == previous
    cold = results(tmp_path)
    assert cold.read("P1", "lot", DAY).status == "ready"
    assert cold.read("P1", "lot", DAY).records.empty  # New decisions default to not released.
    assert cold.read("P2", "lot", DAY).status == "ready"
    assert cold.read("P2", "lot", DAY).records.empty
    assert cold.read("P3", "lot", DAY).status == "missing"


def test_failure_preserves_last_success_and_other_kind(tmp_path):
    service = results(tmp_path)
    publish(service)
    publish(service, kind="trend", records=[])
    service.mark_failed("P1", "lot", DAY, "YIELD_ANALYSIS_FAILED")
    assert service.read("P1", "lot", DAY).status == "failed"
    assert service.read("P1", "trend", DAY).status == "ready"
    assert pd.read_excel(tmp_path / "lots.xlsx", sheet_name="P1")["超规 Lot ID"].tolist() == ["L1"]
    assert publish(service, records=[])
    assert service.read("P1", "lot", DAY).records.empty


def test_missing_stale_policy_and_uncovered_results_are_distinct(tmp_path):
    service = results(tmp_path)
    assert service.read("P1", "lot", DAY).status == "missing"
    publish(service)
    assert results(tmp_path, clock=lambda: NOW + timedelta(hours=13)).read("P1", "lot", DAY).status == "stale"
    assert results(tmp_path, policy="v2").read("P1", "lot", DAY).status == "stale"
    assert service.read("P1", "lot", date(2026, 10, 21)).status == "stale"
    service.publish("P2", "lot", [], reference_date=DAY, coverage_start="2026-10-19",
        coverage_end="2026-10-20", has_data=True, health=HEALTH, calculation_signature="short")
    assert service.read("P2", "lot", DAY).status == "uncovered"


def test_valid_empty_facts_are_not_a_missing_publication(tmp_path):
    service = results(tmp_path)
    service.publish("P1", "lot", [], reference_date=DAY, coverage_start="2026-09-20",
        coverage_end="2026-10-20", has_data=False, health=HEALTH, calculation_signature="empty")
    assert service.read("P1", "lot", DAY).status == "no_data"


def test_source_age_is_not_reset_by_republishing(tmp_path):
    service = results(tmp_path)
    old = {**HEALTH, "refreshed_at": (NOW-timedelta(hours=13)).isoformat()}
    service.publish("P1", "lot", [], reference_date=DAY, coverage_start="2026-09-20",
        coverage_end="2026-10-20", has_data=True, health=old, calculation_signature="old")
    assert service.read("P1", "lot", DAY).status == "stale"


def test_source_health_naive_time_uses_local_timezone(tmp_path):
    service = results(tmp_path)
    health = {**HEALTH, "refreshed_at": "2026-10-20T09:00:00"}
    service.publish("P1", "lot", [], reference_date=DAY, coverage_start="2026-08-01",
        coverage_end=str(DAY), has_data=True, health=health, calculation_signature="local")
    assert service.read("P1", "lot", DAY).status == "ready"


def test_historical_trend_rows_do_not_turn_current_status_red(tmp_path):
    service = results(tmp_path)
    row = {"level": "group", "defect_group": "G", "defect_desc": None,
        "time_period": "2026-08", "curr_rate": .02, "prev_rate": .001,
        "rules": ["环比翻倍"], "period_scope": "monthly", "is_current": False}
    publish(service, kind="trend", records=[row])
    assert pd.read_excel(tmp_path/"trends.xlsx", sheet_name="P1").shape[0] == 1
    assert service.read("P1", "trend", DAY).records.empty


def test_saved_monthly_trends_are_suppressed_and_weekly_policy_keys_the_cache(tmp_path, monkeypatch):
    from src.yield_domain.application import alert_results as module

    service = results(tmp_path)
    base = {'level': 'group', 'defect_group': 'G', 'defect_desc': None,
            'curr_rate': .02, 'prev_rate': .001, 'rules': ['rise'], 'is_current': True}
    monthly = {**base, 'time_period': '2026-10', 'period_scope': 'monthly'}
    weekly = {**base, 'time_period': '2026-W42', 'period_scope': 'weekly'}
    publish(service, kind='trend', records=[monthly, weekly])
    before = (tmp_path / 'trends.xlsx').read_bytes()
    published = service.read('P1', 'trend', DAY)
    assert published.records['time_period'].tolist() == ['2026-W42']
    signature = service.source_signature('P1', 'trend', DAY)
    monkeypatch.setattr(module, 'TREND_ALERT_SCOPES', ('monthly', 'weekly'))
    assert service.source_signature('P1', 'trend', DAY) != signature
    assert (tmp_path / 'trends.xlsx').read_bytes() == before
    monkeypatch.setattr(module, 'TREND_ALERT_SCOPES', ('weekly',))
    publish(service, kind='trend', records=[monthly])
    assert service.read('P1', 'trend', DAY).status == 'ready'
    assert service.read('P1', 'trend', DAY).records.empty


def test_write_failure_keeps_previous_bytes(tmp_path, monkeypatch):
    from src.yield_domain.infrastructure import alert_result_workbooks as module
    service = results(tmp_path)
    publish(service)
    path = tmp_path / "lots.xlsx"
    before = path.read_bytes()
    monkeypatch.setattr(module, "replace_workbook_sheets", lambda *args: type("Failure", (), {"written": False})())
    with pytest.raises(OSError):
        publish(service, records=[])
    assert path.read_bytes() == before


@pytest.mark.parametrize("kind, row_key", [("lot", "yield_lot_oos"), ("trend", "yield_trend_fluctuation")])
def test_download_partial_flag_edits_republication_and_cached_public_projection(tmp_path, monkeypatch, kind, row_key):
    from io import BytesIO
    from app.sections.shared.alert_results import read_alert_result
    from src.yield_domain import composition

    service = results(tmp_path)
    monkeypatch.setattr(composition, "build_yield_alert_results", lambda **_: service)
    key = "超规 Lot ID" if kind == "lot" else "defect_group"
    base = LOT if kind == "lot" else dict(
        level="group", defect_desc=None, time_period="2026-W42", curr_rate=.02,
        prev_rate=.001, rules=["rise"], period_scope="weekly", is_current=True,
    )
    records = [{**base, key: identity} for identity in ("001", "002")]
    publish(service, kind=kind, records=records)

    def visible():
        return read_alert_result(row_key, "P1", DAY)["records"][key].tolist()

    def download():
        return pd.read_excel(BytesIO(service.store.decision_workbook("P1", kind)),
                             sheet_name="P1__flags", dtype=object, keep_default_na=False)

    def edit(value):
        change = download().iloc[:1].assign(flag=value)
        stream = BytesIO()
        with pd.ExcelWriter(stream) as writer:
            change.to_excel(writer, sheet_name="P1__flags", index=False)
        service.store.update_decisions("P1", kind, stream.getvalue())

    assert visible() == ([] if kind == "lot" else ["001", "002"])
    from openpyxl import load_workbook
    stale = load_workbook(BytesIO(service.store.decision_workbook("P1", kind)))
    stale["P1__flags"].cell(row=3, column=stale["P1__flags"].max_column).value = "Delete"
    stale_upload = BytesIO()
    stale.save(stale_upload)
    edit(False)
    assert visible() == (["001"] if kind == "lot" else ["001", "002"])
    edit("Delete")
    with pytest.raises(ValueError, match="重新下载"):
        service.store.update_decisions("P1", kind, stale_upload.getvalue())
    assert visible() == ([] if kind == "lot" else ["002"])
    legacy = BytesIO()
    with pd.ExcelWriter(legacy) as writer:
        download().assign(flag=False).to_excel(writer, sheet_name="P1__flags", index=False)
    with pytest.raises(ValueError, match="旧版"):
        service.store.update_decisions("P1", kind, legacy.getvalue())
    publish(service, kind=kind, records=[])
    assert visible() == []
    publish(service, kind=kind, records=records)
    assert download().set_index(key)["flag"].to_dict() == {"001": "Delete", "002": kind == "lot"}
    assert visible() == ([] if kind == "lot" else ["002"])
    edit(True)
    assert visible() == ([] if kind == "lot" else ["002"])


def test_yield_alert_centers_render_and_apply_admin_upload(tmp_path, monkeypatch):
    from io import BytesIO
    from streamlit.testing.v1 import AppTest
    from app.sections.yield_domain import published_alert_center
    from src.yield_domain import composition

    service = results(tmp_path)
    publish(service)
    publish(service, kind="trend", records=[])
    monkeypatch.setattr(published_alert_center, "build_yield_alert_results", lambda: service)
    monkeypatch.setattr(composition, "build_yield_alert_results", lambda: service)
    app = AppTest.from_string(
        "from datetime import date\n"
        "from app.sections.yield_domain.published_alert_center import render_saved_yield_alerts\n"
        "render_saved_yield_alerts('P1', date(2026, 10, 20))"
    ).run()
    assert not app.file_uploader
    for spelling in ("true", "ture"):
        app.query_params["admin"] = spelling
        app.run()
        assert [widget.key for widget in app.file_uploader] == ["yield_P1_lot_upload", "yield_P1_trend_upload"]
    flags = pd.read_excel(BytesIO(service.store.decision_workbook("P1", "lot")), sheet_name="P1__flags")
    data = BytesIO()
    with pd.ExcelWriter(data) as writer:
        flags.assign(flag=False).to_excel(writer, sheet_name="P1__flags", index=False)
    app.file_uploader(key="yield_P1_lot_upload").upload("decisions.xlsx", data.getvalue()).run()
    app.button(key="yield_P1_lot_save").click().run()
    assert not app.exception
    assert app.dataframe[0].value["超规 Lot ID"].tolist() == ["L1"]
    del app.query_params["admin"]
    app.run()
    assert not app.file_uploader
    assert "flag" not in app.dataframe[0].value


def test_expiry_changes_signature_without_rewriting_workbook(tmp_path):
    clock = {"now": NOW}
    service = results(tmp_path, clock=lambda: clock["now"])
    publish(service)
    first = service.source_signature("P1", "lot", DAY)
    clock["now"] += timedelta(hours=13)
    assert service.source_signature("P1", "lot", DAY) != first


def test_reader_rejects_corrupt_metadata_and_recovers_on_publication(tmp_path):
    service = results(tmp_path)
    publish(service)
    from src.shared_kernel.utils.excel_tools import replace_workbook_sheets
    path = tmp_path / "lots.xlsx"
    corrupted = pd.read_excel(path, sheet_name="P1").iloc[:0]
    assert replace_workbook_sheets(path, {"P1": corrupted}).written
    with pytest.raises(ValueError):
        service.read("P1", "lot", DAY)
    assert publish(service)
    assert service.read("P1", "lot", DAY).status == "ready"


def test_lot_report_window_is_explicit_and_preserves_published_latest_source_anchor(tmp_path):
    service = results(tmp_path)
    rows = [{**LOT, "超规 Lot ID": "OLDER", "入库时间": "2026/09/18"},
            {**LOT, "超规 Lot ID": "LATEST_ALERT", "入库时间": "2026/10/15"}]
    service.publish("P1", "lot", rows, reference_date=DAY, coverage_start="2026-09-01",
                    coverage_end="2026-10-20", has_data=True, health=HEALTH,
                    calculation_signature="scope", lot_reference_date="2026-10-18")
    from src.shared_kernel.utils.excel_tools import replace_workbook_sheets
    path = service.store.paths["lot"]
    decisions = pd.read_excel(path, sheet_name="P1__flags").assign(flag=False)
    assert replace_workbook_sheets(path, {"P1__flags": decisions}).written
    # The latest source Lot can be normal and therefore absent from the alert rows.
    monthly = service.read("P1", "lot", DAY, time_period=30)
    assert monthly.records["超规 Lot ID"].tolist() == ["OLDER", "LATEST_ALERT"]
    weekly = service.read("P1", "lot", DAY)
    assert weekly.records["超规 Lot ID"].tolist() == ["LATEST_ALERT"]
    assert service.source_signature("P1", "lot", DAY) != service.source_signature("P1", "lot", DAY, time_period=30)
    explicit = service.read("P1", "lot", DAY, start_date="2026-10-01", end_date="2026-10-18")
    assert explicit.records["超规 Lot ID"].tolist() == ["LATEST_ALERT"]
    assert service.read("P1", "lot", DAY, start_date="2026-08-01", end_date="2026-10-18").status == "uncovered"

@pytest.mark.parametrize("decision", [None, "Delete"])
def test_upgrade_trend_keeps_current_marker_and_manual_decision(tmp_path, decision):
    service = results(tmp_path)
    rows = [dict(level="group", defect_group="A", defect_desc="", time_period=week,
                 curr_rate=2, prev_rate=1, rules="up", period_scope="weekly", is_current=current)
            for week, current in [("2026-W41", False), ("2026-W42", True)]]
    publish(service, kind="trend", records=rows)
    store = service.store
    path = store.paths["trend"]
    legacy = pd.read_excel(path, sheet_name=None, dtype=object)
    del legacy["P1__flags"]
    legacy["P1__meta"] = legacy["P1__meta"].drop(columns="release_schema_version")
    legacy["P1"] = legacy["P1"].drop(columns="flag")
    if decision is not None:
        legacy["P1"]["flag"] = [True, decision]
    with pd.ExcelWriter(path) as writer:
        for name, frame in legacy.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    from tools.initialize_yield_alert_controls import main

    assert main(["--product", "P1", "--kind", "trend"], store=store, enabled_products=["P1"]) == 0
    after = pd.read_excel(path, sheet_name=None, dtype=object)
    expected = [False, False] if decision is None else [True, decision]
    assert after["P1"]["flag"].tolist() == after["P1__flags"]["flag"].tolist() == expected
    assert after["P1"]["is_current"].tolist() == [False, True]
    assert after["P1__meta"]["generated_at"].equals(legacy["P1__meta"]["generated_at"])
    assert not store.initialize_controls("P1", "trend")
    public = service.read("P1", "trend", DAY)
    assert public.records["time_period"].tolist() == (["2026-W42"] if decision is None else [])
