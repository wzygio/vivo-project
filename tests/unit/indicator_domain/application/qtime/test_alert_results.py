from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.indicator_domain.application.qtime.alert_results import QTimeAlertResults, QTIME_ALERT_COLUMNS
from src.indicator_domain.infrastructure.qtime.alert_workbook import QTimeAlertWorkbook


AS_OF = date(2026, 10, 27)
NOW = datetime(2026, 10, 27, 10)
HEALTH = {"status": "fresh", "refreshed_at": "2026-10-27T09:30:00"}


def rows():
    return pd.DataFrame([
        dict(shop="ARRAY", prodcode="M678", f_step="100", t_step="200", step_desc=step,
             lot_id=lot, q_spec=12., wait_time=14., over_hours=2., timekey=when)
        for lot, when, step in [
            ("LAST", "20261020100000", "PATH1"),
            ("CURRENT", "20261026100000", "PATH2"),
            ("EARLY", "20261001100000", "PATH1"),
        ]
    ])


def service(tmp_path: Path, *, clock=lambda: NOW):
    return QTimeAlertResults(QTimeAlertWorkbook(tmp_path / "qtime-alerts.xlsx"), ttl_seconds=86400, clock=clock)


def publish_all(results, frame=None):
    frame = rows() if frame is None else frame
    for shop in ("ARRAY", "OLED", "TP"):
        chosen = frame if shop == "ARRAY" else frame.iloc[:0]
        results.publish(shop, chosen, chosen, HEALTH, AS_OF)


def test_published_reader_filters_exact_product_and_last_iso_week(tmp_path):
    results = service(tmp_path)
    publish_all(results)
    value = results.read("M678", AS_OF)
    assert value["state"] == "alert"
    assert value["records"]["lot_id"].tolist() == ["LAST"]
    assert value["alert_factories"] == ["ARRAY"]
    assert value["records"].columns.tolist() == QTIME_ALERT_COLUMNS
    assert "flag" not in value["records"]
    assert results.read("M67", AS_OF)["state"] == "no_data"


def test_report_selection_preserves_rolling_window_shop_and_station_scope(tmp_path):
    results = service(tmp_path)
    results.publish("ARRAY", rows(), rows(), HEALTH, AS_OF)
    value = results.read(
        "M678", AS_OF, shops=("ARRAY",), step_descriptions=("PATH2",),
        start_date="2026-09-01", end_date="2026-10-27",
    )
    assert value["records"]["lot_id"].tolist() == ["CURRENT"]
    assert value["metadata"]["total_lots"] == 1
    assert results.read("M678", AS_OF)["state"] == "error"  # Missing other shops is not green.


def test_same_lot_across_shops_is_counted_once(tmp_path):
    results = service(tmp_path)
    for shop in ("ARRAY", "OLED", "TP"):
        source = rows().iloc[:1].assign(shop=shop)
        results.publish(shop, source, source, HEALTH, AS_OF)
    value = results.read("M678", AS_OF)
    assert value["state"] == "alert" and len(value["records"]) == 3
    assert value["metadata"]["total_lots"] == 1


def test_explicit_empty_template_is_not_a_successful_publication(tmp_path):
    results = service(tmp_path)
    assert results.store.initialize_empty_template()
    before = results.store.path.read_bytes()
    assert not results.store.initialize_empty_template()
    assert before == results.store.path.read_bytes()
    value = results.read('M678', AS_OF)
    assert value['state'] == 'no_data'
    assert value['records'].empty
    assert value['metadata'].get('status') == 'initialized'
    # Templates have no invented freshness or completed-analysis timestamp.
    assert results.store.read()['__meta__'].empty
    results.publish('ARRAY', rows(), rows(), HEALTH, AS_OF)
    assert results.read('M678', AS_OF, shops=('ARRAY',))['state'] == 'alert'
    assert results.read('M678', AS_OF)['state'] == 'error'  # Other shops still unknown.


def test_nonempty_rows_without_metadata_are_not_an_initialized_template(tmp_path):
    results = service(tmp_path)
    results.store.initialize_empty_template()
    sheets = results.store.read()
    sheets['alerts_ARRAY'] = rows().reindex(columns=QTIME_ALERT_COLUMNS)
    with pd.ExcelWriter(results.store.path) as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    assert results.read('M678', AS_OF)['state'] == 'error'


def test_valid_empty_publication_differs_from_missing_and_stale(tmp_path):
    results = service(tmp_path)
    assert results.read("M678", AS_OF)["state"] == "error"
    publish_all(results, rows().iloc[:0])
    assert results.read("M678", AS_OF)["state"] == "no_data"
    assert results.read("M678", AS_OF + timedelta(days=1))["state"] == "error"
    expired = service(tmp_path, clock=lambda: NOW + timedelta(days=2))
    assert expired.read("M678", AS_OF)["state"] == "error"


def test_product_with_source_but_no_alerts_is_ok(tmp_path):
    results = service(tmp_path)
    for shop in ("ARRAY", "OLED", "TP"):
        source = rows() if shop == "ARRAY" else rows().iloc[:0]
        results.publish(shop, source, rows().iloc[:0], HEALTH, AS_OF)
    assert results.read("M678", AS_OF)["state"] == "ok"


def test_publication_rejects_stale_and_preserves_existing_bytes(tmp_path):
    results = service(tmp_path)
    publish_all(results)
    before = results.store.path.read_bytes()
    with pytest.raises(ValueError, match="fresh"):
        results.publish("ARRAY", rows(), rows(), {"status": "stale"}, AS_OF)
    assert results.store.path.read_bytes() == before
    publish_all(results)
    assert results.store.path.read_bytes() == before  # Same semantic result is not rewritten.


def test_reader_is_read_only_and_source_signature_tracks_file_policy_and_expiry(tmp_path):
    results = service(tmp_path)
    publish_all(results)
    before = results.store.path.read_bytes()
    signature = results.source_signature("M678", AS_OF)
    assert results.read("M678", AS_OF)["state"] == "alert"
    assert results.store.path.read_bytes() == before
    results.policy_signature = lambda: "changed-policy"
    assert results.source_signature("M678", AS_OF) != signature
    assert results.read("M678", AS_OF)["state"] == "error"


def test_corrupt_shop_sheet_is_unavailable_without_a_query_fallback(tmp_path):
    results = service(tmp_path)
    publish_all(results)
    sheets = pd.read_excel(results.store.path, sheet_name=None)
    sheets["alerts_ARRAY"]["timekey"] = sheets["alerts_ARRAY"]["timekey"].astype(object)
    sheets["alerts_ARRAY"].loc[0, "timekey"] = "bad-time"
    with pd.ExcelWriter(results.store.path) as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    assert results.read("M678", AS_OF)["state"] == "error"


def test_atomic_publish_failure_preserves_old_success_and_other_shop_sheets(tmp_path, monkeypatch):
    from src.indicator_domain.infrastructure.qtime import alert_workbook

    results = service(tmp_path)
    publish_all(results)
    original = results.store.path.read_bytes()
    monkeypatch.setattr(alert_workbook.os, "replace", lambda *_: (_ for _ in ()).throw(OSError("blocked")))
    with pytest.raises(OSError):
        results.publish("ARRAY", rows(), rows().iloc[:1], HEALTH, AS_OF)
    assert results.store.path.read_bytes() == original
    assert not list(tmp_path.glob(".qtime-alert-*.xlsx"))
