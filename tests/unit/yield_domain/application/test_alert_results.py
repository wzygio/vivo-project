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
    assert len(cold.read("P1", "lot", DAY).records) == 1
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
