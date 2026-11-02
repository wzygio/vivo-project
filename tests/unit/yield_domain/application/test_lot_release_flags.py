from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore
from src.shared_kernel.utils.excel_tools import replace_workbook_sheets

DAY = date(2026, 10, 20)
NOW = datetime(2026, 10, 20, 10, tzinfo=timezone(timedelta(hours=8)))
LOT = {"超规 Lot ID": "L1", "异常 Code": "D1", "管控规格线": "1%",
    "实际不良率": "2%", "不良panel数": 2, "入库时间": "2026/10/15", "阵列投入时间": "2026/10/01"}


def service(tmp_path):
    return YieldAlertResults(YieldAlertWorkbookStore({"lot": tmp_path / "lot.xlsx",
        "trend": tmp_path / "trend.xlsx"}), policy_signature=lambda _: "v1",
        ttl_seconds=86400, clock=lambda: NOW)


def publish(results, rows=None, product="P1"):
    return results.publish(product, "lot", [LOT] if rows is None else rows,
        reference_date=DAY, coverage_start="2026-09-01", coverage_end=str(DAY),
        has_data=True, health={"status": "fresh", "refreshed_at": NOW.isoformat()},
        calculation_signature="v1")


def test_new_lot_defaults_denied_and_manual_release_survives_publication(tmp_path):
    results = service(tmp_path)
    publish(results)
    first = results.read("P1", "lot", DAY)
    assert first.release_evidence is True
    assert first.records.empty
    path = results.store.paths["lot"]
    flags = pd.read_excel(path, sheet_name="P1__flags")
    assert flags["flag"].tolist() == [True]
    flags["flag"] = False
    assert replace_workbook_sheets(path, {"P1__flags": flags}).written
    before = path.read_bytes()
    released = results.read("P1", "lot", DAY)
    assert released.records["超规 Lot ID"].tolist() == ["L1"]
    assert released.release_records["flag"].tolist() == [False]
    assert "flag" not in released.records
    assert path.read_bytes() == before
    publish(results)
    assert results.read("P1", "lot", DAY).records["超规 Lot ID"].tolist() == ["L1"]


@pytest.mark.parametrize("flag", [True, "Delete", None, "invalid", ""])
def test_nonfalse_decisions_revoke_without_republication_or_rewriting(tmp_path, flag):
    results = service(tmp_path)
    publish(results)
    path = results.store.paths["lot"]
    flags = pd.read_excel(path, sheet_name="P1__flags").assign(flag=flag)
    assert replace_workbook_sheets(path, {"P1__flags": flags}).written
    before = path.read_bytes()
    result = results.read("P1", "lot", DAY)
    assert result.release_evidence is True and result.records.empty
    assert path.read_bytes() == before


def test_history_other_products_and_manual_sheets_survive_republication(tmp_path):
    results = service(tmp_path)
    publish(results)
    publish(results, product="P2")
    path = results.store.paths["lot"]
    flags = pd.read_excel(path, sheet_name="P1__flags").assign(flag=False)
    note = pd.DataFrame([{"note": "maintained"}])
    assert replace_workbook_sheets(path, {"P1__flags": flags, "manual": note}).written
    second = {**LOT, "超规 Lot ID": "L2"}
    publish(results, rows=[second])
    history = pd.read_excel(path, sheet_name="P1__flags")
    assert dict(zip(history["超规 Lot ID"], history["flag"])) == {"L1": False, "L2": True}
    assert results.read("P1", "lot", DAY).records.empty
    assert results.read("P2", "lot", DAY).release_evidence is True
    assert pd.read_excel(path, sheet_name="manual").to_dict("records") == [{"note": "maintained"}]
    publish(results, rows=[LOT])
    assert results.read("P1", "lot", DAY).records["超规 Lot ID"].tolist() == ["L1"]
    assert len(pd.read_excel(path, sheet_name="P1__flags")) == 2


def make_legacy(results):
    path = results.store.paths["lot"]
    sheets = pd.read_excel(path, sheet_name=None, dtype=object)
    sheets["P1"] = sheets["P1"].drop(columns="flag")
    sheets["P1__meta"] = sheets["P1__meta"].drop(columns="release_schema_version")
    del sheets["P1__flags"]
    with pd.ExcelWriter(path) as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    return path


def test_legacy_is_not_silently_released_and_explicit_initialization_preserves_age(tmp_path):
    results = service(tmp_path)
    publish(results)
    path = make_legacy(results)
    before = path.read_bytes()
    legacy = results.read("P1", "lot", DAY)
    assert legacy.release_evidence is False
    assert legacy.records["超规 Lot ID"].tolist() == ["L1"]  # Existing page compatibility only.
    assert path.read_bytes() == before
    generated_at = legacy.metadata["generated_at"]
    assert results.store.initialize_lot_controls("P1")
    assert not results.store.initialize_lot_controls("P1")
    migrated = results.read("P1", "lot", DAY)
    assert migrated.release_evidence is True and migrated.records.empty
    assert migrated.metadata["generated_at"] == generated_at


def test_controlled_publication_cannot_lose_its_decisions_or_have_duplicate_keys(tmp_path):
    results = service(tmp_path)
    publish(results)
    path = results.store.paths["lot"]
    flags = pd.read_excel(path, sheet_name="P1__flags")
    duplicate = pd.concat([flags, flags.assign(flag=False)], ignore_index=True)
    assert replace_workbook_sheets(path, {"P1__flags": duplicate}).written
    before = path.read_bytes()
    with pytest.raises(ValueError, match="Duplicate"):
        results.read("P1", "lot", DAY)
    assert path.read_bytes() == before


def test_removing_control_sheet_does_not_downgrade_a_controlled_publication_to_legacy(tmp_path):
    results = service(tmp_path)
    publish(results)
    path = results.store.paths["lot"]
    sheets = pd.read_excel(path, sheet_name=None, dtype=object)
    del sheets["P1__flags"]
    with pd.ExcelWriter(path) as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="Missing Yield release controls"):
        results.read("P1", "lot", DAY)
    assert path.read_bytes() == before


def test_failed_release_commit_preserves_original_bytes(tmp_path, monkeypatch):
    from src.yield_domain.infrastructure import alert_result_workbooks

    results = service(tmp_path)
    publish(results)
    path = make_legacy(results)
    before = path.read_bytes()
    monkeypatch.setattr(alert_result_workbooks, "replace_workbook_sheets",
        lambda *_: type("Failure", (), {"written": False})())
    with pytest.raises(OSError):
        results.store.initialize_lot_controls("P1")
    assert path.read_bytes() == before
