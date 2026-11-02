from datetime import date, datetime

import pandas as pd
import pytest

from src.indicator_domain.application.qtime.alert_results import QTimeAlertResults
from src.indicator_domain.infrastructure.qtime.alert_workbook import QTimeAlertWorkbook
from src.shared_kernel.utils.excel_tools import replace_workbook_sheets

DAY = date(2026, 10, 27)
NOW = datetime(2026, 10, 27, 10)
HEALTH = {"status": "fresh", "refreshed_at": "2026-10-27T09:00:00"}


def source_rows():
    return pd.DataFrame([dict(prodcode="P1", f_step="001", t_step="002",
        step_desc="FROM_TO", lot_id="0001", q_spec=1., wait_time=2.,
        over_hours=1., timekey="20261020100000", flag=False)])


def test_publication_retains_explicit_release_evidence_without_public_flag(tmp_path):
    service = QTimeAlertResults(QTimeAlertWorkbook(tmp_path / "qtime.xlsx"),
        ttl_seconds=86400, clock=lambda: NOW)
    rows = source_rows()
    service.publish("ARRAY", rows, rows, HEALTH, DAY)
    result = service.read("P1", DAY, shops=("ARRAY",))
    assert result["release_evidence"] is True
    assert result["release_records"]["flag"].tolist() == [False]
    assert "flag" not in result["alerts"]
    assert "flag" not in result["records"]


@pytest.mark.parametrize("flag", [True, "Delete", None, "invalid"])
def test_publication_rejects_unreleased_rows(tmp_path, flag):
    service = QTimeAlertResults(QTimeAlertWorkbook(tmp_path / "qtime.xlsx"),
        ttl_seconds=86400, clock=lambda: NOW)
    rows = source_rows().assign(flag=flag)
    with pytest.raises(ValueError, match="release"):
        service.publish("ARRAY", rows, rows, HEALTH, DAY)
    assert not service.store.path.exists()


def test_legacy_rows_remain_public_but_are_not_explicit_api_release_evidence(tmp_path):
    service = QTimeAlertResults(QTimeAlertWorkbook(tmp_path / "qtime.xlsx"),
        ttl_seconds=86400, clock=lambda: NOW)
    rows = source_rows()
    service.publish("ARRAY", rows, rows, HEALTH, DAY)
    path = service.store.path
    legacy = pd.read_excel(path, sheet_name="alerts_ARRAY").drop(columns="flag")
    assert replace_workbook_sheets(path, {"alerts_ARRAY": legacy}).written
    before = path.read_bytes()
    result = service.read("P1", DAY, shops=("ARRAY",))
    assert result["state"] == "alert"
    assert result["release_evidence"] is False
    assert result["release_records"].empty
    assert path.read_bytes() == before


def test_qtime_revoke_is_read_only_and_normal_publish_restores_source_decision(tmp_path):
    service = QTimeAlertResults(QTimeAlertWorkbook(tmp_path / "qtime.xlsx"),
        ttl_seconds=86400, clock=lambda: NOW)
    rows = source_rows()
    service.publish("ARRAY", rows, rows, HEALTH, DAY)
    path = service.store.path
    edited = pd.read_excel(path, sheet_name="alerts_ARRAY", dtype=object).assign(flag=True)
    assert replace_workbook_sheets(path, {"alerts_ARRAY": edited}).written
    before = path.read_bytes()
    revoked = service.read("P1", DAY, shops=("ARRAY",))
    assert revoked["state"] == "ok" and revoked["release_records"].empty
    assert path.read_bytes() == before
    service.publish("ARRAY", rows, rows, HEALTH, DAY)
    restored = service.read("P1", DAY, shops=("ARRAY",))
    assert restored["release_records"]["f_step"].tolist() == ["001"]
    assert restored["release_records"]["lot_id"].tolist() == ["0001"]


def test_qtime_missing_release_column_is_rejected_before_writing(tmp_path):
    service = QTimeAlertResults(QTimeAlertWorkbook(tmp_path / "qtime.xlsx"),
        ttl_seconds=86400, clock=lambda: NOW)
    rows = source_rows().drop(columns="flag")
    with pytest.raises(ValueError, match="release"):
        service.publish("ARRAY", rows, rows, HEALTH, DAY)
    assert not service.store.path.exists()
