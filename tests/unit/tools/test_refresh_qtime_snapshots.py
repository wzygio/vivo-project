from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.indicator_domain.application.qtime import cached_monitoring
from src.indicator_domain.application.qtime.dtos import QTimeStepOption
from src.indicator_domain.application.qtime.ports import QTimeSnapshotRefresh
from src.indicator_domain.application.qtime.service import QTimeMonitoringResult
from tools import refresh_qtime_snapshots


@pytest.fixture(autouse=True)
def isolated_monitoring_cache():
    cached_monitoring._cached_monitoring.clear()
    yield
    cached_monitoring._cached_monitoring.clear()


class FakeService:
    def __init__(self, *, refreshed_from_database: bool = True) -> None:
        self.refreshed_from_database = refreshed_from_database
        self.as_of: date | None = None
        self.published = []
        self.decoration_path = None

    def get_filter_options(self, shop):
        return {"step_options": (QTimeStepOption("PATH", "100", "200"),)}

    def get_current_monitoring(self, **kwargs):
        return QTimeMonitoringResult(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None,
        )

    def publish_alert_results(self, result, shop, as_of):
        self.published.append((shop, as_of))

    def refresh_snapshots(
        self,
        *,
        as_of: date | None = None,
    ) -> tuple[QTimeSnapshotRefresh, ...]:
        self.as_of = as_of
        return tuple(
            QTimeSnapshotRefresh(
                shop=shop,  # type: ignore[arg-type]
                row_count=10,
                refreshed_from_database=self.refreshed_from_database,
                source_start="2026-07-28T00:00:00",
                source_end="2026-08-30T00:00:00",
                refreshed_at="2026-09-02T07:00:00+08:00",
            )
            for shop in ("ARRAY", "OLED", "TP")
        )


def test_cli_refreshes_all_shops_for_an_explicit_date(capsys) -> None:
    service = FakeService()

    exit_code = refresh_qtime_snapshots.main(
        ["--as-of", "2026-09-02"],
        service_factory=lambda: service,
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert service.as_of == date(2026, 9, 2)
    assert [item["shop"] for item in payload["snapshots"]] == [
        "ARRAY",
        "OLED",
        "TP",
    ]
    assert payload["status"] == "success"
    assert service.published == [
        (shop, date(2026, 9, 2)) for shop in ("ARRAY", "OLED", "TP")
    ]


def test_cli_returns_nonzero_when_refresh_only_falls_back_to_stale_data(
    capsys,
) -> None:
    service = FakeService(refreshed_from_database=False)

    exit_code = refresh_qtime_snapshots.main([], service_factory=lambda: service)

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["status"] == "stale-fallback"
    assert service.published == []


def test_cli_does_not_claim_success_when_alert_publication_fails(capsys, monkeypatch):
    service = FakeService()

    def fail(*args):
        raise OSError("publication blocked")

    monkeypatch.setattr(service, "publish_alert_results", fail)
    cached_monitoring._cached_monitoring.clear()
    try:
        exit_code = refresh_qtime_snapshots.main([], service_factory=lambda: service)
    finally:
        cached_monitoring._cached_monitoring.clear()

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["status"] == "alert-publication-failed"


def test_windows_task_adapter_defaults_to_daily_0700() -> None:
    script_path = (
        Path(__file__).resolve().parents[3]
        / "tools"
        / "register_qtime_snapshot_task.ps1"
    )
    content = script_path.read_text(encoding="utf-8")

    assert '[string]$At = "07:00"' in content
    assert "New-ScheduledTaskTrigger -Daily -At $At" in content
    assert "refresh_qtime_snapshots.py" in content
