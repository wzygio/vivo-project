"""The scheduled runner publishes full/partial results and preserves old data on failure."""
from datetime import date
import json

import pytest

from app.sections.inline_domain.monitor.alert_matrix_snapshot import DailyMatrixSnapshot
from tools.warm_alert_matrix import main


@pytest.mark.parametrize("state,expected", [("ok", 0), ("error", 1)])
def test_warmup_publishes_results_and_exit_status(tmp_path, state, expected):
    calls = []
    def load(**kwargs):
        calls.append(kwargs)
        return {"reference_date": date.today().isoformat(), "cells": {
            ("test", "P1"): {"state": state, "detail_key": "test|P1", "cache_signature": "s"}}}
    store = DailyMatrixSnapshot(tmp_path / "daily.json")
    assert main([], loader=load, store=store, log_dir=tmp_path / "logs") == expected
    assert calls == [{"reference_date": date.today(), "reuse_snapshot": False}]
    assert store.read(date.today(), 3600)["cells"]["test", "P1"]["state"] == state
    status = json.loads(next((tmp_path / "logs").glob("*.json")).read_text(encoding="utf-8"))
    assert status["status"] == ("success" if expected == 0 else "partial")


def test_fatal_failure_keeps_old_snapshot_and_redacts_error(tmp_path):
    store = DailyMatrixSnapshot(tmp_path / "daily.json")
    store.path.write_text("previous-result", encoding="utf-8")
    def fail(**kwargs):
        raise ConnectionError("private-driver-message")
    assert main([], loader=fail, store=store, log_dir=tmp_path / "logs") == 2
    assert store.path.read_text() == "previous-result"
    report = next((tmp_path / "logs").glob("*.json")).read_text(encoding="utf-8")
    assert "private-driver-message" not in report
    assert json.loads(report)["error_type"] == "ConnectionError"
