"""Daily scheduled results cross process boundaries without bypassing invalidation."""
from datetime import date, datetime, timedelta
import json

import pytest

from app.sections.inline_domain.monitor.alert_matrix_snapshot import DailyMatrixSnapshot
from app.sections.inline_domain.monitor import alert_matrix_cache as cache
from app.sections.inline_domain.monitor import alert_matrix_service as service


def payload(day):
    return {
        "reference_date": day.isoformat(),
        "cells": {("spc_cpk_trend", "M626"): {
            "state": "alert", "detail_key": "spc_cpk_trend|M626",
            "cache_signature": "signature", "cache_revision": "0",
            "computed_at": datetime.combine(day, datetime.min.time()).isoformat(),
            "alert_factories": ["ARRAY"], "message": "",
        }},
    }


def test_roundtrip_and_date_ttl_guard(tmp_path):
    day = date(2026, 9, 11)
    now = datetime(2026, 9, 11, 7, 30)
    store = DailyMatrixSnapshot(tmp_path / "daily.json")
    store.write(payload(day), now=now)
    assert DailyMatrixSnapshot(store.path).read(day, 3600, now=now)["cells"] == payload(day)["cells"]
    assert store.read(day, 3600, now=now + timedelta(hours=2)) is None
    assert store.read(day + timedelta(days=1), 86400, now=now + timedelta(days=1)) is None
    assert store.read(day, 3600, now=now - timedelta(minutes=1)) is None


@pytest.mark.parametrize("content", ["broken", "[]", '{"version": 0}', '{"version": 1, "cells": null}'])
def test_invalid_snapshot_does_not_break_live_query(tmp_path, content):
    store = DailyMatrixSnapshot(tmp_path / "daily.json")
    store.path.write_text(content, encoding="utf-8")
    assert store.read(date.today(), 3600) is None


def test_failed_write_keeps_previous_snapshot(tmp_path, monkeypatch):
    from app.sections.inline_domain.monitor import alert_matrix_snapshot as module
    store = DailyMatrixSnapshot(tmp_path / "daily.json")
    store.write(payload(date.today()))
    before = store.path.read_bytes()
    def fail(*args):
        raise OSError("blocked")
    monkeypatch.setattr(module.os, "replace", fail)
    with pytest.raises(OSError):
        store.write(payload(date.today()))
    assert store.path.read_bytes() == before


def test_saved_error_state_does_not_publish_raw_exception(tmp_path):
    source = payload(date.today())
    source["cells"]["spc_cpk_trend", "M626"].update(state="error", message="driver-private-message")
    store = DailyMatrixSnapshot(tmp_path / "daily.json")
    store.write(source)
    assert "driver-private-message" not in store.path.read_text(encoding="utf-8")
    assert store.read(date.today(), 3600)["cells"]["spc_cpk_trend", "M626"]["state"] == "error"


def test_new_process_cache_reuses_snapshot_but_changed_cell_recomputes(tmp_path, monkeypatch):
    calls = []
    def evaluate(product, context):
        calls.append(product)
        return {"state": "ok", "detail_key": f"test|{product}"}
    row = service.AlertMatrixRow("test", "test", "test", "weekly", evaluate)
    monkeypatch.setattr(service, "MATRIX_ROWS", (row,))
    revisions = {"P1": "0", "P2": "0"}
    store = DailyMatrixSnapshot(tmp_path / "daily.json")
    kwargs = dict(products=("P1", "P2"), reference_date=date.today(),
        _context_factory=lambda: service.AlertMatrixContext(date.today()),
        _signature_provider=lambda *args: "source",
        _revision_provider=lambda key, product: revisions[product], _snapshot_store=store)
    cache._cached_alert_matrix_cell.clear()
    first = cache.get_cached_alert_matrix(**kwargs, reuse_snapshot=False)
    store.write(first)
    cache._cached_alert_matrix_cell.clear()  # 模拟网页进程没有脚本的内存缓存
    second = cache.get_cached_alert_matrix(**kwargs)
    assert calls == ["P1", "P2"]
    assert second["cells"] == first["cells"]
    revisions["P1"] = "changed"
    cache.get_cached_alert_matrix(**kwargs)
    assert calls == ["P1", "P2", "P1"]
    cache._cached_alert_matrix_cell.clear()
