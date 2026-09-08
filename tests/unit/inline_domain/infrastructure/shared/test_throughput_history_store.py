from __future__ import annotations

import multiprocessing
import time
from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.infrastructure.shared.throughput_history_store import (
    ThroughputHistoryStore,
)


def _facts(day: str, item_id: str) -> pd.DataFrame:
    return pd.DataFrame(
        [{"scope": "spc", "factory": "ARRAY", "prod_code": "M626", "event_date": day, "item_id": item_id}]
    )


def _concurrent_update(snapshot_dir: str, day: str, item_id: str, start) -> None:
    store = ThroughputHistoryStore(Path(snapshot_dir))
    original_read = store.read

    def slow_read(scope: str, prod_code: str) -> pd.DataFrame:
        result = original_read(scope, prod_code)
        time.sleep(0.2)
        return result

    store.read = slow_read  # type: ignore[method-assign]
    start.wait(timeout=5)
    store.update(
        "spc", "M626", _facts(day, item_id),
        coverage_start=pd.Timestamp(day),
        coverage_end=pd.Timestamp(day) + pd.Timedelta(days=1),
    )


def test_update_replaces_only_covered_days(tmp_path) -> None:
    store = ThroughputHistoryStore(tmp_path)
    store.update(
        "spc", "M626", _facts("2026-08-01", "S1"),
        coverage_start=pd.Timestamp("2026-08-01"),
        coverage_end=pd.Timestamp("2026-08-02"),
    )
    result = store.update(
        "spc", "M626", _facts("2026-08-02", "S2"),
        coverage_start=pd.Timestamp("2026-08-02"),
        coverage_end=pd.Timestamp("2026-08-03"),
    )

    assert result["item_id"].tolist() == ["S1", "S2"]


def test_snapshot_path_rejects_unknown_scope(tmp_path) -> None:
    store = ThroughputHistoryStore(tmp_path)

    with pytest.raises(ValueError, match="unsupported throughput scope"):
        store.snapshot_path("../outside", "M626")


def test_two_processes_keep_both_coverage_updates(tmp_path) -> None:
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    workers = [
        context.Process(
            target=_concurrent_update,
            args=(str(tmp_path), day, item_id, start),
        )
        for day, item_id in (("2026-08-01", "S1"), ("2026-08-02", "S2"))
    ]
    for worker in workers:
        worker.start()
    start.set()
    for worker in workers:
        worker.join(timeout=10)
        assert worker.exitcode == 0

    result = ThroughputHistoryStore(tmp_path).read("spc", "M626")
    assert result["item_id"].tolist() == ["S1", "S2"]
