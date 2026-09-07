from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.infrastructure.shared.oos_history_store import (
    OosHistoryReadError,
    OosHistoryStore,
)


def _spc(rows: list[tuple[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "step_id": "1100",
                "param_name": "PPA",
                "sheet_id": sheet_id,
                "sheet_start_time": timestamp,
                "sheet_max": 3.0,
                "sheet_min": 1.0,
                "sheet_mean": 2.0,
                "usl": 2.5,
                "lsl": 1.5,
                "oos_type": "USL",
                "flag": False,
            }
            for sheet_id, timestamp in rows
        ]
    )


def test_update_replaces_overlap_and_preserves_history(tmp_path: Path) -> None:
    store = OosHistoryStore(tmp_path)
    store.update(
        "spc",
        "M626",
        _spc([("OLD", "2026-07-10"), ("REPLACED", "2026-08-10")]),
        coverage_start=pd.Timestamp("2026-07-01"),
        coverage_end=pd.Timestamp("2026-09-01"),
        refreshed_at=datetime(2026, 9, 1, 8),
    )

    snapshot = store.update(
        "spc",
        "M626",
        _spc([("NEW", "2026-08-12")]),
        coverage_start=pd.Timestamp("2026-08-01"),
        coverage_end=pd.Timestamp("2026-09-01"),
        refreshed_at=datetime(2026, 9, 2, 8),
    )

    assert snapshot.frame["sheet_id"].tolist() == ["OLD", "NEW"]
    assert "flag" not in snapshot.frame.columns
    assert snapshot.metadata.row_count == 2
    assert snapshot.metadata.coverage_start == "2026-08-01T00:00:00"


def test_empty_successful_window_clears_stale_rows_and_updates_metadata(
    tmp_path: Path,
) -> None:
    store = OosHistoryStore(tmp_path)
    store.update(
        "spc",
        "M626",
        _spc([("STALE", "2026-08-10")]),
        coverage_start=pd.Timestamp("2026-08-01"),
        coverage_end=pd.Timestamp("2026-09-01"),
        refreshed_at=datetime(2026, 9, 1, 8),
    )

    snapshot = store.update(
        "spc",
        "M626",
        pd.DataFrame(),
        coverage_start=pd.Timestamp("2026-08-01"),
        coverage_end=pd.Timestamp("2026-09-01"),
        refreshed_at=datetime(2026, 9, 2, 8),
    )

    assert snapshot.frame.empty
    assert snapshot.metadata.refreshed_at == "2026-09-02T08:00:00"
    assert snapshot.metadata.row_count == 0


def test_corrupt_existing_snapshot_is_not_silently_overwritten(tmp_path: Path) -> None:
    store = OosHistoryStore(tmp_path)
    path = store.snapshot_path("spc", "M626")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not parquet")

    with pytest.raises(OosHistoryReadError):
        store.update(
            "spc",
            "M626",
            _spc([("NEW", "2026-08-12")]),
            coverage_start=pd.Timestamp("2026-08-01"),
            coverage_end=pd.Timestamp("2026-09-01"),
            refreshed_at=datetime(2026, 9, 2, 8),
        )

    assert path.read_bytes() == b"not parquet"


@pytest.mark.parametrize("scope", ["spc", "ctq", "aoi_tt", "aoi_rs"])
def test_all_supported_scopes_can_persist_empty_refresh(scope: str, tmp_path: Path) -> None:
    snapshot = OosHistoryStore(tmp_path).update(
        scope,
        "M626",
        pd.DataFrame(),
        coverage_start=pd.Timestamp("2026-08-01"),
        coverage_end=pd.Timestamp("2026-09-01"),
        refreshed_at=datetime(2026, 9, 2, 8),
    )

    assert snapshot.frame.empty
    assert snapshot.metadata.scope == scope
