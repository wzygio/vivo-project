import os
from types import SimpleNamespace

import pandas as pd

from src.equipment_domain.infrastructure import data_loader
from src.equipment_domain.infrastructure.data_loader import compact_part_snapshot


def test_snapshot_compaction_keeps_latest_per_key_and_preserves_ttl(tmp_path):
    path = tmp_path / "snapshot.parquet"
    original = pd.DataFrame({
        "step_id": ["S1", "S1", "S1", "S2"],
        "sub_equip_id": ["EQ1"] * 4,
        "param_name": ["P1", "P1", "P2", "P1"],
        "value": [80.0, 0.0, 30.0, 40.0],
        "glass_start_time": pd.to_datetime([
            "2026-09-06", "2026-09-08", "2026-09-07", "2026-09-05",
        ]),
    })
    original.to_parquet(path, index=False)
    os.utime(path, (1700000000, 1700000000))
    mtime = path.stat().st_mtime_ns

    latest = compact_part_snapshot(path)

    assert len(latest) == 3
    assert latest.iloc[0]["value"] == 0.0
    assert path.stat().st_mtime_ns == mtime
    pd.testing.assert_frame_equal(pd.read_parquet(path), latest)
    pd.testing.assert_frame_equal(compact_part_snapshot(path), latest)


def test_manual_parts_refresh_preserves_older_measurements_and_failed_snapshot(tmp_path, monkeypatch):
    today = pd.Timestamp.now().normalize()
    old = pd.DataFrame({
        "step_id": ["S1", "S1"], "sub_equip_id": ["E1", "E1"], "param_name": ["KEEP", "DELETE"],
        "value": [1, 2], "glass_start_time": [today - pd.Timedelta(days=14), today],
    })
    repo = data_loader.PartsRepository(SimpleNamespace(engine=object()), pd.DataFrame(), tmp_path)
    old.to_parquet(repo._snapshot_path, index=False)
    original = repo._snapshot_path.read_bytes()
    calls = []
    empty = old.iloc[:0].copy()
    empty_success = empty.copy()
    empty_success.attrs["parts_source_read"] = {"status": "success"}
    replies = iter([empty, empty_success])

    def query(_db, _spec, *, start, end):
        calls.append((start, end))
        return next(replies)

    monkeypatch.setattr(data_loader, "_query_part_life_from_db", query)
    assert repo.get_snapshot(force_refresh=True, refresh_days=14).empty
    assert repo._snapshot_path.read_bytes() == original
    refreshed = repo.get_snapshot(force_refresh=True, refresh_days=14)
    assert calls == [(today - pd.Timedelta(days=13), today + pd.Timedelta(days=1))] * 2
    assert refreshed.param_name.tolist() == ["KEEP"]
    assert pd.read_parquet(repo._snapshot_path).param_name.tolist() == ["KEEP"]
