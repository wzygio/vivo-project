import os

import pandas as pd

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
