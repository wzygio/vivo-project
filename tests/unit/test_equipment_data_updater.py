from __future__ import annotations

import os

import pandas as pd
import pytest

from src.equipment_domain.infrastructure.fake_data import (
    FabricationPolicy,
    generate_fabricated_snapshot,
    write_fabricated_snapshot,
)
from src.equipment_domain.infrastructure.fake_data_updater import (
    update_fabricated_snapshot,
    update_fabricated_snapshot_file,
)


def _policy() -> FabricationPolicy:
    return FabricationPolicy(
        random_seed=20260715,
        initial_value_ratio_range=(0.0, 1.0),
        initial_lookback_days=2,
        update_increment_ratio=0.30,
        reset_ratio_range=(0.0, 0.30),
        snapshot_ttl_hours=24,
    )


def _specs() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ["Array", "Target", "PVD", "MO", "DEPO", 100.0, "S1", "EQ1-PM3", "%TRGTLIFE%_G_MAX"],
            ["Array", "Mask", "PVD", "MO", "DEPO", 100.0, "S2", "EQ2-PM4", "%MASKLIFE%_G_MAX"],
            ["Array", "Power", "PVD", "MO", "DEPO", 100.0, "S3", "EQ3-PM5", "%PRE_SPRT_KWH%"],
        ],
        columns=[
            "厂别", "备件类型", "设备类型", "膜层", "制程", "寿命规格",
            "站点", "机台号-腔室", "参数名称",
        ],
    )


def test_update_advances_one_day_adds_thirty_percent_and_resets_crossed_values() -> None:
    original = pd.DataFrame(
        {
            "step_id": ["S1", "S2", "S3"],
            "sub_equip_id": ["EQ1-PM3", "EQ2-PM4", "EQ3-PM5"],
            "param_name": [
                "P3_TRGTLIFE_G_MAX",
                "P4_MASKLIFE_G_MAX",
                "PM5_1_PRE_SPRT_KWH",
            ],
            "value": [20.0, 70.0, 80.0],
            "glass_start_time": pd.to_datetime(
                ["2026-07-18 08:00:00", "2026-07-18 09:00:00", "2026-07-18 10:00:00"]
            ),
        }
    )

    result = update_fabricated_snapshot(original, _specs(), _policy())

    assert result.snapshot_df[["step_id", "sub_equip_id", "param_name"]].equals(
        original[["step_id", "sub_equip_id", "param_name"]]
    )
    assert result.snapshot_df["glass_start_time"].equals(
        original["glass_start_time"] + pd.Timedelta(days=1)
    )
    assert result.snapshot_df.loc[0, "value"] == 50.0
    assert result.snapshot_df.loc[1, "value"] == 100.0
    assert 0.0 <= result.snapshot_df.loc[2, "value"] <= 30.0
    assert result.summary["updated_rows"] == 3
    assert result.summary["reset_rows"] == 1


def test_file_update_skips_fresh_snapshot_updates_at_ttl_and_allows_force(tmp_path) -> None:
    now = pd.Timestamp("2026-07-21 12:00:00")
    generated = generate_fabricated_snapshot(_specs(), _policy(), as_of=now)
    snapshot_path = write_fabricated_snapshot(
        generated.snapshot_df,
        _specs(),
        output_dir=tmp_path,
    )
    fresh_mtime = (now - pd.Timedelta(hours=23)).to_pydatetime().timestamp()
    os.utime(snapshot_path, (fresh_mtime, fresh_mtime))

    skipped = update_fabricated_snapshot_file(
        _specs(),
        _policy(),
        output_dir=tmp_path,
        now=now,
    )
    assert skipped.updated is False
    pd.testing.assert_frame_equal(pd.read_parquet(snapshot_path), generated.snapshot_df)

    expired_mtime = (now - pd.Timedelta(hours=24)).to_pydatetime().timestamp()
    os.utime(snapshot_path, (expired_mtime, expired_mtime))
    expired = update_fabricated_snapshot_file(
        _specs(),
        _policy(),
        output_dir=tmp_path,
        now=now,
    )
    assert expired.updated is True
    assert (pd.read_parquet(snapshot_path)["glass_start_time"] == (
        generated.snapshot_df["glass_start_time"] + pd.Timedelta(days=1)
    )).all()

    forced = update_fabricated_snapshot_file(
        _specs(),
        _policy(),
        output_dir=tmp_path,
        now=now,
        force=True,
    )
    assert forced.updated is True
    assert forced.summary["reason"] == "forced"


def test_update_rejects_malformed_and_unmappable_snapshots() -> None:
    malformed = pd.DataFrame({"step_id": ["S1"]})
    with pytest.raises(ValueError, match="missing fabricated snapshot columns"):
        update_fabricated_snapshot(malformed, _specs(), _policy())

    unmappable = pd.DataFrame({
        "step_id": ["UNKNOWN"],
        "sub_equip_id": ["UNKNOWN-PM1"],
        "param_name": ["UNKNOWN_PARAM"],
        "value": [1.0],
        "glass_start_time": pd.to_datetime(["2026-07-18 08:00:00"]),
    })
    with pytest.raises(ValueError, match="keys missing from specifications"):
        update_fabricated_snapshot(unmappable, _specs(), _policy())
