from __future__ import annotations

from dataclasses import replace
import os

import pandas as pd
import pytest

from src.equipment_domain.config import get_equipment_runtime_config
from src.equipment_domain.infrastructure import data_loader
from src.equipment_domain.infrastructure.fake_data import (
    FabricationPolicy,
    build_fabricated_snapshot_path,
    generate_fabricated_snapshot,
    write_fabricated_snapshot,
)
from src.equipment_domain.infrastructure.fake_data_updater import (
    ensure_fabricated_snapshot_file,
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


def test_update_advances_with_a_deterministic_near_arithmetic_sequence() -> None:
    original = pd.DataFrame(
        {
            "step_id": ["S1", "S2", "S3"],
            "sub_equip_id": ["EQ1-PM3", "EQ2-PM4", "EQ3-PM5"],
            "param_name": [
                "P3_TRGTLIFE_G_MAX",
                "P4_MASKLIFE_G_MAX",
                "PM5_1_PRE_SPRT_KWH",
            ],
            "value": [20.0, 60.0, 97.0],
            "glass_start_time": pd.to_datetime(
                ["2026-07-18 08:00:00", "2026-07-18 09:00:00", "2026-07-18 10:00:00"]
            ),
        }
    )

    result = update_fabricated_snapshot(original, _specs(), _policy())
    repeated = update_fabricated_snapshot(original, _specs(), _policy())

    assert result.snapshot_df[["step_id", "sub_equip_id", "param_name"]].equals(
        original[["step_id", "sub_equip_id", "param_name"]]
    )
    assert result.snapshot_df["glass_start_time"].equals(
        original["glass_start_time"] + pd.Timedelta(days=1)
    )
    increments = (
        result.snapshot_df["value"].to_numpy()
        - original["value"].to_numpy()
    ) % 100.0
    assert all(28.5 <= increment <= 31.5 for increment in increments)
    assert len({round(increment, 6) for increment in increments}) > 1
    assert 25.5 <= result.snapshot_df.loc[2, "value"] <= 28.5
    pd.testing.assert_frame_equal(result.snapshot_df, repeated.snapshot_df)
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

    expired_mtime = (now - pd.Timedelta(hours=49)).to_pydatetime().timestamp()
    os.utime(snapshot_path, (expired_mtime, expired_mtime))
    expired = update_fabricated_snapshot_file(
        _specs(),
        _policy(),
        output_dir=tmp_path,
        now=now,
    )
    assert expired.updated is True
    assert (pd.read_parquet(snapshot_path)["glass_start_time"] == (
        generated.snapshot_df["glass_start_time"] + pd.Timedelta(days=2)
    )).all()
    assert expired.summary["update_periods"] == 2

    forced = update_fabricated_snapshot_file(
        _specs(),
        _policy(),
        output_dir=tmp_path,
        now=now,
        force=True,
    )
    assert forced.updated is True
    assert forced.summary["reason"] == "forced"


def test_ensure_bootstraps_missing_snapshot_and_loader_maintains_it_automatically(
    tmp_path,
    monkeypatch,
) -> None:
    now = pd.Timestamp("2026-07-21 12:00:00")

    created = ensure_fabricated_snapshot_file(
        _specs(),
        _policy(),
        output_dir=tmp_path,
        now=now,
    )

    assert created.created is True
    assert created.updated is False
    assert created.summary["reason"] == "snapshot-created"
    assert created.path == build_fabricated_snapshot_path(_specs(), tmp_path)

    runtime = replace(
        get_equipment_runtime_config(),
        snapshot_dir=tmp_path,
        fabrication_policy=_policy(),
    )
    monkeypatch.setattr(data_loader, "get_equipment_runtime_config", lambda: runtime)
    stale_mtime = (now - pd.Timedelta(hours=25)).to_pydatetime().timestamp()
    os.utime(created.path, (stale_mtime, stale_mtime))
    before = pd.read_parquet(created.path)

    loaded = data_loader.load_fabricated_part_life_snapshot(
        _specs(),
        now=now,
    )

    assert (loaded["glass_start_time"] == before["glass_start_time"] + pd.Timedelta(days=1)).all()


def test_report_snapshot_boundary_filters_source_time_before_display_shift(
    monkeypatch,
) -> None:
    as_of = pd.Timestamp("2026-08-10 12:00:00")
    source_real = pd.DataFrame(
        {
            "glass_start_time": [pd.Timestamp("2026-08-08 08:00:00")],
            "value": [42.0],
        }
    )
    source_fabricated = pd.DataFrame(
        {
            "glass_start_time": [pd.Timestamp("2026-08-09 09:00:00")],
            "value": [24.0],
        }
    )
    filtered_times: list[pd.Timestamp] = []

    monkeypatch.setattr(
        data_loader,
        "load_part_life_snapshot",
        lambda _db, _specs: source_real.copy(),
    )
    monkeypatch.setattr(
        data_loader,
        "load_fabricated_part_life_snapshot",
        lambda _specs, *, now: source_fabricated.copy(),
    )

    def capture_source_filter(
        frame: pd.DataFrame,
        *,
        as_of: pd.Timestamp,
        max_age_days: int,
    ) -> pd.DataFrame:
        del as_of, max_age_days
        filtered_times.extend(frame["glass_start_time"].tolist())
        return frame.copy()

    monkeypatch.setattr(
        data_loader,
        "filter_recent_part_life_measurements",
        capture_source_filter,
    )

    displayed_real, displayed_fabricated = (
        data_loader.load_report_part_life_snapshots(
            object(),
            _specs(),
            as_of=as_of,
            max_age_days=3,
        )
    )

    assert filtered_times == [pd.Timestamp("2026-08-08 08:00:00")]
    assert displayed_real.loc[0, "glass_start_time"] == pd.Timestamp(
        "2026-08-12 08:00:00"
    )
    assert displayed_fabricated.loc[0, "glass_start_time"] == pd.Timestamp(
        "2026-08-13 09:00:00"
    )
    assert source_real.loc[0, "glass_start_time"] == pd.Timestamp(
        "2026-08-08 08:00:00"
    )
    assert source_fabricated.loc[0, "glass_start_time"] == pd.Timestamp(
        "2026-08-09 09:00:00"
    )


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
