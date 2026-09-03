from __future__ import annotations

import pandas as pd
import pytest

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.snapshot_window import snapshot_window_start


def test_enabled_policy_shifts_manufacturing_time_without_mutating_source() -> None:
    source = pd.DataFrame(
        {
            "start_time": [pd.Timestamp("2026-08-29 10:30:00")],
            "value": [12.5],
        }
    )

    result = DataForwardPolicy(enabled=True, offset_days=4).shift_frame(
        source,
        ("start_time",),
    )

    assert result.loc[0, "start_time"] == pd.Timestamp("2026-09-02 10:30:00")
    assert source.loc[0, "start_time"] == pd.Timestamp("2026-08-29 10:30:00")
    assert result.loc[0, "value"] == 12.5


def test_policy_rejects_negative_offsets() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        DataForwardPolicy(enabled=True, offset_days=-1)


def test_config_loader_reads_enabled_data_forward_policy(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "global.yaml").write_text(
        "data_forward:\n  enabled: true\n  offset_days: 4\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    policy = ConfigLoader.get_data_forward_policy()

    assert policy == DataForwardPolicy(enabled=True, offset_days=4)


def test_policy_translates_display_window_to_source_time() -> None:
    policy = DataForwardPolicy(enabled=True, offset_days=4)

    start, end = policy.to_source_window(
        pd.Timestamp("2026-09-01 00:00:00"),
        pd.Timestamp("2026-09-03 00:00:00"),
    )

    assert start == pd.Timestamp("2026-08-28 00:00:00")
    assert end == pd.Timestamp("2026-08-30 00:00:00")


@pytest.mark.parametrize(
    ("display_end", "expected"),
    [
        ("2026-09-02", "2026-06-01"),
        ("2026-10-01", "2026-07-01"),
    ],
)
def test_snapshot_window_always_starts_at_third_prior_month_first_day(
    display_end: str,
    expected: str,
) -> None:
    assert snapshot_window_start(display_end) == pd.Timestamp(expected)


def test_policy_signature_changes_with_mode_and_offset() -> None:
    signatures = {
        DataForwardPolicy(enabled=False, offset_days=4).signature,
        DataForwardPolicy(enabled=True, offset_days=4).signature,
        DataForwardPolicy(enabled=True, offset_days=5).signature,
    }

    assert len(signatures) == 3


def test_disabled_policy_preserves_time_values_and_dtype() -> None:
    source = pd.DataFrame({"start_time": ["2026-08-29 10:30:00"]})

    result = DataForwardPolicy(enabled=False, offset_days=4).shift_frame(
        source,
        ("start_time",),
    )

    pd.testing.assert_frame_equal(result, source)


def test_missing_global_policy_defaults_to_disabled(tmp_path, monkeypatch) -> None:
    (tmp_path / "config").mkdir()
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    assert ConfigLoader.get_data_forward_policy() == DataForwardPolicy(
        enabled=False,
        offset_days=4,
    )


def test_invalid_enabled_value_is_rejected(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "global.yaml").write_text(
        'data_forward:\n  enabled: "true"\n  offset_days: 4\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    with pytest.raises(ValueError, match="boolean"):
        ConfigLoader.get_data_forward_policy()
