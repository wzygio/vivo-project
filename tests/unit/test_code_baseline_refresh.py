from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.shared_kernel.config_model import AppConfig
from src.yield_domain.core.mwd_trend_processor import (
    _calc_code_ema_noise,
    _ensure_code_baseline_current,
    _load_code_baseline,
    _read_code_baseline_metadata,
)


def _write_baseline(path: Path, generated_at: str, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, sheet_name="Sheet1")
        pd.DataFrame(
            [
                {"key": "generated_at", "value": generated_at},
                {"key": "refresh_reason", "value": "test"},
            ]
        ).to_excel(writer, index=False, sheet_name="_metadata")


def _raw_daily_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-05-01"),
                "defect_desc": "CodeA",
                "defect_panel_count": 2,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-02"),
                "defect_desc": "CodeA",
                "defect_panel_count": 4,
                "total_panels": 100,
            },
        ]
    )


def _mwd_config_with_multipliers() -> AppConfig:
    return AppConfig.model_validate(
        {
            "application": {"cache_ttl_hours": 1},
            "data_source": {"product_code": "PTEST"},
            "processing": {"defect_multipliers": {"CodeA": 1.5}},
        }
    )


def test_expired_code_baseline_rebuilds_from_current_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    baseline_path = tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx"
    _write_baseline(
        baseline_path,
        generated_at="2026-04-01T00:00:00",
        rows=[{"defect_desc": "CodeA", "baseline_rate": 0.9}],
    )

    refreshed = _ensure_code_baseline_current(
        _raw_daily_rows(),
        "PTEST",
        now=datetime(2026, 5, 2),
        max_age_days=30,
    )
    baseline_map = _load_code_baseline("PTEST")

    assert refreshed.loc[0, "defect_desc"] == "CodeA"
    assert baseline_map["CodeA"] == pytest.approx(0.03)


def test_fresh_code_baseline_rebuilds_when_current_window_has_new_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    baseline_path = tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx"
    _write_baseline(
        baseline_path,
        generated_at="2026-05-01T00:00:00",
        rows=[{"defect_desc": "CodeA", "baseline_rate": 0.9}],
    )
    current_rows = pd.concat(
        [
            _raw_daily_rows(),
            pd.DataFrame(
                [
                    {
                        "warehousing_time": pd.Timestamp("2026-05-02"),
                        "defect_desc": "CodeB",
                        "defect_panel_count": 1,
                        "total_panels": 100,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    _ensure_code_baseline_current(
        current_rows,
        "PTEST",
        now=datetime(2026, 5, 10),
        max_age_days=30,
    )
    baseline_map = _load_code_baseline("PTEST")

    assert baseline_map["CodeA"] == pytest.approx(0.03)
    assert baseline_map["CodeB"] == pytest.approx(0.01)


def test_fresh_code_baseline_rebuilds_when_multiplier_signature_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    baseline_path = tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx"
    _write_baseline(
        baseline_path,
        generated_at="2026-05-01T00:00:00",
        rows=[
            {"defect_desc": "CodeA", "baseline_rate": 0.9},
        ],
    )

    refreshed = _ensure_code_baseline_current(
        _raw_daily_rows(),
        "PTEST",
        now=datetime(2026, 5, 10),
        max_age_days=30,
        defect_multipliers_signature="CodeA=1.5",
    )
    metadata = _read_code_baseline_metadata(baseline_path)

    assert refreshed.loc[0, "defect_desc"] == "CodeA"
    assert refreshed.loc[0, "baseline_rate"] == pytest.approx(0.03)
    assert metadata["refresh_reason"] == "multiplier_changed"
    assert metadata["defect_multipliers_signature"] == "CodeA=1.5"


def test_code_ema_passes_multiplier_signature_into_baseline_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    raw_daily = _raw_daily_rows().assign(defect_group="Array_Pixel")

    result = _calc_code_ema_noise(
        raw_daily,
        span=3,
        scale=1.0,
        volatility=0.0,
        config=_mwd_config_with_multipliers(),
        prod_code="PTEST",
    )
    metadata = _read_code_baseline_metadata(
        tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx"
    )

    assert not result.empty
    assert metadata["defect_multipliers_signature"] == "CodeA=1.5"


def test_empty_multiplier_signature_does_not_force_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    baseline_path = tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx"

    _ensure_code_baseline_current(
        _raw_daily_rows(),
        "PTEST",
        now=datetime(2026, 5, 2),
        max_age_days=30,
    )
    _ensure_code_baseline_current(
        _raw_daily_rows(),
        "PTEST",
        now=datetime(2026, 5, 10),
        max_age_days=30,
    )
    metadata = _read_code_baseline_metadata(baseline_path)

    assert metadata["refresh_reason"] == "missing_file"
    assert metadata["defect_multipliers_signature"] == ""
