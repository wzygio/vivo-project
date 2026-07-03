from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.shared_kernel.config_model import AppConfig
from src.yield_domain.application.yield_service import YieldAnalysisService
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
                "warehousing_time": pd.Timestamp("2026-04-01"),
                "defect_desc": "CodeA",
                "defect_panel_count": 2,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-04-02"),
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


def test_code_ema_uses_previous_month_baseline_for_each_month(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    raw_daily = pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-04-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 10,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 0,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-06-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 40,
                "total_panels": 100,
            },
        ]
    )

    result = _calc_code_ema_noise(
        raw_daily,
        span=3,
        scale=1.0,
        volatility=0.0,
        prod_code="PTEST",
    )

    may_first = result[
        (result["warehousing_time"] == pd.Timestamp("2026-05-01"))
        & (result["defect_desc"] == "CodeA")
    ].iloc[0]
    june_first = result[
        (result["warehousing_time"] == pd.Timestamp("2026-06-01"))
        & (result["defect_desc"] == "CodeA")
    ].iloc[0]

    assert may_first["defect_panel_count"] == 5
    assert june_first["defect_panel_count"] == 20


def test_legacy_code_baseline_migrates_to_period_scoped_current_window(
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


def test_existing_period_baseline_rows_are_not_rewritten_when_new_month_is_added(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    baseline_path = tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx"
    _write_baseline(
        baseline_path,
        generated_at="2026-06-01T00:00:00",
        rows=[
            {
                "baseline_month": "2026-05",
                "source_month": "2026-04",
                "defect_desc": "CodeA",
                "baseline_rate": 0.9,
            }
        ],
    )
    current_rows = pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-04-01"),
                "defect_desc": "CodeA",
                "defect_panel_count": 10,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-01"),
                "defect_desc": "CodeA",
                "defect_panel_count": 2,
                "total_panels": 100,
            },
        ]
    )

    _ensure_code_baseline_current(
        current_rows,
        "PTEST",
        now=datetime(2026, 6, 2),
        max_age_days=30,
    )
    refreshed = pd.read_excel(baseline_path, sheet_name="Sheet1", engine="openpyxl")
    metadata = _read_code_baseline_metadata(baseline_path)

    may_row = refreshed[
        (refreshed["baseline_month"] == "2026-05")
        & (refreshed["defect_desc"] == "CodeA")
    ].iloc[0]
    june_row = refreshed[
        (refreshed["baseline_month"] == "2026-06")
        & (refreshed["defect_desc"] == "CodeA")
    ].iloc[0]

    assert may_row["baseline_rate"] == pytest.approx(0.9)
    assert june_row["baseline_rate"] == pytest.approx(0.02)
    assert metadata["refresh_reason"] == "missing_period_rows"


def test_code_baseline_only_generates_through_current_month(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    baseline_path = tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx"
    current_rows = pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-06-01"),
                "defect_desc": "CodeA",
                "defect_panel_count": 3,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-07-01"),
                "defect_desc": "CodeA",
                "defect_panel_count": 50,
                "total_panels": 100,
            },
        ]
    )

    _ensure_code_baseline_current(
        current_rows,
        "PTEST",
        now=datetime(2026, 7, 3),
        max_age_days=30,
    )
    refreshed = pd.read_excel(baseline_path, sheet_name="Sheet1", engine="openpyxl")

    assert refreshed["baseline_month"].tolist() == ["2026-07"]
    assert refreshed.loc[0, "source_month"] == "2026-06"
    assert refreshed.loc[0, "baseline_rate"] == pytest.approx(0.03)


def test_code_ema_uses_current_month_mean_when_previous_month_had_no_shipments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    raw_daily = pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-04-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 0,
                "total_panels": 0,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 20,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-02"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 20,
                "total_panels": 100,
            },
        ]
    )

    result = _calc_code_ema_noise(
        raw_daily,
        span=3,
        scale=1.0,
        volatility=0.0,
        prod_code="PTEST",
    )
    baseline = pd.read_excel(
        tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx",
        sheet_name="Sheet1",
        engine="openpyxl",
    )
    may_first = result[
        (result["warehousing_time"] == pd.Timestamp("2026-05-01"))
        & (result["defect_desc"] == "CodeA")
    ].iloc[0]

    assert baseline.loc[0, "baseline_month"] == "2026-05"
    assert baseline.loc[0, "baseline_rate"] == pytest.approx(0.0)
    assert baseline.loc[0, "source_total_panels"] == 0
    assert may_first["defect_panel_count"] == 20


def test_code_ema_treats_legacy_zero_baseline_as_unusable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    baseline_path = tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx"
    _write_baseline(
        baseline_path,
        generated_at="2026-05-01T00:00:00",
        rows=[
            {
                "baseline_month": "2026-05",
                "source_month": "2026-04",
                "defect_desc": "CodeA",
                "baseline_rate": 0.0,
            }
        ],
    )
    raw_daily = pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-05-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 20,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-02"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 20,
                "total_panels": 100,
            },
        ]
    )

    result = _calc_code_ema_noise(
        raw_daily,
        span=3,
        scale=1.0,
        volatility=0.0,
        prod_code="PTEST",
    )
    may_first = result[
        (result["warehousing_time"] == pd.Timestamp("2026-05-01"))
        & (result["defect_desc"] == "CodeA")
    ].iloc[0]

    assert may_first["defect_panel_count"] == 20


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
                        "warehousing_time": pd.Timestamp("2026-04-02"),
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
    raw_daily = pd.concat(
        [
            _raw_daily_rows().assign(defect_group="Array_Pixel"),
            pd.DataFrame(
                [
                    {
                        "warehousing_time": pd.Timestamp("2026-05-01"),
                        "defect_group": "Array_Pixel",
                        "defect_desc": "CodeA",
                        "defect_panel_count": 0,
                        "total_panels": 100,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

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


def test_yield_time_window_starts_at_first_day_three_months_before_end() -> None:
    original_end = YieldAnalysisService._custom_end_date
    try:
        YieldAnalysisService.set_analysis_end_date(datetime(2026, 7, 3, 15, 30))

        start_dt, end_dt = YieldAnalysisService.get_time_window()

        assert start_dt == datetime(2026, 4, 1, 0, 0)
        assert end_dt == datetime(2026, 7, 3, 15, 30)
    finally:
        YieldAnalysisService._custom_end_date = original_end
