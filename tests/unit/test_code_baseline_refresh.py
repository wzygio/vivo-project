from datetime import datetime
from pathlib import Path

import pandas as pd

from src.yield_domain.application.yield_service import YieldAnalysisService
from src.yield_domain.core.mwd_trend_processor import _calc_code_ema_noise


def _row_count(result: pd.DataFrame, day: str, code: str = "CodeA") -> int:
    row = result[
        (result["warehousing_time"] == pd.Timestamp(day))
        & (result["defect_desc"] == code)
    ].iloc[0]
    return int(row["defect_panel_count"])


def test_code_ema_starts_each_month_at_first_nonzero_day_rate(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    raw_daily = pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-05-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 0,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-02"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 10,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-03"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 30,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-06-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 20,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-06-02"),
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

    assert _row_count(result, "2026-05-01") == 0
    assert _row_count(result, "2026-05-02") == 10
    assert _row_count(result, "2026-05-03") == 10
    assert _row_count(result, "2026-06-01") == 20
    assert _row_count(result, "2026-06-02") == 20
    assert not (tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx").exists()


def test_code_ema_keeps_zero_month_zero(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    raw_daily = pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-05-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 0,
                "total_panels": 100,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-02"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 0,
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

    assert _row_count(result, "2026-05-01") == 0
    assert _row_count(result, "2026-05-02") == 0
    assert not (tmp_path / "resources" / "PTEST" / "PTEST_codebaseline.xlsx").exists()


def test_yield_time_window_starts_at_first_day_three_months_before_end() -> None:
    original_end = YieldAnalysisService._custom_end_date
    try:
        YieldAnalysisService.set_analysis_end_date(datetime(2026, 7, 3, 15, 30))

        start_dt, end_dt = YieldAnalysisService.get_time_window()

        assert start_dt == datetime(2026, 4, 1, 0, 0)
        assert end_dt == datetime(2026, 7, 3, 15, 30)
    finally:
        YieldAnalysisService._custom_end_date = original_end
