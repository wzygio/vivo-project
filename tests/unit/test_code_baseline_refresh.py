from datetime import datetime
from pathlib import Path

import pandas as pd

from src.yield_domain.application.yield_service import YieldAnalysisService
from src.yield_domain.core.mwd_trend.code_baseline import (
    ensure_code_baseline_current,
    generate_code_baseline,
    load_code_baseline_frame,
    read_code_baseline_metadata,
)
from src.shared_kernel.utils import excel_tools
from yield_domain.core.mwd_trend.mwd_trend_processor import _calc_code_ema_noise


def _write_baseline(path: Path, rows: list[dict], prod_code: str = "PTEST") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    excel_tools.replace_workbook_sheet(path, prod_code, pd.DataFrame(rows))
    excel_tools.replace_workbook_sheet(
        path,
        f"{prod_code}_metadata",
        pd.DataFrame([{"key": "generated_at", "value": "2026-07-01T00:00:00"}]),
    )


def _row_count(result: pd.DataFrame, day: str, code: str = "CodeA") -> int:
    row = result[
        (result["warehousing_time"] == pd.Timestamp(day))
        & (result["defect_desc"] == code)
    ].iloc[0]
    return int(row["defect_panel_count"])


def test_code_ema_starts_each_month_at_first_stable_nonzero_day_rate(
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
                "total_panels": 2000,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-02"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 10,
                "total_panels": 2000,
            },
            {
                "warehousing_time": pd.Timestamp("2026-05-03"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 30,
                "total_panels": 2000,
            },
            {
                "warehousing_time": pd.Timestamp("2026-06-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 20,
                "total_panels": 2000,
            },
            {
                "warehousing_time": pd.Timestamp("2026-06-02"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 40,
                "total_panels": 2000,
            },
        ]
    )

    result = _calc_code_ema_noise(
        raw_daily,
        span=3,
        scale=1.0,
        volatility=0.0,
        prod_code=None,
    )

    assert _row_count(result, "2026-05-01") == 5
    assert _row_count(result, "2026-05-02") == 10
    assert _row_count(result, "2026-05-03") == 20
    assert _row_count(result, "2026-06-01") == 20
    assert _row_count(result, "2026-06-02") == 30
    assert not (tmp_path / "resources" / "codebaseline.xlsx").exists()


def test_zero_code_baseline_uses_first_stable_nonzero_day_rate(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_baseline(
        tmp_path / "resources" / "codebaseline.xlsx",
        [
            {
                "baseline_month": "2026-07",
                "source_month": "2026-06",
                "defect_desc": "CodeA",
                "baseline_rate": 0.0,
                "source_total_panels": 38190,
            }
        ],
    )
    raw_daily = pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-07-01"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 0,
                "total_panels": 2000,
            },
            {
                "warehousing_time": pd.Timestamp("2026-07-02"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 50,
                "total_panels": 500,
            },
            {
                "warehousing_time": pd.Timestamp("2026-07-03"),
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
                "defect_panel_count": 30,
                "total_panels": 3000,
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

    assert _row_count(result, "2026-07-01") == 10
    assert _row_count(result, "2026-07-02") == 5
    assert _row_count(result, "2026-07-03") == 30


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
    assert not (tmp_path / "resources" / "codebaseline.xlsx").exists()


def test_multiplier_change_rebuilds_only_affected_code_baselines(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    baseline_path = tmp_path / "resources" / "codebaseline.xlsx"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    excel_tools.replace_workbook_sheet(
        baseline_path,
        "PTEST",
        pd.DataFrame(
            [
                {
                    "baseline_month": "2026-06",
                    "source_month": "2026-05",
                    "defect_desc": "CodeA",
                    "baseline_rate": 0.1,
                    "source_total_panels": 100,
                },
                {
                    "baseline_month": "2026-06",
                    "source_month": "2026-05",
                    "defect_desc": "CodeB",
                    "baseline_rate": 0.77777,
                    "source_total_panels": 100,
                },
            ]
        ),
    )
    excel_tools.replace_workbook_sheet(
        baseline_path,
        "PTEST_metadata",
        pd.DataFrame(
            [
                {"key": "generated_at", "value": "2026-06-01T00:00:00"},
                {"key": "defect_multipliers_signature", "value": "CodeA=1;CodeB=1"},
            ]
        ),
    )

    current_data = pd.DataFrame(
        [
            {
                "warehousing_time": "2026-05-01",
                "defect_desc": "CodeA",
                "defect_panel_count": 20,
                "total_panels": 100,
            },
            {
                "warehousing_time": "2026-05-01",
                "defect_desc": "CodeB",
                "defect_panel_count": 30,
                "total_panels": 100,
            },
        ]
    )

    baseline = ensure_code_baseline_current(
        current_data,
        "PTEST",
        now=pd.Timestamp("2026-06-15"),
        defect_multipliers_signature="CodeA=2;CodeB=1",
    )

    rates = baseline.set_index("defect_desc")["baseline_rate"].to_dict()
    assert rates == {"CodeA": 0.2, "CodeB": 0.77777}
    assert (
        read_code_baseline_metadata(baseline_path, "PTEST_metadata")["refresh_reason"]
        == "multiplier_changed_codes"
    )


def test_shared_workbook_keeps_other_product_sheets(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    source = pd.DataFrame(
        [
            {
                "warehousing_time": "2026-05-01",
                "defect_desc": "CodeA",
                "defect_panel_count": 10,
                "total_panels": 100,
            }
        ]
    )

    generate_code_baseline(source, "PTEST", generated_at=pd.Timestamp("2026-06-15"))
    generate_code_baseline(source, "POTH", generated_at=pd.Timestamp("2026-06-15"))

    workbook_path = tmp_path / "resources" / "codebaseline.xlsx"
    ptest = load_code_baseline_frame(workbook_path, "PTEST")
    poth = load_code_baseline_frame(workbook_path, "POTH")
    assert ptest["baseline_rate"].tolist() == [0.1]
    assert poth["baseline_rate"].tolist() == [0.1]
    assert (
        read_code_baseline_metadata(workbook_path, "PTEST_metadata")["product_code"]
        == "PTEST"
    )
    assert (
        read_code_baseline_metadata(workbook_path, "POTH_metadata")["product_code"]
        == "POTH"
    )


def test_code_baseline_reads_encrypted_workbook_via_com(tmp_path: Path, monkeypatch) -> None:
    baseline_path = tmp_path / "encrypted_codebaseline.xlsx"
    baseline_path.write_bytes(b"encrypted-placeholder")

    def fail_openpyxl(*args, **kwargs):
        raise ValueError("File is not a zip file")

    def read_via_com(path: Path, sheet_name: str | None = None) -> pd.DataFrame:
        assert path == baseline_path
        if sheet_name == "PTEST_metadata":
            return pd.DataFrame(
                [{"key": "defect_multipliers_signature", "value": "CodeA=1"}]
            )
        return pd.DataFrame(
            [
                {
                    "baseline_month": "2026-06",
                    "source_month": "2026-05",
                    "defect_desc": "CodeA",
                    "baseline_rate": 0.2,
                    "source_total_panels": 100,
                }
            ]
        )

    monkeypatch.setattr(pd, "read_excel", fail_openpyxl)
    monkeypatch.setattr(excel_tools, "_read_encrypted_xlsx_via_com", read_via_com)

    baseline = load_code_baseline_frame(baseline_path, "PTEST")

    assert baseline["baseline_rate"].tolist() == [0.2]
    assert read_code_baseline_metadata(baseline_path, "PTEST_metadata") == {
        "defect_multipliers_signature": "CodeA=1"
    }


def test_yield_time_window_starts_at_first_day_three_months_before_end() -> None:
    original_end = YieldAnalysisService._custom_end_date
    try:
        YieldAnalysisService.set_analysis_end_date(datetime(2026, 7, 3, 15, 30))

        start_dt, end_dt = YieldAnalysisService.get_time_window()

        assert start_dt == datetime(2026, 4, 1, 0, 0)
        assert end_dt == datetime(2026, 7, 3, 15, 30)
    finally:
        YieldAnalysisService._custom_end_date = original_end
