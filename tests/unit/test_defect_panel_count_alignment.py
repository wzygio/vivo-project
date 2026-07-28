import pandas as pd

from src.shared_kernel.config_model import AppConfig
from yield_domain.core.mwd_trend import mwd_trend_processor as trend_module
from yield_domain.core.mwd_trend.mwd_trend_processor import MWDTrendProcessor


def test_code_formatter_exposes_full_weekly_history_but_keeps_ui_at_three_weeks() -> None:
    weekly = pd.DataFrame(
        {
            "warehousing_time": pd.date_range(
                "2026-05-24",
                periods=6,
                freq="7D",
            ),
            "total_panels": [10_000] * 6,
            "defect_group": ["Array_Line"] * 6,
            "defect_desc": ["CodeA"] * 6,
            "defect_panel_count": [1, 2, 3, 4, 5, 6],
        }
    )

    result = trend_module._format_code_results(
        monthly=pd.DataFrame(),
        weekly=weekly,
        daily=pd.DataFrame(),
    )

    assert result["weekly"]["time_period"].astype(str).unique().tolist() == [
        "2026-W24",
        "2026-W25",
        "2026-W26",
    ]
    assert result["weekly_full"]["time_period"].astype(str).unique().tolist() == [
        "2026-W21",
        "2026-W22",
        "2026-W23",
        "2026-W24",
        "2026-W25",
        "2026-W26",
    ]


def test_code_daily_counts_are_reconciled_to_raw_monthly_integer_total() -> None:
    daily = pd.DataFrame(
        {
            "warehousing_time": pd.to_datetime(
                ["2026-05-01", "2026-05-02", "2026-05-03"]
            ),
            "total_panels": [100, 100, 100],
            "defect_group": ["Array_Pixel"] * 3,
            "defect_desc": ["CodeA"] * 3,
            "defect_panel_count": [5, 3, 2],
        }
    )
    raw_daily = daily.copy()
    raw_daily["defect_panel_count"] = [2, 1, 1]

    result = MWDTrendProcessor.reconcile_code_daily_counts(
        daily,
        raw_daily,
    )

    assert result["defect_panel_count"].tolist() == [2, 1, 1]
    assert result["defect_panel_count"].sum() == 4
    assert pd.api.types.is_integer_dtype(result["defect_panel_count"])


def test_zero_ema_total_falls_back_to_daily_input_weights() -> None:
    daily = pd.DataFrame(
        {
            "warehousing_time": pd.to_datetime(
                ["2026-06-01", "2026-06-02", "2026-06-03"]
            ),
            "total_panels": [0, 100, 200],
            "defect_group": ["Array_Line"] * 3,
            "defect_desc": ["CodeB"] * 3,
            "defect_panel_count": [0, 0, 0],
        }
    )
    raw_daily = daily.copy()
    raw_daily["defect_panel_count"] = [1, 1, 1]

    result = MWDTrendProcessor.reconcile_code_daily_counts(daily, raw_daily)

    assert result["defect_panel_count"].tolist() == [0, 1, 2]
    assert result["defect_panel_count"].sum() == 3


def test_zero_raw_monthly_target_clears_ema_counts() -> None:
    daily = pd.DataFrame(
        {
            "warehousing_time": pd.to_datetime(["2026-06-01", "2026-06-02"]),
            "total_panels": [0, 100],
            "defect_group": ["Array_Line"] * 2,
            "defect_desc": ["CodeB"] * 2,
            "defect_panel_count": [4, 7],
        }
    )
    raw_daily = daily.copy()
    raw_daily["defect_panel_count"] = 0

    result = MWDTrendProcessor.reconcile_code_daily_counts(daily, raw_daily)

    assert result["defect_panel_count"].tolist() == [0, 0]


def test_reconciliation_redistributes_counts_that_exceed_daily_input() -> None:
    daily = pd.DataFrame(
        {
            "warehousing_time": pd.to_datetime(["2026-07-01", "2026-07-02"]),
            "total_panels": [100, 100],
            "defect_group": ["Array_Mura"] * 2,
            "defect_desc": ["CodeC"] * 2,
            "defect_panel_count": [1000, 0],
        }
    )
    raw_daily = daily.copy()
    raw_daily["defect_panel_count"] = [50, 100]

    result = MWDTrendProcessor.reconcile_code_daily_counts(daily, raw_daily)

    assert result["defect_panel_count"].tolist() == [100, 50]
    assert result["defect_panel_count"].sum() == 150


def test_manual_overrides_run_after_calibration_with_daily_precedence() -> None:
    calibrated_daily = pd.DataFrame(
        {
            "warehousing_time": pd.to_datetime(
                ["2026-07-01", "2026-07-02", "2026-07-03"]
            ),
            "total_panels": [10, 10, 10],
            "defect_group": ["Array_Pixel"] * 3,
            "defect_desc": ["CodeA"] * 3,
            "defect_panel_count": [1, 1, 1],
        }
    )

    result = MWDTrendProcessor.apply_code_manual_overrides_to_daily(
        calibrated_daily,
        monthly_values={"CodeA": {"2026-07": 0.1}},
        weekly_values={"CodeA": {"2026-W27": 0.2}},
        daily_values={"CodeA": {"2026-07-02": 0.3}},
    )

    assert result["defect_panel_count"].tolist() == [2, 3, 2]
    assert pd.api.types.is_integer_dtype(result["defect_panel_count"])


def test_code_level_pipeline_removes_ema_tail_count_inflation(monkeypatch) -> None:
    panel_rows = []
    for day in ["20260501", "20260502", "20260503"]:
        for panel_number in range(10):
            is_defect = day == "20260501" and panel_number == 0
            panel_rows.append(
                {
                    "warehousing_time": day,
                    "panel_id": f"{day}-P{panel_number:02d}",
                    "defect_group": "Array_Pixel" if is_defect else None,
                    "defect_desc": "CodeA" if is_defect else None,
                }
            )
    panel_details = pd.DataFrame(panel_rows)
    empty_baseline = pd.DataFrame(columns=trend_module.CODE_BASELINE_COLUMNS)
    monkeypatch.setattr(
        trend_module,
        "_ensure_code_baseline_current",
        lambda *args, **kwargs: empty_baseline,
    )
    monkeypatch.setattr(
        trend_module,
        "_load_code_baseline_frame",
        lambda *args, **kwargs: empty_baseline,
    )
    config = AppConfig.model_validate(
        {
            "application": {"cache_ttl_hours": 4},
            "data_source": {"product_code": "PTEST"},
            "processing": {},
        }
    )

    result = MWDTrendProcessor.create_code_level_mwd_trend_data(
        panel_details_df=panel_details,
        config=config,
        ema_span=3,
        scaling_factor=1.0,
        volatility=0.0,
        warning_lines={},
        target_end_date=pd.Timestamp("2026-05-03"),
    )

    assert result is not None
    code_daily = result["daily_full"][
        result["daily_full"]["defect_desc"] == "CodeA"
    ]
    assert code_daily["defect_panel_count"].sum() == 1
    assert result["monthly"].loc[
        result["monthly"]["defect_desc"] == "CodeA",
        "defect_panel_count",
    ].sum() == 1


def test_code_level_pipeline_reaggregates_after_daily_manual_override(monkeypatch) -> None:
    panel_rows = []
    for day in ["20260501", "20260502", "20260503"]:
        for panel_number in range(10):
            is_defect = day == "20260501" and panel_number == 0
            panel_rows.append(
                {
                    "warehousing_time": day,
                    "panel_id": f"{day}-P{panel_number:02d}",
                    "defect_group": "Array_Pixel" if is_defect else None,
                    "defect_desc": "CodeA" if is_defect else None,
                }
            )
    panel_details = pd.DataFrame(panel_rows)
    empty_baseline = pd.DataFrame(columns=trend_module.CODE_BASELINE_COLUMNS)
    monkeypatch.setattr(
        trend_module,
        "_ensure_code_baseline_current",
        lambda *args, **kwargs: empty_baseline,
    )
    monkeypatch.setattr(
        trend_module,
        "_load_code_baseline_frame",
        lambda *args, **kwargs: empty_baseline,
    )
    config = AppConfig.model_validate(
        {
            "application": {"cache_ttl_hours": 4},
            "data_source": {"product_code": "PTEST"},
            "processing": {
                "code_daily_values": {"CodeA": {"2026-05-02": 0.3}},
            },
        }
    )

    result = MWDTrendProcessor.create_code_level_mwd_trend_data(
        panel_details_df=panel_details,
        config=config,
        ema_span=3,
        scaling_factor=1.0,
        volatility=0.0,
        warning_lines={},
        target_end_date=pd.Timestamp("2026-05-03"),
    )

    assert result is not None
    code_daily = result["daily_full"][
        result["daily_full"]["defect_desc"] == "CodeA"
    ]
    assert code_daily.loc[
        code_daily["warehousing_time"] == pd.Timestamp("2026-05-02"),
        "defect_panel_count",
    ].item() == 3
    assert result["weekly"].loc[
        result["weekly"]["defect_desc"] == "CodeA",
        "defect_panel_count",
    ].sum() == code_daily["defect_panel_count"].sum()
    assert result["monthly"].loc[
        result["monthly"]["defect_desc"] == "CodeA",
        "defect_panel_count",
    ].sum() == code_daily["defect_panel_count"].sum()
