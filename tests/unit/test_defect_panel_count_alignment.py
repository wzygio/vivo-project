# tests/unit/test_defect_panel_count_alignment.py
"""facade 端到端：指定良损驱动的 MWD 月/周/日趋势。"""
import pandas as pd
import pytest

from src.shared_kernel.config_model import AppConfig
from yield_domain.core.mwd_trend import mwd_trend_processor as trend_module
from yield_domain.core.mwd_trend.mwd_trend_processor import MWDTrendProcessor


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "application": {"cache_ttl_hours": 4},
            "data_source": {"product_code": "PTEST"},
            "processing": {},
        }
    )


def _panel_details(days, defective) -> pd.DataFrame:
    """构造 panel 明细：每天 10 片 panel；defective = {day: [(suffix, group, code)]}。"""
    rows = []
    for day in days:
        defect_map = {suffix: (group, code) for suffix, group, code in defective.get(day, [])}
        for number in range(10):
            suffix = f"P{number:02d}"
            group, code = defect_map.get(suffix, (None, None))
            rows.append(
                {
                    "warehousing_time": day,
                    "panel_id": f"{day}-{suffix}",
                    "defect_group": group,
                    "defect_desc": code,
                }
            )
    return pd.DataFrame(rows)


def test_code_formatter_exposes_full_weekly_history_but_keeps_ui_at_three_weeks() -> None:
    weekly = pd.DataFrame(
        {
            "warehousing_time": pd.date_range("2026-05-24", periods=6, freq="7D"),
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


def test_specified_rate_drives_code_monthly_total() -> None:
    days = ["20260501", "20260502", "20260503"]
    panel_details = _panel_details(days, {"20260501": [("P00", "Array_Pixel", "CodeA")]})
    targets = {"CodeA": {"2026-05": 0.2}}  # 30 片投入 × 0.2 = 6 片不良

    result = MWDTrendProcessor.create_code_level_mwd_trend_data(
        panel_details_df=panel_details,
        config=_config(),
        modifier_targets=targets,
        target_end_date=pd.Timestamp("2026-05-03"),
    )

    assert result is not None
    code_daily = result["daily_full"][result["daily_full"]["defect_desc"] == "CodeA"]
    assert code_daily["defect_panel_count"].sum() == 6
    monthly = result["monthly"][result["monthly"]["defect_desc"] == "CodeA"]
    assert monthly["defect_panel_count"].sum() == 6
    weekly = result["weekly_full"][result["weekly_full"]["defect_desc"] == "CodeA"]
    assert weekly["defect_panel_count"].sum() == 6


def test_unspecified_code_uses_raw_monthly_rate() -> None:
    days = ["20260501", "20260502", "20260503"]
    panel_details = _panel_details(days, {"20260501": [("P00", "Array_Pixel", "CodeA")]})

    result = MWDTrendProcessor.create_code_level_mwd_trend_data(
        panel_details_df=panel_details,
        config=_config(),
        modifier_targets={},
        target_end_date=pd.Timestamp("2026-05-03"),
    )

    assert result is not None
    code_daily = result["daily_full"][result["daily_full"]["defect_desc"] == "CodeA"]
    assert code_daily["defect_panel_count"].sum() == 1


def test_output_contract_keys_and_determinism() -> None:
    days = ["20260501", "20260502", "20260503"]
    panel_details = _panel_details(days, {"20260501": [("P00", "Array_Pixel", "CodeA")]})
    targets = {"CodeA": {"2026-05": 0.2}}

    first = MWDTrendProcessor.create_code_level_mwd_trend_data(
        panel_details, _config(), targets, target_end_date=pd.Timestamp("2026-05-03")
    )
    second = MWDTrendProcessor.create_code_level_mwd_trend_data(
        panel_details, _config(), targets, target_end_date=pd.Timestamp("2026-05-03")
    )

    assert set(first) == {"monthly", "weekly", "weekly_full", "daily_full", "daily"}
    pd.testing.assert_frame_equal(first["daily_full"], second["daily_full"])


def test_group_sheet_only_overrides_monthly_group_result() -> None:
    days = ["20260501", "20260502", "20260503"]
    panel_details = _panel_details(
        days,
        {
            "20260501": [("P00", "Array_Pixel", "CodeA")],
            "20260502": [("P01", "Array_Pixel", "CodeB"), ("P02", "OLED_Mura", "CodeC")],
        },
    )
    config = _config()
    group_targets = {
        "Array_Pixel": {"2026-05": 0.5},
        "OLED_Mura": {"2026-05": 0.2},
    }
    code_targets = {
        "CodeA": {"2026-05": 0.2},
        "CodeB": {"2026-05": 0.1},
        "CodeC": {"2026-05": 0.1},
    }
    code_results = MWDTrendProcessor.create_code_level_mwd_trend_data(
        panel_details_df=panel_details,
        config=config,
        modifier_targets=code_targets,
        target_end_date=pd.Timestamp("2026-05-03"),
    )
    group_results = MWDTrendProcessor.create_mwd_trend_data(
        panel_details_df=panel_details,
        mwd_code_data=code_results,
        config=config,
        modifier_targets=group_targets,
        target_end_date=pd.Timestamp("2026-05-03"),
    )

    assert group_results is not None
    group_monthly = group_results["monthly"]
    pixel = group_monthly[group_monthly["defect_group"] == "Array_Pixel"]
    assert pixel["defect_rate"].sum() == pytest.approx(0.5)
    mura = group_monthly[group_monthly["defect_group"] == "OLED_Mura"]
    assert mura["defect_rate"].sum() == pytest.approx(0.2)

    code_daily = code_results["daily_full"]
    expected_daily = code_daily.groupby(["time_period", "defect_group"])[
        "defect_panel_count"
    ].sum()
    group_daily_rows = group_results["daily_full"].copy()
    group_daily_rows["defect_panel_count"] = (
        group_daily_rows["defect_rate"] * group_daily_rows["total_panels"]
    ).round().astype(int)
    group_daily = group_daily_rows.set_index(
        ["time_period", "defect_group"]
    )["defect_panel_count"]
    pd.testing.assert_series_equal(
        group_daily.sort_index(),
        expected_daily.sort_index(),
        check_names=False,
    )

    # Group 月度指定只覆盖月度展示，不反向改写 Code 汇总得到的日度/周度事实。
    assert group_daily_rows["defect_panel_count"].sum() == 12
    weekly_counts = (
        group_results["weekly"]["defect_rate"]
        * group_results["weekly"]["total_panels"]
    ).round().astype(int)
    assert weekly_counts.sum() == 12
