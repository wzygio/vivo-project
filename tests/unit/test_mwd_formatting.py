"""MWD 输出格式化的共享预处理契约。"""

import pandas as pd

from src.yield_domain.core.mwd_trend.formatting import (
    format_code_results,
    format_group_results,
)


def _count_datetime_calls(monkeypatch):
    import src.yield_domain.core.mwd_trend.formatting as module

    real_to_datetime = pd.to_datetime
    calls = {"count": 0}

    def counting_to_datetime(*args, **kwargs):
        calls["count"] += 1
        return real_to_datetime(*args, **kwargs)

    monkeypatch.setattr(module.pd, "to_datetime", counting_to_datetime)
    return calls


def test_code_daily_full_and_recent_share_one_preparation(monkeypatch) -> None:
    dates = pd.date_range("2026-07-01", periods=10, freq="D")
    daily = pd.DataFrame(
        {
            "warehousing_time": dates,
            "total_panels": [100] * 10,
            "defect_group": ["Array_Line"] * 10,
            "defect_desc": ["CodeA"] * 10,
            "defect_panel_count": list(range(10)),
        }
    )
    calls = _count_datetime_calls(monkeypatch)

    result = format_code_results(pd.DataFrame(), pd.DataFrame(), daily)

    assert calls["count"] == 1
    assert result["daily_full"].columns.tolist() == [
        "warehousing_time",
        "total_panels",
        "defect_group",
        "defect_desc",
        "defect_panel_count",
        "time_period",
        "defect_rate",
    ]
    assert result["daily_full"]["time_period"].astype(str).tolist() == [
        date.strftime("%Y-%m-%d") for date in dates
    ]
    assert result["daily"]["time_period"].astype(str).unique().tolist() == [
        date.strftime("%m-%d") for date in dates[-7:]
    ]


def test_group_daily_full_and_recent_share_one_preparation(monkeypatch) -> None:
    dates = pd.date_range("2026-07-01", periods=10, freq="D")
    daily = pd.DataFrame(
        {
            "total_panels": [100] * 10,
            "Array_Line": list(range(10)),
        },
        index=dates,
    )
    daily.index.name = "warehousing_time"
    calls = _count_datetime_calls(monkeypatch)

    result = format_group_results(
        pd.DataFrame(), pd.DataFrame(), daily, ["Array_Line"]
    )

    assert calls["count"] == 1
    assert len(result["daily_full"]) == 10
    assert result["daily"]["time_period"].astype(str).unique().tolist() == [
        date.strftime("%m-%d") for date in dates[-7:]
    ]
