import pandas as pd

from yield_domain.core.mwd_trend.aggregation import (
    safe_trend_aggregator as _safe_trend_aggregator,
)
from yield_domain.core.mwd_trend.manual_overrides import (
    apply_group_daily_overrides,
    apply_group_manual_overrides_to_daily as _apply_group_manual_overrides_to_daily,
    apply_group_period_overrides,
    generate_code_daily_from_period_baseline as _generate_code_daily_from_period_baseline,
    generate_group_daily_from_period_baseline as _generate_group_daily_from_period_baseline,
    rebuild_code_daily_from_weekly,
    rebuild_group_daily_from_weekly,
)
from yield_domain.core.mwd_trend.pipeline import run_manual_period_pipeline


def _group_daily_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "total_panels": [100] * 7,
            "Array_Line": [1] * 7,
        },
        index=pd.date_range("2026-05-04", periods=7, freq="D"),
    )


def _code_daily_skeleton() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "warehousing_time": pd.date_range("2026-05-04", periods=7, freq="D"),
            "total_panels": [100] * 7,
        }
    )


def test_monthly_override_does_not_rebuild_daily_before_weekly_aggregation() -> None:
    daily = _group_daily_frame()

    rebuilt = _apply_group_manual_overrides_to_daily(
        daily,
        monthly_values={"Array_Line": {"2026-05": 0.2}},
        weekly_values={},
        daily_values={},
        target_defects=["Array_Line"],
    )

    weekly = _safe_trend_aggregator(
        rebuilt,
        pd.Timestamp("2026-05-10"),
        "W",
        is_group_level=True,
    )

    assert rebuilt["Array_Line"].sum() == 7
    assert weekly["Array_Line"].sum() == 7


def test_weekly_override_rebuilds_daily_before_monthly_aggregation() -> None:
    daily = _group_daily_frame()

    rebuilt = _apply_group_manual_overrides_to_daily(
        daily,
        monthly_values={},
        weekly_values={"Array_Line": {"2026-W19": 0.2}},
        daily_values={},
        target_defects=["Array_Line"],
    )

    monthly = _safe_trend_aggregator(
        rebuilt,
        pd.Timestamp("2026-05-10"),
        "M",
        is_group_level=True,
    )

    assert rebuilt["Array_Line"].sum() == 140
    assert monthly["Array_Line"].sum() == 140


def test_period_code_generator_supports_monthly_and_weekly_targets() -> None:
    skeleton = _code_daily_skeleton()
    target = pd.DataFrame(
        {
            "warehousing_time": [pd.Timestamp("2026-05-01")],
            "total_panels": [700],
            "defect_group": ["Array_Line"],
            "defect_desc": ["CodeA"],
            "defect_panel_count": [21],
        }
    )

    monthly_daily = _generate_code_daily_from_period_baseline(
        skeleton,
        target,
        volatility=0.0,
        period_freq="M",
    )
    weekly_target = target.assign(warehousing_time=pd.Timestamp("2026-05-04"))
    weekly_daily = _generate_code_daily_from_period_baseline(
        skeleton,
        weekly_target,
        volatility=0.0,
        period_freq="W",
    )

    assert monthly_daily["defect_panel_count"].sum() == 21
    assert weekly_daily["defect_panel_count"].sum() == 21


def test_group_period_generator_preserves_target_total() -> None:
    skeleton = _group_daily_frame()[["total_panels"]]
    target = pd.DataFrame(
        {"Array_Line": [21], "total_panels": [700]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-05-04")]),
    )

    generated = _generate_group_daily_from_period_baseline(
        skeleton,
        target,
        target_defects=["Array_Line"],
        volatility=0.0,
        period_freq="W",
    )

    assert generated["Array_Line"].sum() == 21


def test_pipeline_weekly_override_rebuilds_daily_then_monthly() -> None:
    daily = _group_daily_frame()
    monthly, weekly, rebuilt = run_manual_period_pipeline(
        automatic_daily=daily,
        last_day=pd.Timestamp("2026-05-10"),
        aggregate_monthly=lambda data, anchor: _safe_trend_aggregator(
            data, anchor, "M", is_group_level=True
        ),
        aggregate_weekly=lambda data, anchor: _safe_trend_aggregator(
            data, anchor, "W", is_group_level=True
        ),
        apply_monthly_override=lambda data, values: apply_group_period_overrides(
            data, values, "monthly", ["Array_Line"]
        ),
        apply_weekly_override=lambda data, values: apply_group_period_overrides(
            data, values, "weekly", ["Array_Line"]
        ),
        rebuild_daily_from_weekly=lambda data, period_data, values: rebuild_group_daily_from_weekly(
            data, period_data, values, ["Array_Line"], 0.0
        ),
        apply_daily_override=lambda data, values: apply_group_daily_overrides(
            data, values, ["Array_Line"]
        ),
        monthly_values={},
        weekly_values={"Array_Line": {"2026-W19": 0.2}},
        daily_values={},
    )

    assert rebuilt["Array_Line"].sum() == 140
    assert weekly["Array_Line"].sum() == 140
    assert monthly["Array_Line"].sum() == 140


def test_pipeline_monthly_override_does_not_rebuild_daily() -> None:
    daily = _group_daily_frame()
    monthly, weekly, rebuilt = run_manual_period_pipeline(
        automatic_daily=daily,
        last_day=pd.Timestamp("2026-05-10"),
        aggregate_monthly=lambda data, anchor: _safe_trend_aggregator(
            data, anchor, "M", is_group_level=True
        ),
        aggregate_weekly=lambda data, anchor: _safe_trend_aggregator(
            data, anchor, "W", is_group_level=True
        ),
        apply_monthly_override=lambda data, values: apply_group_period_overrides(
            data, values, "monthly", ["Array_Line"]
        ),
        apply_weekly_override=lambda data, values: apply_group_period_overrides(
            data, values, "weekly", ["Array_Line"]
        ),
        rebuild_daily_from_weekly=lambda data, period_data, values: rebuild_group_daily_from_weekly(
            data, period_data, values, ["Array_Line"], 0.0
        ),
        apply_daily_override=lambda data, values: apply_group_daily_overrides(
            data, values, ["Array_Line"]
        ),
        monthly_values={"Array_Line": {"2026-05": 0.2}},
        weekly_values={},
        daily_values={},
    )

    assert rebuilt["Array_Line"].sum() == 7
    assert weekly["Array_Line"].sum() == 7
    assert monthly["Array_Line"].sum() == 140


def test_rebuild_code_daily_from_weekly_dedupes_daily_skeleton() -> None:
    """长表 daily 输入不得在重建后产生每个 (day, code) 多行的爆炸结果。

    回归：骨架未按天去重时，skeleton x codes 的笛卡尔积会把每天的
    total_panels 重复 N 次，分配结果被拆成大量 count=1 的碎行，
    前端堆叠后出现白条、标签重叠加粗且位置偏移。
    """
    dates = pd.date_range("2026-07-27", periods=2, freq="D")
    daily = pd.DataFrame(
        [
            {
                "warehousing_time": day,
                "total_panels": 100,
                "defect_group": "Array_Line",
                "defect_desc": code,
                "defect_panel_count": 1,
            }
            for day in dates
            for code in ("CodeA", "CodeB")
        ]
    )
    weekly = pd.DataFrame(
        {
            "warehousing_time": [pd.Timestamp("2026-07-28")],
            "total_panels": [200],
            "defect_group": ["Array_Line"],
            "defect_desc": ["CodeA"],
            "defect_panel_count": [20],
        }
    )

    rebuilt = rebuild_code_daily_from_weekly(
        daily,
        weekly,
        {"CodeA": {"2026-W31": 0.1}},
        volatility=0.0,
    )

    code_a = rebuilt[rebuilt["defect_desc"] == "CodeA"]
    assert code_a.groupby("warehousing_time").size().max() == 1
    assert code_a["defect_panel_count"].sum() == 20
    # 未被 override 的 CodeB 保持原样
    code_b = rebuilt[rebuilt["defect_desc"] == "CodeB"]
    assert code_b["defect_panel_count"].tolist() == [1, 1]
