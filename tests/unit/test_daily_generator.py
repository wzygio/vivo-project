# tests/unit/test_daily_generator.py
"""指定良损驱动的日度生成器行为测试。"""
import pandas as pd
import pytest

from src.yield_domain.core.mwd_trend.daily_generator import generate_daily_counts


def _padded_daily(months_days: dict[str, int], total_panels: int = 10000) -> pd.DataFrame:
    """构造 Code 级 padded 日度长表（单缺陷）。"""
    dates = []
    for month, days in months_days.items():
        for day in range(1, days + 1):
            dates.append(f"{month}-{day:02d}")
    rows = []
    for date in dates:
        rows.append(
            {
                "warehousing_time": pd.Timestamp(date),
                "total_panels": total_panels,
                "defect_group": "Array_Line",
                "defect_desc": "G向单亮线",
                "defect_panel_count": 7,  # 原始日度不良数（待被生成值替换）
            }
        )
    return pd.DataFrame(rows)


class TestGenerateDailyCounts:
    def test_output_is_deterministic_across_runs(self):
        df = _padded_daily({"2026-06": 30, "2026-07": 31})
        targets = {"G向单亮线": {"2026-06": 0.001, "2026-07": 0.002}}

        first = generate_daily_counts(df, targets, "M999")
        second = generate_daily_counts(df, targets, "M999")

        pd.testing.assert_frame_equal(first, second)

    def test_monthly_total_matches_specified_rate(self):
        df = _padded_daily({"2026-07": 31}, total_panels=10000)
        targets = {"G向单亮线": {"2026-07": 0.002}}

        result = generate_daily_counts(df, targets, "M999")

        assert result["defect_panel_count"].sum() == round(0.002 * 31 * 10000)

    def test_daily_count_never_exceeds_daily_input(self):
        df = _padded_daily({"2026-07": 31}, total_panels=100)
        targets = {"G向单亮线": {"2026-07": 0.5}}  # 极高良损，容易撞容量

        result = generate_daily_counts(df, targets, "M999")

        assert (result["defect_panel_count"] <= result["total_panels"]).all()

    def test_unspecified_defect_keeps_raw_counts(self):
        df = _padded_daily({"2026-07": 31})
        targets = {}  # 空目标 → 全部回落原始

        result = generate_daily_counts(df, targets, "M999")

        assert (result["defect_panel_count"] == 7).all()

    def test_zero_input_days_get_zero_counts(self):
        df = _padded_daily({"2026-07": 31}, total_panels=10000)
        df.loc[df["warehousing_time"] == pd.Timestamp("2026-07-10"), "total_panels"] = 0
        targets = {"G向单亮线": {"2026-07": 0.002}}

        result = generate_daily_counts(df, targets, "M999")

        zero_day = result[result["warehousing_time"] == pd.Timestamp("2026-07-10")]
        assert zero_day["defect_panel_count"].iloc[0] == 0
        # 但月度合计仍精确达标
        assert result["defect_panel_count"].sum() == round(0.002 * (31 * 10000 - 10000))


import numpy as np

from src.yield_domain.core.mwd_trend.daily_generator import (
    _hash_unit_interval,
    interpolated_base_rates,
)


class TestCrossMonthSmoothness:
    """月中锚点线性插值：跨月基线连续，无阶梯。"""

    def test_base_rate_is_continuous_across_month_boundary(self):
        dates = pd.Series(pd.to_datetime(["2026-06-30", "2026-07-01"]))
        rates = interpolated_base_rates(dates, {"2026-06": 0.001, "2026-07": 0.004})

        step = abs(rates[1] - rates[0])
        # 相邻两日基线率之差远小于两月目标之差（若是阶梯则相等）
        assert step < 0.2 * (0.004 - 0.001)
        assert rates[0] > 0.001 and rates[1] < 0.004  # 处于过渡区间

    def test_flat_extension_beyond_anchor_range(self):
        dates = pd.Series(pd.to_datetime(["2026-06-01", "2026-07-31"]))
        rates = interpolated_base_rates(dates, {"2026-06": 0.001, "2026-07": 0.004})

        assert rates[0] == pytest.approx(0.001)  # 首锚之前平延
        assert rates[1] == pytest.approx(0.004)  # 末锚之后平延


class TestNoPeriodicOscillation:
    """哈希白噪声：无固定周期结构。"""

    def test_noise_sequence_has_no_periodic_autocorrelation(self):
        days = pd.date_range("2026-05-01", periods=120, freq="D")
        noise = np.array(
            [_hash_unit_interval("M999", "G向单亮线", d) for d in days]
        )
        centered = noise - noise.mean()
        for lag in range(1, 15):
            corr = np.corrcoef(centered[:-lag], centered[lag:])[0, 1]
            assert abs(corr) < 0.5, f"lag={lag} 出现周期性: {corr}"

    def test_noise_is_not_sine_like_repeating(self):
        # 任意 7 日窗口的噪声值不应与下一窗口逐日相等（排除 weekly 周期）
        days = pd.date_range("2026-06-01", periods=14, freq="D")
        noise = [_hash_unit_interval("M999", "G向单亮线", d) for d in days]
        assert noise[:7] != noise[7:]
