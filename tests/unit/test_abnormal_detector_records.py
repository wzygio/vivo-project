"""AbnormalDetector 结构化告警记录测试：驱动按 Defect Code 自动出图。

既有文本接口 detect_system_trend_alerts 的输出必须保持不变（由 test_abnormal_detector.py 回归保证）。
"""

import pandas as pd

from src.yield_domain.core.abnormal_detector import AbnormalDetector


def _code_monthly(desc: str, group: str, rates: list[float]) -> pd.DataFrame:
    periods = ["2026-06", "2026-07", "2026-08"][-len(rates):]
    return pd.DataFrame(
        {
            "defect_desc": [desc] * len(rates),
            "defect_group": [group] * len(rates),
            "time_period": periods,
            "defect_rate": rates,
        }
    )


def _group_monthly(group: str, rates: list[float]) -> pd.DataFrame:
    periods = ["2026-06", "2026-07", "2026-08"][-len(rates):]
    return pd.DataFrame(
        {
            "defect_group": [group] * len(rates),
            "time_period": periods,
            "defect_rate": rates,
        }
    )


def test_code_level_doubling_produces_structured_record():
    code_df = _code_monthly("Particle", "DEFECT", [0.001, 0.001, 0.005])
    records = AbnormalDetector.detect_system_trend_records(pd.DataFrame(), code_df)

    assert len(records) == 1
    rec = records[0]
    assert rec["level"] == "code"
    assert rec["defect_desc"] == "Particle"
    assert rec["defect_group"] == "DEFECT"
    assert rec["time_period"] == "2026-08"
    assert rec["curr_rate"] == 0.005
    assert rec["prev_rate"] == 0.001
    assert "环比翻倍" in rec["rules"]


def test_group_level_surge_produces_structured_record():
    group_df = _group_monthly("MURA", [0.010, 0.010, 0.013])
    records = AbnormalDetector.detect_system_trend_records(group_df, pd.DataFrame())

    assert len(records) == 1
    rec = records[0]
    assert rec["level"] == "group"
    assert rec["defect_group"] == "MURA"
    assert rec["defect_desc"] is None
    assert rec["rules"] == ["增幅>0.2%"]


def test_normal_trend_produces_no_records():
    code_df = _code_monthly("Particle", "DEFECT", [0.001, 0.0011, 0.0012])
    assert AbnormalDetector.detect_system_trend_records(pd.DataFrame(), code_df) == []


def test_empty_inputs_produce_no_records():
    assert AbnormalDetector.detect_system_trend_records(pd.DataFrame(), pd.DataFrame()) == []
    assert AbnormalDetector.detect_system_trend_records(None, None) == []


def test_doubling_and_surge_rules_both_listed():
    code_df = _code_monthly("Particle", "DEFECT", [0.001, 0.001, 0.005])
    records = AbnormalDetector.detect_system_trend_records(pd.DataFrame(), code_df)
    assert records[0]["rules"] == ["环比翻倍", "增幅>0.2%"]


def test_records_align_with_text_alerts():
    """结构化记录与文本预警一一对应（同输入下同数量）。"""
    code_df = _code_monthly("Particle", "DEFECT", [0.001, 0.001, 0.005])
    group_df = _group_monthly("MURA", [0.010, 0.010, 0.013])
    records = AbnormalDetector.detect_system_trend_records(group_df, code_df)
    texts = AbnormalDetector.detect_system_trend_alerts(group_df, code_df)
    assert len(records) == len(texts) == 2
