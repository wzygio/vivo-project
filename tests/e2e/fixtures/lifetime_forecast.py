"""Deterministic report inputs and a previously published result for UI tests."""

import pandas as pd


def forecast_source(raw):
    samples = raw.loc[raw.test_time.eq('0')]
    return pd.concat([
        samples.assign(test_time=str(hour), eff_decay=value)
        for hour, value in [(100, 0.99), (200, 0.97), (300, 0.98), (400, 0.965)]
    ], ignore_index=True)


def fixture_alert_result(scenario):
    if scenario in ('saved_alert', 'saved_alert_source_failure', 'spec', 'near_threshold'):
        return {'state': 'alert', 'alerts': pd.DataFrame([{
            '产品型号': 'M678', '批次号': '2026/3/10', '测试画面': 'W', '样品编号': 1,
            '指标': '效率衰减', '规格时间(H)': 660,
            '规格点数值': 0.949978 if scenario == 'near_threshold' else 0.9481,
            '取值方式': '预测', '判定阈值': 0.95,
        }])}
    return {'state': 'ok', 'alerts': pd.DataFrame()}
