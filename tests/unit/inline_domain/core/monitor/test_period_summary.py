from __future__ import annotations

import pandas as pd

from src.inline_domain.core.monitor.period_summary import build_period_summary


def test_period_summary_builds_overlapping_year_quarter_month_week_columns() -> None:
    alerts = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M626", "item_id": "S1", "event_time": "2026-01-10", "alarm_type": "OOS"},
            {"factory": "ARRAY", "prod_code": "M626", "item_id": "S2", "event_time": "2026-04-15", "alarm_type": "OOC"},
            {"factory": "ARRAY", "prod_code": "M626", "item_id": "S3", "event_time": "2026-09-07", "alarm_type": "OOS"},
            # 同一片的另一参数事实，不应重复计为报警片数。
            {"factory": "ARRAY", "prod_code": "M626", "item_id": "S3", "event_time": "2026-09-07 01:00", "alarm_type": "OOS"},
        ]
    )
    throughput = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M626", "event_date": "2026-01-10", "item_id": "S1"},
            {"factory": "ARRAY", "prod_code": "M626", "event_date": "2026-04-15", "item_id": "S2"},
            {"factory": "ARRAY", "prod_code": "M626", "event_date": "2026-09-07", "item_id": "S3"},
            # ALL scope 下同一物理片跨模块出现时仍只计一次。
            {"factory": "ARRAY", "prod_code": "M626", "event_date": "2026-09-07", "item_id": "S3", "scope": "ctq"},
        ]
    )

    result = build_period_summary(
        alerts, throughput, end_date=pd.Timestamp("2026-09-07")
    ).set_index("报警类型")

    assert result.columns.tolist() == [
        "Y26", "Q1", "Q2", "Q3", "M7", "M8", "M9", "W37"
    ]
    assert result.loc["过货量"].tolist() == [3, 1, 1, 1, 0, 0, 1, 1]
    assert result.loc["OOC报警片数", ["Y26", "Q2", "W37"]].tolist() == [1, 1, 0]
    assert result.loc["SOOS报警片数"].sum() == 0
    assert result.loc["OOS报警片数", ["Y26", "Q1", "Q3", "W37"]].tolist() == [2, 1, 1, 1]
    assert result.loc["Total报警片数", "Y26"] == 3
