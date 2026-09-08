from __future__ import annotations

import pandas as pd

from src.inline_domain.core.monitor.period_summary import (
    MONITOR_SUMMARY_COLUMNS,
    build_current_period_records,
    build_period_summary,
    build_period_summary_from_records,
)


def test_fixed_periods_cross_year_keep_four_iso_weeks_and_unknown_history():
    result = build_period_summary_from_records(
        pd.DataFrame(columns=MONITOR_SUMMARY_COLUMNS), end_date=pd.Timestamp("2026-01-01")
    )
    assert result.columns.tolist() == ["报警类型", "Y26", "Q1", "M11", "M12", "M1", "W50", "W51", "W52", "W1"]
    assert result.drop(columns="报警类型").eq("—").all(axis=None)


def test_period_summary_builds_overlapping_year_quarter_month_week_columns() -> None:
    alerts = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M626", "item_id": "S1",
             "event_time": "2026-01-10", "alarm_type": "OOS"},
            {"factory": "ARRAY", "prod_code": "M626", "item_id": "S2",
             "event_time": "2026-04-15", "alarm_type": "OOC"},
            {"factory": "ARRAY", "prod_code": "M626", "item_id": "S3",
             "event_time": "2026-09-07", "alarm_type": "OOS"},
            # 同一片的另一参数事实，不应重复计为报警片数。
            {"factory": "ARRAY", "prod_code": "M626", "item_id": "S3",
             "event_time": "2026-09-07 01:00", "alarm_type": "OOS"},
        ]
    )
    throughput = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M626",
             "event_date": "2026-01-10", "item_id": "S1"},
            {"factory": "ARRAY", "prod_code": "M626",
             "event_date": "2026-04-15", "item_id": "S2"},
            {"factory": "ARRAY", "prod_code": "M626",
             "event_date": "2026-09-07", "item_id": "S3"},
            # ALL scope 下同一物理片跨模块出现时仍只计一次。
            {"factory": "ARRAY", "prod_code": "M626",
             "event_date": "2026-09-07", "item_id": "S3", "scope": "ctq"},
        ]
    )

    result = build_period_summary(
        alerts, throughput, end_date=pd.Timestamp("2026-09-07")
    ).set_index("报警类型")

    assert result.columns.tolist() == [
        "Y26", "Q1", "Q2", "Q3", "M7", "M8", "M9", "W34", "W35", "W36", "W37"
    ]
    assert result.loc["过货量"].tolist() == [3, 1, 1, 1, 0, 0, 1, 0, 0, 0, 1]
    assert result.loc["OOC报警片数", ["Y26", "Q2", "W37"]].tolist() == [1, 1, 0]
    assert result.loc["SOOS报警片数"].sum() == 0
    assert result.loc["OOS报警片数", ["Y26", "Q1", "Q3", "W37"]].tolist() == [2, 1, 1, 1]
    assert result.loc["Total报警片数", "Y26"] == 3


def test_current_period_records_only_emit_open_periods_per_product() -> None:
    alerts = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M626", "item_id": "S1",
             "event_time": "2026-04-15", "alarm_type": "OOC"},
            {"factory": "ARRAY", "prod_code": "M626", "item_id": "S2",
             "event_time": "2026-09-07", "alarm_type": "OOS"},
        ]
    )
    throughput = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M626", "event_date": "2026-04-15", "item_id": "S1"},
            {"factory": "ARRAY", "prod_code": "M626", "event_date": "2026-09-07", "item_id": "S2"},
        ]
    )

    result = build_current_period_records(
        alerts,
        throughput,
        products=("M626", "M678"),
        scope_key="ALL",
        factory_key="ARRAY",
        end_date=pd.Timestamp("2026-09-07"),
    )

    assert set(result["时间标签"]) == {"2026", "2026-Q3", "2026-09", "2026-W37"}
    assert len(result) == 8
    m626 = result[result["产品"].eq("M626")].set_index("时间标签")
    assert m626.loc["2026", "过货量"] == 2
    assert m626.loc["2026", "Total报警片数"] == 2
    assert m626.loc["2026-Q3", "OOS报警片数"] == 1
    assert m626.loc["2026-09", "OOS报警率"] == 1.0
    assert m626.loc["2026-W37", "Total报警率"] == 1.0


def test_period_summary_from_records_uses_full_time_labels_and_aggregates_products() -> None:
    records = pd.DataFrame(
        [
            {"产品": product, "周期类型": period_type, "时间标签": time_label,
             "显示标签": display_label, "过货量": throughput,
             "OOC报警片数": ooc, "SOOS报警片数": 0,
             "OOS报警片数": oos, "Total报警片数": ooc + oos}
            for product, period_type, time_label, display_label, throughput, ooc, oos in [
                ("M626", "年度", "2026", "Y26", 10, 1, 1),
                ("M678", "年度", "2026", "Y26", 20, 2, 1),
                ("M626", "季度", "2026-Q1", "Q1", 4, 1, 0),
                ("M626", "季度", "2026-Q2", "Q2", 3, 0, 1),
                ("M626", "季度", "2026-Q3", "Q3", 3, 0, 0),
                ("M626", "月度", "2026-07", "M7", 1, 0, 0),
                ("M626", "月度", "2026-08", "M8", 1, 0, 1),
                ("M626", "月度", "2026-09", "M9", 1, 0, 0),
                ("M626", "周度", "2026-W37", "W37", 1, 0, 0),
            ]
        ]
    )

    result = build_period_summary_from_records(
        records, end_date=pd.Timestamp("2026-09-07")
    ).set_index("报警类型")

    assert result.columns.tolist() == ["Y26", "Q1", "Q2", "Q3", "M7", "M8", "M9", "W34", "W35", "W36", "W37"]
    assert result.loc["过货量", "Y26"] == 30
    assert result.loc["OOC报警片数", "Y26"] == 3
    assert result.loc["OOS报警片数", "Q2"] == 1


def test_period_summary_marks_incomplete_product_history_as_unavailable() -> None:
    records = pd.DataFrame(
        [
            {
                "产品": "M626",
                "时间标签": "2026-Q2",
                "过货量": 10,
                "OOC报警片数": pd.NA,
                "SOOS报警片数": pd.NA,
                "OOS报警片数": pd.NA,
                "Total报警片数": pd.NA,
            }
        ]
    )

    result = build_period_summary_from_records(
        records,
        end_date=pd.Timestamp("2026-09-07"),
        expected_products=("M626", "M678"),
    ).set_index("报警类型")

    assert result.loc["过货量", "Q2"] == "—"
    assert result.loc["OOC报警片数", "Q2"] == "—"
    assert result.loc["OOS报警片数", "Q1"] == "—"
