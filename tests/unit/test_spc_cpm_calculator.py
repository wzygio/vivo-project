import math

import numpy as np
import pandas as pd

from src.spc_domain.core.cpm_calculator import (
    build_period_axis,
    build_period_capability_report,
    build_lot_cpm_report,
    calculate_cpk,
    calculate_cpm,
    derive_lot_id,
)


def test_calculate_cpm_uses_target_adjusted_denominator() -> None:
    cpm = calculate_cpm(mean_value=50.0, std_value=1.0, usl=55.0, lsl=45.0, target=50.0)

    assert cpm == 10.0 / 6.0


def test_calculate_cpm_penalizes_mean_drift_from_target() -> None:
    cpm = calculate_cpm(mean_value=51.0, std_value=1.0, usl=55.0, lsl=45.0, target=50.0)

    assert cpm == 10.0 / (6.0 * math.sqrt(2.0))


def test_calculate_cpm_returns_nan_for_single_sided_specs() -> None:
    cpm = calculate_cpm(mean_value=50.0, std_value=1.0, usl=55.0, lsl=np.nan, target=50.0)

    assert math.isnan(cpm)


def test_calculate_cpk_uses_nearest_spec_distance() -> None:
    centered_cpk = calculate_cpk(mean_value=50.0, std_value=1.0, usl=55.0, lsl=45.0)
    drifted_cpk = calculate_cpk(mean_value=54.0, std_value=1.0, usl=55.0, lsl=45.0)

    assert centered_cpk == 5.0 / 3.0
    assert drifted_cpk == 1.0 / 3.0


def test_derive_lot_id_uses_first_nine_chars() -> None:
    assert derive_lot_id("ABCDEFGHIJK") == "ABCDEFGHI"
    assert derive_lot_id("SHORT") == ""


def test_build_period_axis_reserves_two_months_three_weeks_seven_days() -> None:
    axis = build_period_axis(pd.Timestamp("2026-06-25").date())

    assert axis["period_type"].tolist() == ["month", "month", "week", "week", "week"] + ["day"] * 7
    assert axis["period_label"].tolist() == [
        "2026-05",
        "2026-06",
        "2026-W24",
        "2026-W25",
        "2026-W26",
        "2026-06-19",
        "2026-06-20",
        "2026-06-21",
        "2026-06-22",
        "2026-06-23",
        "2026-06-24",
        "2026-06-25",
    ]
    assert axis["period_sort"].tolist() == [101, 102, 201, 202, 203, 301, 302, 303, 304, 305, 306, 307]


def test_build_lot_cpm_report_groups_by_lot_and_indicator() -> None:
    sheet_features = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_mean": 49.0,
                "usl": 55.0,
                "lsl": 45.0,
                "target": 50.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000102",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_mean": 50.0,
                "usl": 55.0,
                "lsl": 45.0,
                "target": 50.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000103",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_mean": 51.0,
                "usl": 55.0,
                "lsl": 45.0,
                "target": 50.0,
            },
        ]
    )

    report = build_lot_cpm_report(sheet_features)

    assert len(report) == 1
    row = report.iloc[0]
    assert row["lot_id"] == "LOT000001"
    assert row["sheet_count"] == 3
    assert row["lot_mean"] == 50.0
    assert row["lot_std"] == 1.0
    assert row["cpm"] == 10.0 / 6.0
    assert row["cpk"] == 5.0 / 3.0


def test_build_period_capability_report_groups_month_week_day() -> None:
    sheet_features = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-05-10",
                "sheet_mean": 49.0,
                "usl": 55.0,
                "lsl": 45.0,
                "ucl": 54.0,
                "lcl": 46.0,
                "target": 50.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000102",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-05-20",
                "sheet_mean": 51.0,
                "usl": 55.0,
                "lsl": 45.0,
                "ucl": 54.0,
                "lcl": 46.0,
                "target": 50.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000201",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-24",
                "sheet_mean": 50.0,
                "usl": 55.0,
                "lsl": 45.0,
                "ucl": 54.0,
                "lcl": 46.0,
                "target": 50.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000202",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-25",
                "sheet_mean": 52.0,
                "usl": 55.0,
                "lsl": 45.0,
                "ucl": 54.0,
                "lcl": 46.0,
                "target": 50.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000203",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-30",
                "sheet_mean": 48.0,
                "usl": 55.0,
                "lsl": 45.0,
                "ucl": 54.0,
                "lcl": 46.0,
                "target": 50.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "OLD00000101",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-04-30",
                "sheet_mean": 50.0,
                "usl": 55.0,
                "lsl": 45.0,
                "ucl": 54.0,
                "lcl": 46.0,
                "target": 50.0,
            },
        ]
    )

    report = build_period_capability_report(sheet_features, end_date=pd.Timestamp("2026-06-30").date())

    assert set(report["period_type"]) == {"month", "week", "day"}
    may_row = report[(report["period_type"] == "month") & (report["period_label"] == "2026-05")].iloc[0]
    assert may_row["sample_count"] == 2
    assert may_row["mean_value"] == 50.0
    assert may_row["cpm"] == 10.0 / (6.0 * math.sqrt(2.0))
    assert may_row["cpk"] == 5.0 / (3.0 * math.sqrt(2.0))
    assert "2026-04" not in report["period_label"].tolist()
