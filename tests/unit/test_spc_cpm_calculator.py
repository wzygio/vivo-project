import math

import numpy as np
import pandas as pd

from src.spc_domain.core.cpm_calculator import (
    PERIOD_SIGMA_SOURCE_POINT_VALUE,
    PERIOD_SIGMA_SOURCE_SHEET_MEAN,
    build_all_available_period_axis,
    build_available_period_axis,
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


def test_build_available_period_axis_uses_recent_periods_with_data_not_continuous_calendar() -> None:
    sheet_features = pd.DataFrame(
        {
            "sheet_start_time": [
                "2026-06-10",
                "2026-06-15",
                "2026-06-16",
                "2026-06-17",
                "2026-06-29",
                "2026-06-30",
                "2026-07-01",
                "2026-07-02",
            ]
        }
    )

    axis = build_available_period_axis(sheet_features, pd.Timestamp("2026-07-07").date())

    assert axis[axis["period_type"] == "month"]["period_label"].tolist() == ["2026-06", "2026-07"]
    assert axis[axis["period_type"] == "week"]["period_label"].tolist() == [
        "2026-W24",
        "2026-W25",
        "2026-W27",
    ]
    assert axis[axis["period_type"] == "day"]["period_label"].tolist() == [
        "2026-06-15",
        "2026-06-16",
        "2026-06-17",
        "2026-06-29",
        "2026-06-30",
        "2026-07-01",
        "2026-07-02",
    ]


def test_build_all_available_period_axis_uses_unique_dates_inside_query_window() -> None:
    sheet_features = pd.DataFrame(
        {
            "sheet_start_time": [
                "2026-04-30",
                "2026-05-02",
                "2026-05-02 12:00:00",
                "invalid",
                "2026-06-30",
                "2026-07-01",
            ]
        }
    )

    axis = build_all_available_period_axis(
        sheet_features,
        pd.Timestamp("2026-06-30").date(),
    )

    assert axis["period_type"].tolist() == ["month", "month", "week", "week", "day", "day"]
    assert axis["period_label"].tolist() == [
        "2026-05",
        "2026-06",
        "2026-W18",
        "2026-W27",
        "2026-05-02",
        "2026-06-30",
    ]
    assert axis["period_sort"].tolist() == [101, 102, 201, 202, 301, 302]


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


def test_build_period_capability_report_uses_sheet_mean_for_mu_and_point_values_for_sigma() -> None:
    sheet_features = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-25 08:00:00",
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
                "sheet_start_time": "2026-06-25 09:00:00",
                "sheet_mean": 51.0,
                "usl": 55.0,
                "lsl": 45.0,
                "target": 50.0,
            },
        ]
    )
    raw_measurements = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "site_name": "P1",
                "sheet_start_time": "2026-06-25 08:00:00",
                "param_value": 45.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "site_name": "P2",
                "sheet_start_time": "2026-06-25 08:00:00",
                "param_value": 53.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000102",
                "step_id": "S1",
                "param_name": "THK",
                "site_name": "P1",
                "sheet_start_time": "2026-06-25 09:00:00",
                "param_value": 47.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000102",
                "step_id": "S1",
                "param_name": "THK",
                "site_name": "P2",
                "sheet_start_time": "2026-06-25 09:00:00",
                "param_value": 55.0,
            },
        ]
    )

    report = build_period_capability_report(
        sheet_features,
        end_date=pd.Timestamp("2026-06-25").date(),
        raw_measurements=raw_measurements,
        sigma_source=PERIOD_SIGMA_SOURCE_POINT_VALUE,
    )
    day_row = report[(report["period_type"] == "day") & (report["period_label"] == "2026-06-25")].iloc[0]
    expected_mean = float(sheet_features["sheet_mean"].mean())
    expected_point_std = float(raw_measurements["param_value"].std(ddof=1))

    assert day_row["sample_count"] == 2
    assert day_row["point_count"] == 4
    assert report["point_count"].dtype == np.dtype("int64")
    assert day_row["sigma_source"] == PERIOD_SIGMA_SOURCE_POINT_VALUE
    assert day_row["mean_value"] == expected_mean
    assert day_row["std_value"] == expected_point_std
    assert day_row["cpk"] == calculate_cpk(expected_mean, expected_point_std, 55.0, 45.0)
    assert day_row["cpm"] == calculate_cpm(expected_mean, expected_point_std, 55.0, 45.0, 50.0)


def test_build_period_capability_report_defaults_to_sheet_mean_sigma_when_raw_measurements_exist() -> None:
    sheet_features = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-25 08:00:00",
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
                "sheet_start_time": "2026-06-25 09:00:00",
                "sheet_mean": 51.0,
                "usl": 55.0,
                "lsl": 45.0,
                "target": 50.0,
            },
        ]
    )
    raw_measurements = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "site_name": "P1",
                "sheet_start_time": "2026-06-25 08:00:00",
                "param_value": 45.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000102",
                "step_id": "S1",
                "param_name": "THK",
                "site_name": "P2",
                "sheet_start_time": "2026-06-25 09:00:00",
                "param_value": 55.0,
            },
        ]
    )

    report = build_period_capability_report(
        sheet_features,
        end_date=pd.Timestamp("2026-06-25").date(),
        raw_measurements=raw_measurements,
    )
    day_row = report[(report["period_type"] == "day") & (report["period_label"] == "2026-06-25")].iloc[0]
    expected_sheet_std = float(sheet_features["sheet_mean"].std(ddof=1))

    assert day_row["sigma_source"] == PERIOD_SIGMA_SOURCE_SHEET_MEAN
    assert day_row["std_value"] == expected_sheet_std
    assert day_row["cpk"] == calculate_cpk(50.0, expected_sheet_std, 55.0, 45.0)


def test_build_period_capability_report_calculates_single_sheet_period_with_point_sigma() -> None:
    sheet_features = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-25 08:00:00",
                "sheet_mean": 50.0,
                "usl": 55.0,
                "lsl": 45.0,
                "target": 50.0,
            },
        ]
    )
    raw_measurements = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "site_name": "P1",
                "sheet_start_time": "2026-06-25 08:00:00",
                "param_value": 49.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "site_name": "P2",
                "sheet_start_time": "2026-06-25 08:00:00",
                "param_value": 51.0,
            },
        ]
    )

    report = build_period_capability_report(
        sheet_features,
        end_date=pd.Timestamp("2026-06-25").date(),
        raw_measurements=raw_measurements,
        sigma_source=PERIOD_SIGMA_SOURCE_POINT_VALUE,
    )
    day_row = report[(report["period_type"] == "day") & (report["period_label"] == "2026-06-25")].iloc[0]

    assert day_row["sample_count"] == 1
    assert day_row["point_count"] == 2
    assert pd.notna(day_row["cpm"])
    assert pd.notna(day_row["cpk"])


def test_build_period_capability_report_keeps_older_days_for_metric_backfill() -> None:
    rows = []
    for day_index in range(1, 9):
        sheet_count = 1 if day_index == 8 else 2
        for sheet_index in range(sheet_count):
            rows.append(
                {
                    "prod_code": "P1",
                    "factory": "ARRAY",
                    "sheet_id": f"LOT{day_index:06d}{sheet_index:02d}",
                    "step_id": "S1",
                    "param_name": "THK",
                    "sheet_start_time": f"2026-06-{day_index:02d}",
                    "sheet_mean": 49.0 + sheet_index,
                    "usl": 55.0,
                    "lsl": 45.0,
                    "target": 50.0,
                }
            )

    report = build_period_capability_report(pd.DataFrame(rows), end_date=pd.Timestamp("2026-06-08").date())
    day_rows = report[report["period_type"] == "day"]
    first_day = day_rows[day_rows["period_label"] == "2026-06-01"].iloc[0]
    latest_day = day_rows[day_rows["period_label"] == "2026-06-08"].iloc[0]

    assert first_day["sample_count"] == 2
    assert pd.notna(first_day["cpm"])
    assert latest_day["sample_count"] == 1
    assert pd.isna(latest_day["cpm"])


def test_build_period_capability_report_filters_before_taking_first_limits() -> None:
    sheet_features = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "INVALID",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-25 07:00:00",
                "sheet_mean": np.nan,
                "usl": 999.0,
                "lsl": 0.0,
                "ucl": 998.0,
                "lcl": 1.0,
                "target": 500.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-25 08:00:00",
                "sheet_mean": 49.0,
                "usl": 55.0,
                "lsl": 45.0,
                "ucl": np.nan,
                "lcl": 46.0,
                "target": np.nan,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000102",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-25 09:00:00",
                "sheet_mean": 51.0,
                "usl": 56.0,
                "lsl": 44.0,
                "ucl": 54.0,
                "lcl": 47.0,
                "target": 52.0,
            },
        ]
    )

    report = build_period_capability_report(
        sheet_features,
        end_date=pd.Timestamp("2026-06-25").date(),
    )
    day_row = report[report["period_type"] == "day"].iloc[0]

    assert day_row["sample_count"] == 2
    assert day_row["usl"] == 55.0
    assert day_row["lsl"] == 45.0
    assert day_row["ucl"] == 54.0
    assert day_row["lcl"] == 46.0
    assert day_row["target"] == 52.0


def test_build_period_capability_report_keeps_nan_group_keys() -> None:
    sheet_features = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": np.nan,
                "sheet_id": f"LOT0000010{index}",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": f"2026-06-25 0{index}:00:00",
                "sheet_mean": value,
                "usl": 55.0,
                "lsl": 45.0,
            }
            for index, value in enumerate((49.0, 51.0), start=1)
        ]
    )

    report = build_period_capability_report(
        sheet_features,
        end_date=pd.Timestamp("2026-06-25").date(),
    )

    assert len(report) == 3
    assert report["factory"].isna().all()
    assert report["sample_count"].tolist() == [2, 2, 2]


def test_build_period_capability_report_falls_back_per_group_when_point_stats_missing() -> None:
    sheet_features = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": f"LOT0000010{sheet_index}",
                "step_id": "S1",
                "param_name": param_name,
                "sheet_start_time": f"2026-06-25 0{sheet_index}:00:00",
                "sheet_mean": value,
                "usl": 55.0,
                "lsl": 45.0,
            }
            for param_name in ("CD", "THK")
            for sheet_index, value in enumerate((49.0, 51.0), start=1)
        ]
    )
    raw_measurements = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-25 01:00:00",
                "param_value": value,
            }
            for value in (48.0, 50.0, 52.0)
        ]
    )

    report = build_period_capability_report(
        sheet_features,
        end_date=pd.Timestamp("2026-06-25").date(),
        raw_measurements=raw_measurements,
        sigma_source=PERIOD_SIGMA_SOURCE_POINT_VALUE,
    )
    day_rows = report[report["period_type"] == "day"].set_index("param_name")

    assert day_rows.loc["THK", "sigma_source"] == PERIOD_SIGMA_SOURCE_POINT_VALUE
    assert day_rows.loc["THK", "point_count"] == 3
    assert day_rows.loc["THK", "std_value"] == 2.0
    assert day_rows.loc["CD", "sigma_source"] == PERIOD_SIGMA_SOURCE_SHEET_MEAN
    assert pd.isna(day_rows.loc["CD", "point_count"])
    assert day_rows.loc["CD", "std_value"] == math.sqrt(2.0)
    assert report["point_count"].dtype == np.dtype("float64")


def test_build_period_capability_report_isolates_point_sigma_by_factory() -> None:
    sheet_features = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": factory,
                "sheet_id": f"{factory}{sheet_index}",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": f"2026-06-25 0{sheet_index}:00:00",
                "sheet_mean": value,
                "usl": 55.0,
                "lsl": 45.0,
            }
            for factory in ("ARRAY", "OLED")
            for sheet_index, value in enumerate((49.0, 51.0), start=1)
        ]
    )
    raw_measurements = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "ARRAY1",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_start_time": "2026-06-25 01:00:00",
                "param_value": value,
            }
            for value in (48.0, 50.0, 52.0)
        ]
    )

    report = build_period_capability_report(
        sheet_features,
        end_date=pd.Timestamp("2026-06-25").date(),
        raw_measurements=raw_measurements,
        sigma_source=PERIOD_SIGMA_SOURCE_POINT_VALUE,
    )
    day_rows = report[report["period_type"] == "day"].set_index("factory")

    assert day_rows.loc["ARRAY", "sigma_source"] == PERIOD_SIGMA_SOURCE_POINT_VALUE
    assert day_rows.loc["ARRAY", "point_count"] == 3
    assert day_rows.loc["OLED", "sigma_source"] == PERIOD_SIGMA_SOURCE_SHEET_MEAN
    assert pd.isna(day_rows.loc["OLED", "point_count"])
