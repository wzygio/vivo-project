"""MWD 日度容量数据准备测试。"""

import pandas as pd

from yield_domain.core.mwd_trend.data_preparation import prepare_code_raw_data


def test_month_keys_match_strftime_for_repeated_dates_nulls_and_iso_year_boundary():
    from yield_domain.core.mwd_trend.data_preparation import month_keys

    dates = pd.Series(pd.to_datetime(['2026-12-28', '2027-01-03', None,
                                     '2026-12-28', '2024-02-29']))
    pd.testing.assert_series_equal(month_keys(dates), dates.dt.strftime('%Y-%m'))


def test_monthly_preparation_keeps_distinct_panels_and_duplicate_indexes():
    panels = pd.DataFrame({
        'warehousing_time': ['20261231', '20261231', '20261231', '20270101', '20270101'],
        'panel_id': ['P1', 'P1', 'P1_SIM_1', 'P2', 'P3'],
        'defect_group': ['G', 'G', 'G', 'G', None],
        'defect_desc': ['C', 'C', 'C', 'C', None],
    }, index=[1, 1, 2, 2, 3])
    _, _, targets = prepare_code_raw_data(panels, pd.Timestamp('2027-02-01'))
    assert targets == {'C': {'2026-12': 1.0, '2027-01': .5, '2027-02': 0.0}}


def test_code_preparation_does_not_calculate_original_daily_defect_counts() -> None:
    panel_details = pd.DataFrame(
        [
            {
                "warehousing_time": "20260501",
                "panel_id": "P01",
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
            },
            {
                "warehousing_time": "20260501",
                "panel_id": "P02",
                "defect_group": "Array_Pixel",
                "defect_desc": "CodeA",
            },
            {
                "warehousing_time": "20260502",
                "panel_id": "P03",
                "defect_group": None,
                "defect_desc": None,
            },
        ]
    )

    prepared, _, raw_monthly_targets = prepare_code_raw_data(panel_details)

    assert prepared["defect_panel_count"].eq(0).all()
    assert prepared[["defect_group", "defect_desc"]].drop_duplicates().to_dict(
        "records"
    ) == [{"defect_group": "Array_Pixel", "defect_desc": "CodeA"}]
    assert prepared.groupby("warehousing_time")["total_panels"].first().tolist() == [
        2,
        1,
    ]
    assert raw_monthly_targets == {"CodeA": {"2026-05": 2 / 3}}


def test_code_preparation_zero_fills_months_without_input() -> None:
    panels = pd.DataFrame({
        "warehousing_time": ["20260701", "20260901"],
        "panel_id": ["P01", "P02"],
        "defect_group": ["Array_Pixel", "Array_Pixel"],
        "defect_desc": ["CodeA", "CodeA"],
    })

    _, _, targets = prepare_code_raw_data(panels, pd.Timestamp("2026-10-05"))

    assert targets == {"CodeA": {
        "2026-07": 1.0, "2026-08": 0.0, "2026-09": 1.0, "2026-10": 0.0,
    }}
