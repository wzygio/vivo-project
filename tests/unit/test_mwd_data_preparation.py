"""MWD 日度容量数据准备测试。"""

import pandas as pd

from yield_domain.core.mwd_trend.data_preparation import prepare_code_raw_data


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
