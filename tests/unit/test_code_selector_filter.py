import pandas as pd

from app.charts.mwd_chart import prepare_union_data_for_filter
from app.components.code_selector import (
    _build_code_options_by_group,
    _calculate_eligible_series,
    _get_default_group,
    _prepare_processed_dataframe,
)


def test_rate_filter_prefers_monthly_grain_from_mwd_dict() -> None:
    source_data = {
        "monthly": pd.DataFrame(
            [
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "RGB黑斑",
                    "defect_rate": 0.00005,
                },
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "高发Code",
                    "defect_rate": 0.0002,
                },
            ]
        ),
        "daily": pd.DataFrame(
            [
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "RGB黑斑",
                    "defect_rate": 0.01,
                },
            ]
        ),
    }

    processed_df = _prepare_processed_dataframe(source_data)

    eligible = _calculate_eligible_series(
        processed_df,
        filter_by="rate",
        rate_threshold=0.0001,
        count_threshold=20,
    )

    assert ("Array_Mura", "RGB黑斑") not in eligible.index
    assert eligible.loc[("Array_Mura", "高发Code")] == 0.0002


def test_rate_filter_uses_monthly_average_metric_when_present() -> None:
    processed_df = pd.DataFrame(
        [
            {
                "defect_group": "Array_Mura",
                "defect_desc": "RGB黑斑",
                "defect_rate": 0.02,
                "monthly_avg_rate": 0.00005,
            },
            {
                "defect_group": "Array_Mura",
                "defect_desc": "高发Code",
                "defect_rate": 0.002,
                "monthly_avg_rate": 0.0002,
            },
        ]
    )

    eligible = _calculate_eligible_series(
        processed_df,
        filter_by="rate",
        rate_threshold=0.0001,
        count_threshold=20,
    )
    options = _build_code_options_by_group(["Array_Mura"], eligible)

    assert options["Array_Mura"] == ["---请选择---", "高发Code"]


def test_rate_filter_falls_back_to_defect_rate_without_monthly_context() -> None:
    processed_df = pd.DataFrame(
        [
            {
                "defect_group": "Array_Mura",
                "defect_desc": "Lot高发Code",
                "defect_rate": 0.0002,
            },
        ]
    )

    eligible = _calculate_eligible_series(
        processed_df,
        filter_by="rate",
        rate_threshold=0.0001,
        count_threshold=20,
    )

    assert eligible.loc[("Array_Mura", "Lot高发Code")] == 0.0002


def test_union_filter_payload_preserves_monthly_average_for_threshold() -> None:
    mwd_data = {
        "monthly": pd.DataFrame(
            [
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "RGB黑斑",
                    "defect_rate": 0.00005,
                }
            ]
        ),
        "daily": pd.DataFrame(
            [
                {
                    "defect_group": "Array_Mura",
                    "defect_desc": "RGB黑斑",
                    "defect_rate": 0.01,
                }
            ]
        ),
    }
    lot_data = {
        "code_level_details": {
            "Array_Mura": pd.DataFrame(
                [
                    {
                        "defect_group": "Array_Mura",
                        "defect_desc": "RGB黑斑",
                        "defect_rate": 0.02,
                    }
                ]
            )
        }
    }

    payload = prepare_union_data_for_filter(mwd_data, lot_data, pd.DataFrame())
    eligible = _calculate_eligible_series(
        payload,
        filter_by="rate",
        rate_threshold=0.0001,
        count_threshold=20,
    )

    assert payload.loc[0, "defect_rate"] == 0.02
    assert payload.loc[0, "monthly_avg_rate"] == 0.00005
    assert ("Array_Mura", "RGB黑斑") not in eligible.index


def test_default_group_prefers_group_with_selectable_codes() -> None:
    options = {
        "Array_Line": ["---请选择---"],
        "Array_Mura": ["---请选择---", "高发Code"],
    }

    assert _get_default_group(["Array_Line", "Array_Mura"], options) == "Array_Mura"
