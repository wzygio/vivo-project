import pandas as pd

from yield_domain.core.sheet_lot.overrides import (
    _calculate_lot_override_rate_heuristic,
)
from yield_domain.core.sheet_lot.simulation import _simulate_concentration


def _lot_raw_results() -> dict:
    base_info = pd.DataFrame(
        {
            "lot_id": ["LOT-MON", "LOT-TUE", "LOT-NEXT"],
            "warehousing_time": ["20260706", "20260707", "20260713"],
            "array_input_time": pd.to_datetime(
                ["2026-07-01", "2026-07-02", "2026-07-08"]
            ),
            "total_panels": [100_000, 100_000, 100_000],
            "pass_rate": [1.0, 1.0, 1.0],
        }
    )
    existing_code_row = base_info.iloc[[0]].copy()
    existing_code_row["defect_group"] = "Array_Line"
    existing_code_row["defect_desc"] = "CodeA"
    existing_code_row["defect_panel_count"] = 1
    existing_code_row["defect_rate"] = 0.00001
    return {
        "group_level_summary_for_chart": base_info,
        "code_level_details": {"Array_Line": existing_code_row},
    }


def _weekly_mwd(daily_rates: list[float]) -> dict:
    weekly = pd.DataFrame(
        {
            "time_period": ["2026-W28", "2026-W29"],
            "defect_group": ["Array_Line", "Array_Line"],
            "defect_desc": ["CodeA", "CodeA"],
            "defect_rate": [0.01, 0.02],
        }
    )
    return {
        "weekly": weekly.copy(),
        "weekly_full": weekly.copy(),
        "daily_full": pd.DataFrame(
            {
                "time_period": ["2026-07-06", "2026-07-07", "2026-07-13"],
                "defect_group": ["Array_Line"] * 3,
                "defect_desc": ["CodeA"] * 3,
                "defect_rate": daily_rates,
            }
        ),
    }


def test_lot_simulation_uses_its_iso_week_rate_and_ignores_daily_shape() -> None:
    config = {"sheet_hotspot_config": {"enable": True, "random_seed": 2025}}

    first = _simulate_concentration(
        raw_results=_lot_raw_results(),
        mwd_code_data=_weekly_mwd([0.001, 0.09, 0.001]),
        processing_config=config,
        entity_id_col="lot_id",
    )["Array_Line"].sort_values("lot_id")
    second = _simulate_concentration(
        raw_results=_lot_raw_results(),
        mwd_code_data=_weekly_mwd([0.09, 0.001, 0.09]),
        processing_config=config,
        entity_id_col="lot_id",
    )["Array_Line"].sort_values("lot_id")

    assert first["lot_id"].tolist() == ["LOT-MON", "LOT-NEXT", "LOT-TUE"]
    assert first["defect_panel_count"].tolist() == second[
        "defect_panel_count"
    ].tolist()

    rates = first.set_index("lot_id")["defect_rate"]
    assert 0.008 <= rates["LOT-MON"] <= 0.012
    assert 0.008 <= rates["LOT-TUE"] <= 0.012
    assert 0.016 <= rates["LOT-NEXT"] <= 0.024


def test_lot_simulation_preserves_raw_values_when_weekly_trend_is_missing() -> None:
    raw_results = _lot_raw_results()

    result = _simulate_concentration(
        raw_results=raw_results,
        mwd_code_data={"daily_full": _weekly_mwd([0.01, 0.01, 0.01])["daily_full"]},
        processing_config={"sheet_hotspot_config": {"enable": True}},
        entity_id_col="lot_id",
    )

    actual = result["Array_Line"].reset_index(drop=True)
    expected = raw_results["code_level_details"]["Array_Line"].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected)


def test_lot_simulation_uses_full_weekly_history_beyond_the_ui_window() -> None:
    raw_results = _lot_raw_results()
    mwd_code_data = _weekly_mwd([0.01, 0.01, 0.01])
    mwd_code_data["weekly"] = mwd_code_data["weekly"].loc[
        mwd_code_data["weekly"]["time_period"] == "2026-W29"
    ].copy()

    result = _simulate_concentration(
        raw_results=raw_results,
        mwd_code_data=mwd_code_data,
        processing_config={"sheet_hotspot_config": {"enable": True}},
        entity_id_col="lot_id",
    )["Array_Line"]

    historical_week = result[result["lot_id"].isin(["LOT-MON", "LOT-TUE"])]
    assert historical_week["defect_panel_count"].sum() == 2_000
    assert historical_week["defect_panel_count"].min() > 1
    assert 0.008 <= historical_week["defect_rate"].min()
    assert historical_week["defect_rate"].max() <= 0.012


def test_lot_simulation_does_not_fallback_to_raw_for_a_missing_full_week() -> None:
    raw_results = _lot_raw_results()
    mwd_code_data = _weekly_mwd([0.01, 0.01, 0.01])
    only_recent_week = mwd_code_data["weekly_full"].loc[
        mwd_code_data["weekly_full"]["time_period"] == "2026-W29"
    ].copy()
    mwd_code_data["weekly"] = only_recent_week.copy()
    mwd_code_data["weekly_full"] = only_recent_week

    result = _simulate_concentration(
        raw_results=raw_results,
        mwd_code_data=mwd_code_data,
        processing_config={"sheet_hotspot_config": {"enable": True}},
        entity_id_col="lot_id",
    )["Array_Line"]

    historical = result[result["lot_id"] == "LOT-MON"].iloc[0]
    assert historical["defect_panel_count"] == 0
    assert historical["defect_rate"] == 0.0


def test_lot_simulation_allocates_rare_weekly_defects_across_lots() -> None:
    lot_count = 10
    lot_base_info = pd.DataFrame(
        {
            "lot_id": [f"LOT-{index:02d}" for index in range(lot_count)],
            "warehousing_time": ["20260706"] * lot_count,
            "array_input_time": pd.to_datetime(["2026-07-01"] * lot_count),
            "total_panels": [2_000] * lot_count,
            "pass_rate": [1.0] * lot_count,
        }
    )
    existing_code_row = lot_base_info.iloc[[0]].copy()
    existing_code_row["defect_group"] = "Array_Line"
    existing_code_row["defect_desc"] = "RareCode"
    existing_code_row["defect_panel_count"] = 1
    existing_code_row["defect_rate"] = 0.0005
    raw_results = {
        "group_level_summary_for_chart": lot_base_info,
        "code_level_details": {"Array_Line": existing_code_row},
    }
    mwd_code_data = {
        "weekly": pd.DataFrame(
            {
                "time_period": ["2026-W28"],
                "defect_group": ["Array_Line"],
                "defect_desc": ["RareCode"],
                "defect_rate": [0.00005],
            }
        )
    }

    result = _simulate_concentration(
        raw_results=raw_results,
        mwd_code_data=mwd_code_data,
        processing_config={"sheet_hotspot_config": {"enable": True, "random_seed": 7}},
        entity_id_col="lot_id",
    )["Array_Line"]

    assert len(result) == lot_count
    assert result["defect_panel_count"].sum() == 1
    assert (result["defect_panel_count"] > 0).sum() == 1


def test_lot_override_heuristic_uses_lot_iso_week_as_its_base() -> None:
    override_df = pd.DataFrame(
        {
            "lot_id": ["LOT-TUE"],
            "defect_desc": ["CodeA"],
            "override_rate": [0.1],
        }
    )
    lot_base = pd.DataFrame(
        {"lot_id": ["LOT-TUE"], "warehousing_time": ["20260707"]}
    )

    mwd_code_data = _weekly_mwd([0.0, 0.0, 0.0])
    mwd_code_data["weekly"] = mwd_code_data["weekly"].loc[
        mwd_code_data["weekly"]["time_period"] == "2026-W29"
    ].copy()

    result = _calculate_lot_override_rate_heuristic(
        override_df=override_df,
        lot_base_info_df=lot_base,
        mwd_code_data=mwd_code_data,
    )

    expected_rate = 0.01 + 0.1 / 31
    assert result.loc[0, "override_rate_avg"] == expected_rate
