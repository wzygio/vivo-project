from pathlib import Path

import pandas as pd

from src.shared_kernel.config import ConfigLoader
from src.yield_domain.core.sheet_lot_processor import calculate_lot_defect_rates
from src.yield_domain.core.trend_regulator import TrendRegulator


TARGET_CODE = "G3\u4eae\u70b9"
TARGET_GROUP = "Array_Pixel"


def _make_lot_panels(lot_id: str, defect_count: int) -> list[dict]:
    rows: list[dict] = []
    for panel_index in range(1200):
        has_defect = panel_index < defect_count
        rows.append(
            {
                "batch_no": lot_id,
                "lot_id": lot_id,
                "sheet_id": f"{lot_id}{panel_index // 190:02d}",
                "panel_id": f"{lot_id}{panel_index:04d}",
                "warehousing_time": "20260612",
                "prod_code": "M673",
                "defect_code": TARGET_CODE if has_defect else None,
                "defect_desc": TARGET_CODE if has_defect else None,
                "defect_group": TARGET_GROUP if has_defect else None,
            }
        )
    return rows


def test_trend_regulator_caps_after_lower_floor_when_spec_bounds_are_equal() -> None:
    daily_df = pd.DataFrame(
        [
            {
                "warehousing_time": pd.Timestamp("2026-06-12"),
                "total_panels": 10000,
                "defect_group": TARGET_GROUP,
                "defect_desc": TARGET_CODE,
                "defect_panel_count": 80,
            }
        ]
    )

    regulated = TrendRegulator.regulate_code_daily_base(
        daily_df,
        warning_lines={TARGET_CODE: {"upper": 0.004, "lower": 0.004}},
    )

    assert regulated.loc[0, "defect_panel_count"] <= 40


def test_lot_simulation_expands_positive_daily_code_to_all_same_day_lots() -> None:
    panel_df = pd.DataFrame(
        _make_lot_panels("LTEST0001", defect_count=1)
        + _make_lot_panels("LTEST0002", defect_count=0)
    )
    mwd_code_data = {
        "daily_full": pd.DataFrame(
            [
                {
                    "time_period": "2026-06-12",
                    "warehousing_time": pd.Timestamp("2026-06-12"),
                    "total_panels": 2400,
                    "defect_group": TARGET_GROUP,
                    "defect_desc": TARGET_CODE,
                    "defect_panel_count": 10,
                    "defect_rate": 0.004,
                }
            ]
        )
    }
    config = ConfigLoader.load_config("M673")
    config.processing["sheet_hotspot_config"] = {"enable": True, "random_seed": 2025}
    config.processing["defect_capping"] = {"enable": False}

    results = calculate_lot_defect_rates(
        panel_details_df=panel_df,
        array_input_times_df=pd.DataFrame(),
        mwd_code_data=mwd_code_data,
        config=config,
        product_dir=Path("resources/M673"),
        warning_lines={},
    )

    assert results is not None
    code_df = results["code_level_details"][TARGET_GROUP]
    g3_rows = code_df[code_df["defect_desc"] == TARGET_CODE]
    assert set(g3_rows["lot_id"]) == {"LTEST0001", "LTEST0002"}
    assert (g3_rows["defect_rate"] > 0).all()
