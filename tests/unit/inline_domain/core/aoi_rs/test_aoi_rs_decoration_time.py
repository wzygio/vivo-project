"""AOI_RS OOS 明细的时间列测试：sheet_start_time 来自图表点帧的 first_start_time。"""

from datetime import date

import pandas as pd

from src.inline_domain.core.aoi_rs.aoi_rs_decoration import (
    AOI_RS_OOS_DETAIL_COLUMNS,
    AOI_RS_OOS_KEY_COLUMNS,
    build_aoi_rs_oos_detail,
)


def _spec_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "type_flag": "LOT_RATIO", "spec": 1.0},
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "type_flag": "SHEET_ID", "spec": 1.0},
        ]
    )


def _lot_points() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "lot_id": "L1",
                "rs_qty": 5,
                "sheet_qty": 2,
                "value": 2.5,
                "first_start_time": pd.Timestamp("2026-08-20 08:00"),
            }
        ]
    )


def _sheet_points() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "sheet_id": "S1",
                "rs_qty": 3,
                "first_start_time": pd.Timestamp("2026-08-21 09:30"),
            }
        ]
    )


def test_detail_includes_sheet_start_time_from_point_frames() -> None:
    detail = build_aoi_rs_oos_detail(_lot_points(), _sheet_points(), _spec_df(), "M678")

    assert "sheet_start_time" in detail.columns
    by_kind = detail.set_index("chart_kind")
    assert by_kind.loc["lot", "sheet_start_time"] == pd.Timestamp("2026-08-20 08:00")
    assert by_kind.loc["sheet", "sheet_start_time"] == pd.Timestamp("2026-08-21 09:30")


def test_time_column_is_not_part_of_merge_keys() -> None:
    assert "sheet_start_time" not in AOI_RS_OOS_KEY_COLUMNS
    # 新列位于明细列清单末尾，向后兼容旧读取方按列名访问
    assert AOI_RS_OOS_DETAIL_COLUMNS[-1] == "sheet_start_time"


def test_missing_first_start_time_yields_nat_not_exception() -> None:
    lot_points = _lot_points().drop(columns=["first_start_time"])
    detail = build_aoi_rs_oos_detail(lot_points, pd.DataFrame(), _spec_df(), "M678")

    assert "sheet_start_time" in detail.columns
    assert detail["sheet_start_time"].isna().all()
