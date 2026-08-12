"""AOI_RS 核心聚合测试：period 趋势（分子/分母）、By Lot、By Sheet、规格线匹配。"""

from datetime import date

import pandas as pd

from src.inline_domain.core.aoi_rs.aoi_rs_calculator import (
    attach_spec_values,
    build_lot_point_df,
    build_period_trend_df,
    build_sheet_point_df,
)

END_DATE = date(2026, 8, 10)


def _details(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["start_time"] = pd.to_datetime(df["start_time"])
    return df


def test_period_trend_computes_ratio_per_code_and_period() -> None:
    details = _details(
        [
            # 8月9日：A1PPS 两片共 5 个；A2CIP 一片 1 个
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 08:00", "sheet_id": "S1", "lot_id": "L1", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 3},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 09:00", "sheet_id": "S2", "lot_id": "L1", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 2},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 10:00", "sheet_id": "S2", "lot_id": "L1", "step_id": "11629", "rs_code": "A2CIP", "code_qty": 1},
            # 8月10日：A1PPS 4 个
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 08:00", "sheet_id": "S3", "lot_id": "L2", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 4},
        ]
    )
    pass_through = _details(
        [
            # 8月9日过货 4 片（含无 RS 的片），8月10日过货 2 片
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 07:00", "sheet_id": "S1", "step_id": "11629"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 07:10", "sheet_id": "S2", "step_id": "11629"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 07:20", "sheet_id": "S8", "step_id": "11629"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 07:30", "sheet_id": "S9", "step_id": "11629"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 07:00", "sheet_id": "S3", "step_id": "11629"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 07:10", "sheet_id": "S4", "step_id": "11629"},
        ]
    )

    trend = build_period_trend_df(details, pass_through, END_DATE)

    day_rows = trend[trend["period_type"] == "day"].set_index(["period_label", "rs_code"])
    assert day_rows.loc[("2026-08-09", "A1PPS"), "rs_qty"] == 5
    assert day_rows.loc[("2026-08-09", "A1PPS"), "sheet_qty"] == 4
    assert day_rows.loc[("2026-08-09", "A1PPS"), "value"] == 5 / 4
    assert day_rows.loc[("2026-08-09", "A2CIP"), "value"] == 1 / 4
    assert day_rows.loc[("2026-08-10", "A1PPS"), "value"] == 4 / 2
    # A2CIP 在 8月10日无数据 → 不出现该行或值为空
    assert ("2026-08-10", "A2CIP") not in day_rows.index


def test_period_trend_axis_skips_empty_days_forward() -> None:
    # RS 数据只分布在 8月8/9/10 三天 → 天轴只取最近 3 个非空天，而非固定 7 天
    details = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": f"2026-08-{d:02d} 08:00", "sheet_id": f"S{d}", "lot_id": "L1", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 1}
            for d in (8, 9, 10)
        ]
    )
    pass_through = details[["factory", "prod_code", "start_time", "sheet_id", "step_id"]].copy()

    trend = build_period_trend_df(details, pass_through, END_DATE)

    day_labels = sorted(trend[trend["period_type"] == "day"]["period_label"].unique())
    assert day_labels == ["2026-08-08", "2026-08-09", "2026-08-10"]


def test_period_trend_zero_denominator_yields_nan_not_exception() -> None:
    details = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 08:00", "sheet_id": "S1", "lot_id": "L1", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 2},
        ]
    )
    pass_through = pd.DataFrame(
        columns=["factory", "prod_code", "start_time", "sheet_id", "step_id"]
    )

    trend = build_period_trend_df(details, pass_through, END_DATE)

    assert not trend.empty
    assert trend["value"].isna().all()
    assert (trend["sheet_qty"] == 0).all()


def test_lot_point_computes_avg_per_sheet_using_pass_through_denominator() -> None:
    details = _details(
        [
            # LOT-B：两片共 5 个；LOT-A：一片 1 个
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-02 08:00", "sheet_id": "S1", "lot_id": "LOT-B", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 2},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-01 08:00", "sheet_id": "S2", "lot_id": "LOT-A", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 1},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-03 08:00", "sheet_id": "S3", "lot_id": "LOT-B", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 3},
        ]
    )
    pass_through = _details(
        [
            # LOT-B 实际过货 4 片（含无 RS 记录的片），LOT-A 过货 2 片
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-02 07:00", "sheet_id": "S1", "step_id": "11629", "lot_id": "LOT-B"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-02 07:10", "sheet_id": "S3", "step_id": "11629", "lot_id": "LOT-B"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-02 07:20", "sheet_id": "S4", "step_id": "11629", "lot_id": "LOT-B"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-02 07:30", "sheet_id": "S5", "step_id": "11629", "lot_id": "LOT-B"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-01 07:00", "sheet_id": "S2", "step_id": "11629", "lot_id": "LOT-A"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-01 07:10", "sheet_id": "S6", "step_id": "11629", "lot_id": "LOT-A"},
        ]
    )

    lots = build_lot_point_df(details, pass_through)

    assert list(lots["lot_id"]) == ["LOT-A", "LOT-B"]  # 按首次过货时间排序
    assert list(lots["rs_qty"]) == [1, 5]
    assert list(lots["sheet_qty"]) == [2, 4]
    assert list(lots["value"]) == [0.5, 1.25]  # Σcode_qty ÷ Lot 内过货片数


def test_lot_point_zero_denominator_yields_nan() -> None:
    details = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-02 08:00", "sheet_id": "S1", "lot_id": "LOT-B", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 2},
        ]
    )
    pass_through = pd.DataFrame(
        columns=["factory", "prod_code", "start_time", "sheet_id", "step_id", "lot_id"]
    )

    lots = build_lot_point_df(details, pass_through)

    assert lots["sheet_qty"].iloc[0] == 0
    assert pd.isna(lots["value"].iloc[0])


def test_sheet_point_aggregates_qty_per_sheet() -> None:
    details = _details(
        [
            {"factory": "OLED", "prod_code": "M678", "start_time": "2026-08-01 08:00", "sheet_id": "G1", "lot_id": "L1", "step_id": "21329", "rs_code": "C4BP3", "code_qty": 2},
            {"factory": "OLED", "prod_code": "M678", "start_time": "2026-08-01 09:00", "sheet_id": "G1", "lot_id": "L1", "step_id": "21329", "rs_code": "C4BP3", "code_qty": 1},
            {"factory": "OLED", "prod_code": "M678", "start_time": "2026-08-01 10:00", "sheet_id": "G2", "lot_id": "L1", "step_id": "21329", "rs_code": "C4BP3", "code_qty": 0},
        ]
    )

    sheets = build_sheet_point_df(details)

    assert list(sheets["sheet_id"]) == ["G1", "G2"]
    assert list(sheets["rs_qty"]) == [3, 0]


def test_attach_spec_values_matches_chart_type_flags() -> None:
    spec_df = pd.DataFrame(
        [
            {"prod_code": "M678", "factory": "ARRAY", "type_flag": "MWD_RATIO", "step_id": "11629", "rs_code": "A1PPS", "code_desc": "d", "spec": 0.5},
            {"prod_code": "M678", "factory": "ARRAY", "type_flag": "LOT_RATIO", "step_id": "11629", "rs_code": "A1PPS", "code_desc": "d", "spec": 30},
            {"prod_code": "M678", "factory": "ARRAY", "type_flag": "SHEET_ID", "step_id": "11629", "rs_code": "A1PPS", "code_desc": "d", "spec": 4},
            {"prod_code": "M678", "factory": "OLED", "type_flag": "GLASS_ID", "step_id": "21329", "rs_code": "C4BP3", "code_desc": "d", "spec": 6},
        ]
    )
    base = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS"},
            {"factory": "OLED", "step_id": "21329", "rs_code": "C4BP3"},
            {"factory": "TP", "step_id": "43629", "rs_code": "T3DMR"},
        ]
    )

    mwd = attach_spec_values(base, spec_df, chart_kind="mwd")
    lot = attach_spec_values(base, spec_df, chart_kind="lot")
    sheet = attach_spec_values(base, spec_df, chart_kind="sheet")

    assert mwd.loc[0, "spec"] == 0.5
    assert lot.loc[0, "spec"] == 30
    assert sheet.loc[0, "spec"] == 4
    assert sheet.loc[1, "spec"] == 6  # GLASS_ID 适用于 By Sheet 图
    # 无规格的 T3DMR → NaN 而非抛错
    assert pd.isna(mwd.loc[2, "spec"])
