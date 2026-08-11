"""AOI_TT 核心聚合测试：period 趋势（分母=明细自身 distinct sheet）、检测片数、By Lot/Sheet、规格线匹配。"""

from datetime import date

import pandas as pd

from src.inline_domain.core.aoi_tt.aoi_tt_calculator import (
    attach_spec_values,
    build_lot_point_df,
    build_period_throughput_df,
    build_period_trend_df,
    build_sheet_point_df,
)

END_DATE = date(2026, 8, 10)


def _details(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["start_time"] = pd.to_datetime(df["start_time"])
    return df


def test_period_trend_computes_ratio_per_tt_and_period() -> None:
    details = _details(
        [
            # 8月9日：TDSUM 两片共 5 个；DSUM_L 一片 1 个
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 08:00", "sheet_id": "S1", "lot_id": "L1", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 3},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 09:00", "sheet_id": "S2", "lot_id": "L1", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 2},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 10:00", "sheet_id": "S2", "lot_id": "L1", "step_id": "11620", "tt_name": "DSUM_L", "tt_qty": 1},
            # 8月10日：TDSUM 4 个
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 08:00", "sheet_id": "S3", "lot_id": "L2", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 4},
        ]
    )

    trend = build_period_trend_df(details, END_DATE)

    day_rows = trend[trend["period_type"] == "day"].set_index(["period_label", "tt_name"])
    # 分母 = 同 period 同站点明细自身的 distinct sheet 数（TDSUM/DSUM_L 共用同一分母）
    assert day_rows.loc[("2026-08-09", "TDSUM"), "tt_qty"] == 5
    assert day_rows.loc[("2026-08-09", "TDSUM"), "sheet_qty"] == 2
    assert day_rows.loc[("2026-08-09", "TDSUM"), "value"] == 5 / 2
    assert day_rows.loc[("2026-08-09", "DSUM_L"), "value"] == 1 / 2
    assert day_rows.loc[("2026-08-10", "TDSUM"), "value"] == 4 / 1
    # DSUM_L 在 8月10日无数据 → 不出现该行
    assert ("2026-08-10", "DSUM_L") not in day_rows.index


def test_period_trend_axis_skips_empty_days_forward() -> None:
    # TT 数据只分布在 8月8/9/10 三天 → 天轴只取最近 3 个非空天，而非固定 7 天
    details = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": f"2026-08-{d:02d} 08:00", "sheet_id": f"S{d}", "lot_id": "L1", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 1}
            for d in (8, 9, 10)
        ]
    )

    trend = build_period_trend_df(details, END_DATE)

    day_labels = sorted(trend[trend["period_type"] == "day"]["period_label"].unique())
    assert day_labels == ["2026-08-08", "2026-08-09", "2026-08-10"]


def test_period_trend_zero_denominator_yields_nan_not_exception() -> None:
    # sheet_id 缺失 → 该 period 的 distinct sheet 数为 0 → value 记 NaN 而非除零抛错
    details = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 08:00", "sheet_id": None, "lot_id": "L1", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 2},
        ]
    )

    trend = build_period_trend_df(details, END_DATE)

    assert not trend.empty
    assert trend["value"].isna().all()
    assert (trend["sheet_qty"] == 0).all()


def test_period_throughput_zero_fills_periods_without_step_data() -> None:
    # 站点 11620 仅 8月10日有数据；站点 21320 在 8月9/10 两天都有 → 天轴覆盖两天
    details = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 08:00", "sheet_id": "S1", "lot_id": "L1", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 2},
            {"factory": "OLED", "prod_code": "M678", "start_time": "2026-08-09 08:00", "sheet_id": "G1", "lot_id": "L1", "step_id": "21320", "tt_name": "DSUM_L", "tt_qty": 1},
            {"factory": "OLED", "prod_code": "M678", "start_time": "2026-08-10 08:00", "sheet_id": "G2", "lot_id": "L1", "step_id": "21320", "tt_name": "DSUM_L", "tt_qty": 1},
        ]
    )

    throughput = build_period_throughput_df(details, END_DATE)

    day_rows = throughput[throughput["period_type"] == "day"].set_index(["period_label", "step_id"])
    assert sorted(day_rows.index.get_level_values("period_label").unique()) == ["2026-08-09", "2026-08-10"]
    # 8月9日站点 11620 无检测 → 0 填充；行仍然存在
    assert day_rows.loc[("2026-08-09", "11620"), "sheet_qty"] == 0
    assert day_rows.loc[("2026-08-10", "11620"), "sheet_qty"] == 1
    assert day_rows.loc[("2026-08-09", "21320"), "sheet_qty"] == 1
    assert day_rows.loc[("2026-08-10", "21320"), "sheet_qty"] == 1


def test_period_throughput_counts_distinct_sheets() -> None:
    # 同一片在同一 period 出现多个 TT 参数行，检测片数只计一次
    details = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 08:00", "sheet_id": "S1", "lot_id": "L1", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 2},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 08:00", "sheet_id": "S1", "lot_id": "L1", "step_id": "11620", "tt_name": "DSUM_L", "tt_qty": 1},
        ]
    )

    throughput = build_period_throughput_df(details, END_DATE)

    day_row = throughput[throughput["period_type"] == "day"].iloc[0]
    assert day_row["sheet_qty"] == 1
    assert day_row["step_id"] == "11620"


def test_period_throughput_empty_input_returns_empty() -> None:
    throughput = build_period_throughput_df(pd.DataFrame(), END_DATE)
    assert throughput.empty


def test_lot_point_aggregates_qty_per_lot_ordered_by_first_time() -> None:
    details = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-02 08:00", "sheet_id": "S1", "lot_id": "LOT-B", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 2},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-01 08:00", "sheet_id": "S2", "lot_id": "LOT-A", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 1},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-03 08:00", "sheet_id": "S3", "lot_id": "LOT-B", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 3},
        ]
    )

    lots = build_lot_point_df(details)

    assert list(lots["lot_id"]) == ["LOT-A", "LOT-B"]  # 按首次过货时间排序
    assert list(lots["tt_qty"]) == [1, 5]


def test_sheet_point_aggregates_qty_per_sheet() -> None:
    details = _details(
        [
            {"factory": "OLED", "prod_code": "M678", "start_time": "2026-08-01 08:00", "sheet_id": "G1", "lot_id": "L1", "step_id": "21320", "tt_name": "DSUM_L", "tt_qty": 2},
            {"factory": "OLED", "prod_code": "M678", "start_time": "2026-08-01 09:00", "sheet_id": "G1", "lot_id": "L1", "step_id": "21320", "tt_name": "DSUM_L", "tt_qty": 1},
            {"factory": "OLED", "prod_code": "M678", "start_time": "2026-08-01 10:00", "sheet_id": "G2", "lot_id": "L1", "step_id": "21320", "tt_name": "DSUM_L", "tt_qty": 0},
        ]
    )

    sheets = build_sheet_point_df(details)

    assert list(sheets["sheet_id"]) == ["G1", "G2"]
    assert list(sheets["tt_qty"]) == [3, 0]


def test_attach_spec_values_matches_by_step_and_tt_name() -> None:
    # 规格表无 factory 列（step_id 全局唯一隐含厂别），按 step_id+tt_name 匹配 usl/ucl
    spec_df = pd.DataFrame(
        [
            {"prod_code": "M678", "step_id": "11620", "tt_name": "TDSUM", "usl": 5.0, "ucl": 3.0},
            {"prod_code": "M678", "step_id": "21320", "tt_name": "DSUM_L", "usl": 2.0, "ucl": 1.0},
        ]
    )
    base = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM"},
            {"factory": "OLED", "step_id": "21320", "tt_name": "DSUM_L"},
            {"factory": "TP", "step_id": "43620", "tt_name": "TOTAL_O_L"},
        ]
    )

    result = attach_spec_values(base, spec_df)

    assert result.loc[0, "usl"] == 5.0
    assert result.loc[0, "ucl"] == 3.0
    assert result.loc[1, "usl"] == 2.0
    assert result.loc[1, "ucl"] == 1.0
    # 无规格的 TOTAL_O_L → NaN 而非抛错
    assert pd.isna(result.loc[2, "usl"])
    assert pd.isna(result.loc[2, "ucl"])


def test_attach_spec_values_with_empty_spec_yields_na_columns() -> None:
    base = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM"},
        ]
    )

    result = attach_spec_values(base, pd.DataFrame())

    assert "usl" in result.columns
    assert "ucl" in result.columns
    assert result["usl"].isna().all()
    assert result["ucl"].isna().all()
