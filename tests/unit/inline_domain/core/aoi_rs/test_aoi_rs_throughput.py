"""AOI_RS 过货量聚合测试：全 period 轴覆盖，供趋势图柱状图使用。"""

from datetime import date

import pandas as pd

from src.inline_domain.core.aoi_rs.aoi_rs_calculator import build_period_throughput_df

END_DATE = date(2026, 8, 10)


def _details(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["start_time"] = pd.to_datetime(df["start_time"])
    return df


def test_throughput_covers_all_axis_periods_even_without_rs_data() -> None:
    # RS 数据只在 8月10日；过货在 8月9/10 两天
    details = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 08:00", "sheet_id": "S1", "lot_id": "L1", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 2},
        ]
    )
    pass_through = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 07:00", "sheet_id": "S8", "step_id": "11629"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-09 07:10", "sheet_id": "S9", "step_id": "11629"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 07:00", "sheet_id": "S1", "step_id": "11629"},
        ]
    )

    throughput = build_period_throughput_df(details, pass_through, END_DATE)

    # period 轴由 RS 数据可用性决定（最近 2 月/3 周/7 天），每个 period 都必须有行
    day_rows = throughput[throughput["period_type"] == "day"].set_index("period_label")
    assert "2026-08-10" in day_rows.index
    assert day_rows.loc["2026-08-10", "sheet_qty"] == 1
    # 8月9日虽无 RS 数据，但只要有 axis 覆盖就应有过货量记录；
    # axis 由 RS 数据决定只含 8月10日 → 日轴恰好一天
    assert list(day_rows.index) == ["2026-08-10"]
    # 月/周 period 也应有行且过货量跨整天聚合
    month_row = throughput[throughput["period_type"] == "month"].iloc[0]
    assert month_row["sheet_qty"] == 3


def test_throughput_zero_fill_and_distinct_count() -> None:
    details = _details(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 08:00", "sheet_id": "S1", "lot_id": "L1", "step_id": "11629", "rs_code": "A1PPS", "code_qty": 2},
        ]
    )
    pass_through = _details(
        [
            # 同一片重复过站只计一次；另有其他站点过货不应混入
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 07:00", "sheet_id": "S1", "step_id": "11629"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 07:30", "sheet_id": "S1", "step_id": "11629"},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": "2026-08-10 07:00", "sheet_id": "S2", "step_id": "99999"},
        ]
    )

    throughput = build_period_throughput_df(details, pass_through, END_DATE)

    day_row = throughput[throughput["period_type"] == "day"].iloc[0]
    assert day_row["sheet_qty"] == 1  # distinct 去重 + 按站点分组
    assert day_row["step_id"] == "11629"


def test_throughput_empty_inputs_return_empty() -> None:
    throughput = build_period_throughput_df(pd.DataFrame(), pd.DataFrame(), END_DATE)
    assert throughput.empty
