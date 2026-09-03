from datetime import date, datetime, time
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from app.sections.indicator_domain.ijp.dashboard import (
    TARGET_HELP,
    build_ijp_table,
    date_range_to_datetimes,
)

FIXTURE_PATH = (
    Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "ijp_app.py"
)


def _details() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "print_time": "2026-08-31 08:00:00",
                "productcode": "M626",
                "glass_id": "L3N464E03182",
                "printer": "3CEE01-IK2-PR1",
                "panel_id": "L3N464E03182CA",
                "image_url": "http://10.73.17.41/IMG_WEB/V3/x.jpg",
                "panel_location": "BOTTOM",
                "rs_code": "C3DM1",
                "code_ratio": 0.667,
            },
            {
                "print_time": "2026-08-31 08:10:00",
                "productcode": "M626",
                "glass_id": "L3N464E03182",
                "printer": "3CEE01-IK2-PR1",
                "panel_id": "L3N464E03182CB",
                "image_url": "http://10.73.17.41/IMG_WEB/V3/y.jpg",
                "panel_location": "KONGTOP",
                "rs_code": "C3RA1",
                "code_ratio": 0.333,
            },
        ]
    )


def test_ijp_table_uses_the_reference_column_contract_and_total_row() -> None:
    table = build_ijp_table(_details())

    assert list(table.columns) == [
        "No\n序号",
        "Print Time",
        "ProductCode",
        "Glass ID",
        "Printer",
        "Panel ID",
        "原图",
        "Panel Location",
        "CODE_RATIO",
    ]
    assert len(table) == 3
    total = table.iloc[-1]
    assert total["No\n序号"] == "Total"
    assert total["CODE_RATIO"] == 1.0
    assert table.iloc[0]["Printer"] == "3CEE01-IK2-PR1"


def test_ijp_table_handles_empty_and_non_numeric_ratios() -> None:
    empty = build_ijp_table(pd.DataFrame())
    assert empty.empty
    assert "CODE_RATIO" in empty.columns

    details = _details()
    details["code_ratio"] = ["bad", None]
    table = build_ijp_table(details)
    assert table.iloc[-1]["CODE_RATIO"] == 0


def test_ijp_dashboard_gates_results_until_the_user_queries() -> None:
    app = AppTest.from_file(str(FIXTURE_PATH)).run()

    assert app.subheader[0].value == "OLED IJP 溢流监控"
    assert len(app.date_input) == 2
    assert not app.datetime_input
    assert app.number_input(key="ijp_target").help == TARGET_HELP
    assert app.info[0].value == "请选择筛选条件并点击“查询”。"
    assert not app.dataframe

    app.button(key="ijp_search").click().run()

    assert not app.exception
    assert not app.info
    assert len(app.dataframe) == 1
    assert len(app.get("plotly_chart")) == 1


def test_ijp_dashboard_rejects_an_inverted_time_window() -> None:
    app = AppTest.from_file(str(FIXTURE_PATH)).run()

    app.date_input(key="ijp_start_time").set_value(date(2026, 9, 2))
    app.date_input(key="ijp_end_time").set_value(date(2026, 9, 1))
    app.button(key="ijp_search").click().run()

    assert app.error[0].value == "结束日期不能早于开始日期"
    assert not app.dataframe


def test_ijp_dashboard_shows_a_safe_database_error() -> None:
    app = AppTest.from_file(str(FIXTURE_PATH)).run()

    app.multiselect(key="ijp_product_codes").set_value(["M678"])
    app.button(key="ijp_search").click().run()

    assert app.error[0].value == "IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。"
    assert not app.dataframe


def test_ijp_dashboard_explains_an_empty_result() -> None:
    app = AppTest.from_file(str(FIXTURE_PATH)).run()

    app.multiselect(key="ijp_codes").set_value(["C3BH2"])
    app.button(key="ijp_search").click().run()

    assert app.info[0].value == "当前筛选条件下暂无 IJP 溢流数据。"
    assert not app.dataframe


def test_ijp_dashboard_invalidates_stale_results_when_filters_change() -> None:
    app = AppTest.from_file(str(FIXTURE_PATH)).run()
    app.button(key="ijp_search").click().run()
    assert len(app.dataframe) == 1

    app.multiselect(key="ijp_codes").set_value(["C3DM1"]).run()

    assert app.info[0].value == "请选择筛选条件并点击“查询”。"
    assert not app.dataframe


def test_date_range_to_datetimes_covers_whole_calendar_days() -> None:
    start_time, end_time = date_range_to_datetimes(
        date(2026, 9, 2),
        date(2026, 9, 3),
    )

    assert start_time == datetime.combine(date(2026, 9, 2), time.min)
    assert end_time == datetime.combine(date(2026, 9, 3), time.max)
