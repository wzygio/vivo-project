from pathlib import Path
from datetime import date

import pandas as pd
from streamlit.testing.v1 import AppTest

from app.sections.indicator_domain.ijp.dashboard import _chunked, build_ijp_table

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


def test_ijp_signature_works_with_pre_cutoff_config_class(monkeypatch):
    from src.shared_kernel.config import ConfigLoader
    from app.sections.indicator_domain.ijp.dashboard import _filter_signature

    monkeypatch.delattr(ConfigLoader, "get_report_cutoff_policy")
    signature = _filter_signature(["M626"], "2026-09-11")
    assert signature[:2] == (("M626",), "2026-09-11")
    assert signature[-1].startswith("report-cutoff-v1:")


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
    assert not app.date_input
    assert not app.datetime_input
    assert not app.number_input
    labels = {widget.label for widget in app.multiselect}
    assert "产品型号" in labels
    assert "设备" not in labels
    assert "边框" not in labels
    assert "产品名称" not in labels
    assert "工单类型" not in labels
    assert not app.text_input
    assert "Cycle" not in labels
    filter_columns = app.get("column")
    assert len(filter_columns) == 4
    assert all(column.weight == 0.25 for column in filter_columns)
    assert app.info[0].value == "请选择筛选条件并点击“查询”。"
    assert not app.dataframe

    app.button(key="ijp_search").click().run()

    assert not app.exception
    assert not app.info
    assert not app.dataframe
    assert len(app.get("plotly_chart")) == 4
    chart_columns = app.get("column")[4:]
    assert [column.weight for column in chart_columns] == [0.5, 0.5, 1.0, 1.0]
    assert [expander.label for expander in app.expander] == [
        "产品：M626",
        "线体：3CEE01",
        "线体：3CEE02",
        "产品：M678",
        "线体：3CEE04",
    ]
    assert all(expander.proto.expanded for expander in app.expander)


def test_chunked_limits_each_chart_row_to_three_items() -> None:
    assert _chunked(("P1", "P2", "P3", "P4"), 3) == (
        ("P1", "P2", "P3"),
        ("P4",),
    )


def test_ijp_dashboard_shows_a_safe_database_error() -> None:
    app = AppTest.from_file(str(FIXTURE_PATH)).run()

    app.multiselect(key="ijp_product_codes").set_value(["M678"])
    app.button(key="ijp_search").click().run()

    assert app.error[0].value == "IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。"
    assert not app.dataframe


def test_ijp_dashboard_explains_an_empty_result() -> None:
    app = AppTest.from_file(str(FIXTURE_PATH)).run()

    app.multiselect(key="ijp_codes").set_value(["C3DM5"])
    app.button(key="ijp_search").click().run()

    assert app.info[0].value == "当前筛选条件下暂无 IJP 溢流数据。"
    assert not app.dataframe


def test_ijp_dashboard_invalidates_stale_results_when_filters_change() -> None:
    app = AppTest.from_file(str(FIXTURE_PATH)).run()
    app.button(key="ijp_search").click().run()
    assert app.get("plotly_chart")
    assert not app.dataframe

    app.multiselect(key="ijp_codes").set_value(["C3DM1"]).run()

    assert app.info[0].value == "请选择筛选条件并点击“查询”。"
    assert not app.dataframe


def test_fixed_window_and_period_chart_without_details():
    app = AppTest.from_file(str(FIXTURE_PATH)).run()
    app.button(key="ijp_search").click().run()
    assert not app.date_input
    assert any("2026/08/01 至 2026/09/07" in item.value for item in app.caption)
    assert len(app.get("plotly_chart")) == 4
    assert not app.dataframe
