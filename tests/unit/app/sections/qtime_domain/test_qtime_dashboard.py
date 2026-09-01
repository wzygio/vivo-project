from datetime import date, datetime
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from app.sections.qtime_domain.qtime_dashboard import (
    build_date_window,
    build_qtime_table,
    default_date_range,
)


def test_qtime_date_filters_default_to_the_last_30_days() -> None:
    today = date(2026, 9, 1)

    assert default_date_range(today) == (date(2026, 8, 2), today)


def test_qtime_date_window_includes_the_selected_end_date() -> None:
    assert build_date_window(date(2026, 8, 2), date(2026, 9, 1)) == (
        datetime(2026, 8, 2),
        datetime(2026, 9, 2),
    )


def test_qtime_table_uses_the_reference_bilingual_column_contract() -> None:
    details = pd.DataFrame(
        [
            {
                "step_desc": "M3_DE->M3_STR",
                "lot_id": "L001",
                "prod_qty": 1,
                "sub_prod_type": "P",
                "f_step": "15500",
                "t_step": "15600",
                "q_spec": 200.4,
                "wait_time": 186.6,
            }
        ]
    )

    table = build_qtime_table(details)

    assert list(table.columns) == [
        "No / 序号",
        "QTime / 监控",
        "LotID / 批次号",
        "ProductQTY / 产品数量",
        "ProductionType / 产品类型",
        "FromOperation / From站点",
        "ToOperation / To站点",
        "T_TimeMeasure / Q_Time标准 (H)",
        "WaitTime / 等待时长 (H)",
    ]
    assert table.to_dict("records") == [
        {
            "No / 序号": 1,
            "QTime / 监控": "M3_DE->M3_STR",
            "LotID / 批次号": "L001",
            "ProductQTY / 产品数量": 1,
            "ProductionType / 产品类型": "P",
            "FromOperation / From站点": "15500",
            "ToOperation / To站点": "15600",
            "T_TimeMeasure / Q_Time标准 (H)": 200,
            "WaitTime / 等待时长 (H)": 187,
        }
    ]


def test_qtime_dashboard_gates_results_until_the_user_queries() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    assert app.subheader[0].value == "北极星QTime监控"
    assert app.info[0].value == "请选择筛选条件并点击“查询”。"

    app.button(key="qtime_search").click().run()

    assert not app.exception
    assert not app.info
    assert len(app.dataframe) == 1
    assert app.dataframe[0].value.shape == (12, 9)
    assert len(app.get("plotly_chart")) == 1


def test_qtime_dashboard_supports_multiple_paths() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    app.multiselect(key="qtime_step_descriptions").set_value(
        ["M3_DE->M3_STR", "PSI_ELA->PSI_PHT"]
    ).run()
    app.button(key="qtime_search").click().run()

    assert not app.exception
    assert set(app.dataframe[0].value["QTime / 监控"]) == {
        "M3_DE->M3_STR",
        "PSI_ELA->PSI_PHT",
    }
    assert len(app.get("plotly_chart")) == 2


def test_qtime_dashboard_rejects_a_non_positive_time_window() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()
    app.date_input(key="qtime_start_date").set_value(date(2026, 9, 2))
    app.date_input(key="qtime_end_date").set_value(date(2026, 9, 1))
    app.button(key="qtime_search").click().run()

    assert app.error[0].value == "结束时间必须晚于开始时间"
    assert not app.dataframe


def test_qtime_dashboard_invalidates_stale_results_when_filters_change() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()
    app.button(key="qtime_search").click().run()
    assert len(app.dataframe) == 1

    app.multiselect(key="qtime_products").set_value(["M678"]).run()

    assert app.info[0].value == "请选择筛选条件并点击“查询”。"
    assert not app.dataframe


def test_qtime_dashboard_shows_a_safe_database_error() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    app.multiselect(key="qtime_products").set_value(["M678"])
    app.button(key="qtime_search").click().run()

    assert app.error[0].value == "Q-Time 数据读取失败，请联系系统管理员确认数据库权限。"
    assert not app.dataframe


def test_qtime_dashboard_explains_an_empty_result() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    app.multiselect(key="qtime_step_descriptions").set_value(
        ["Shipping->Cutting"]
    ).run()
    app.button(key="qtime_search").click().run()

    assert app.info[0].value == "当前筛选条件下暂无 Q-Time 数据。"
    assert not app.dataframe
