import pandas as pd
from datetime import datetime
from pathlib import Path
from streamlit.testing.v1 import AppTest

from app.sections.qtime_domain.qtime_dashboard import build_qtime_table


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
                "q_spec": 2.5,
                "wait_time": 0.41,
            }
        ]
    )

    table = build_qtime_table(details)

    assert list(table.columns) == [
        "No\n序号",
        "QTime监控",
        "LotID\n批次号",
        "ProductQTY\n产品数量",
        "ProductionType\n产品类型",
        "FromOperation\nFrom站点",
        "ToOperation\nTo站点",
        "T_TimeMeasure\nQ_Time标准",
        "WaitTime\n等待时长",
    ]
    assert table.to_dict("records") == [
        {
            "No\n序号": 1,
            "QTime监控": "M3_DE->M3_STR",
            "LotID\n批次号": "L001",
            "ProductQTY\n产品数量": 1,
            "ProductionType\n产品类型": "P",
            "FromOperation\nFrom站点": "15500",
            "ToOperation\nTo站点": "15600",
            "T_TimeMeasure\nQ_Time标准": 2.5,
            "WaitTime\n等待时长": 0.41,
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


def test_qtime_dashboard_rejects_a_non_positive_time_window() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()
    boundary = datetime(2026, 9, 1, 8, 0)

    app.datetime_input(key="qtime_start_time").set_value(boundary)
    app.datetime_input(key="qtime_end_time").set_value(boundary)
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

    app.selectbox(key="qtime_step_desc").set_value("Shipping->Cutting")
    app.button(key="qtime_search").click().run()

    assert app.info[0].value == "当前筛选条件下暂无 Q-Time 数据。"
    assert not app.dataframe
