from pathlib import Path

from streamlit.testing.v1 import AppTest

from src.indicator_domain.application.qtime.dtos import QTimeStepOption


M3_DE_TO_M3_STR = QTimeStepOption("M3_DE->M3_STR", "15500", "15600")
PSI_ELA_TO_PSI_PHT = QTimeStepOption("PSI_ELA->PSI_PHT", "11300", "11400")
SHIPPING_TO_CUTTING = QTimeStepOption("Shipping->Cutting", "2X999", "31000")


def test_qtime_dashboard_gates_results_until_the_user_queries() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    assert app.subheader[0].value == "北极星QTime监控"
    assert [widget.key for widget in app.multiselect] == [
        "qtime_step_descriptions"
    ]
    assert not app.date_input
    assert app.selectbox(key="qtime_shop").label == "厂别"
    assert app.multiselect(key="qtime_step_descriptions").label == "站点"
    assert app.info[0].value == "请选择筛选条件并点击“查询”。"

    app.button(key="qtime_search").click().run()

    assert not app.exception
    assert not app.info
    assert len(app.dataframe) == 1
    assert len(app.get("plotly_chart")) == 1


def test_qtime_dashboard_labels_station_paths_with_original_codes() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    assert app.multiselect(key="qtime_step_descriptions").options[:2] == [
        "15500 → 15600｜M3_DE->M3_STR",
        "11300 → 11400｜PSI_ELA->PSI_PHT",
    ]


def test_qtime_dashboard_renders_the_alert_center_and_decoration_admin() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()
    app.button(key="qtime_search").click().run()

    expander_labels = [expander.label for expander in app.expander]
    assert "Q-Time 超规预警中心" in expander_labels
    assert "开发者后台：Q-Time 超规数据修饰" in expander_labels
    assert any("1 条已确认真实超规" in message.value for message in app.error)


def test_qtime_dashboard_supports_multiple_paths() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    app.multiselect(key="qtime_step_descriptions").set_value(
        [M3_DE_TO_M3_STR, PSI_ELA_TO_PSI_PHT]
    ).run()
    app.button(key="qtime_search").click().run()

    assert not app.exception
    assert len(app.dataframe) == 1
    assert len(app.get("plotly_chart")) == 2


def test_qtime_dashboard_invalidates_stale_results_when_filters_change() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()
    app.button(key="qtime_search").click().run()
    assert len(app.get("plotly_chart")) == 1

    app.multiselect(key="qtime_step_descriptions").set_value(
        [PSI_ELA_TO_PSI_PHT]
    ).run()

    assert app.info[0].value == "请选择筛选条件并点击“查询”。"
    assert not app.get("plotly_chart")


def test_qtime_dashboard_shows_a_safe_database_error() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    app.selectbox(key="qtime_shop").set_value("TP").run()
    app.multiselect(key="qtime_step_descriptions").set_value(
        [QTimeStepOption("TP_OUT->TP_IN", "31000", "31100")]
    ).run()
    app.button(key="qtime_search").click().run()

    assert app.error[0].value == "Q-Time 数据读取失败，请联系系统管理员确认数据库权限。"
    assert not app.get("plotly_chart")


def test_qtime_dashboard_explains_an_empty_result() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    app.multiselect(key="qtime_step_descriptions").set_value(
        [SHIPPING_TO_CUTTING]
    ).run()
    app.button(key="qtime_search").click().run()

    assert app.info[0].value == "当前筛选条件下暂无 Q-Time 数据。"
    assert not app.get("plotly_chart")
