from datetime import date, datetime
from pathlib import Path

from streamlit.testing.v1 import AppTest

from app.sections.qtime_domain.qtime_dashboard import (
    build_date_window,
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


def test_qtime_dashboard_gates_results_until_the_user_queries() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    assert app.subheader[0].value == "北极星QTime监控"
    assert [widget.key for widget in app.multiselect] == [
        "qtime_step_descriptions"
    ]
    assert [widget.label for widget in app.date_input] == ["开始日期", "结束日期"]
    assert app.selectbox(key="qtime_shop").label == "厂别"
    assert app.multiselect(key="qtime_step_descriptions").label == "站点"
    assert app.info[0].value == "请选择筛选条件并点击“查询”。"

    app.button(key="qtime_search").click().run()

    assert not app.exception
    assert not app.info
    assert not app.dataframe
    assert len(app.get("plotly_chart")) == 1


def test_qtime_dashboard_supports_multiple_paths() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    app.multiselect(key="qtime_step_descriptions").set_value(
        ["M3_DE->M3_STR", "PSI_ELA->PSI_PHT"]
    ).run()
    app.button(key="qtime_search").click().run()

    assert not app.exception
    assert not app.dataframe
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
    assert len(app.get("plotly_chart")) == 1

    app.multiselect(key="qtime_step_descriptions").set_value(
        ["PSI_ELA->PSI_PHT"]
    ).run()

    assert app.info[0].value == "请选择筛选条件并点击“查询”。"
    assert not app.get("plotly_chart")


def test_qtime_dashboard_shows_a_safe_database_error() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    app.selectbox(key="qtime_shop").set_value("TP").run()
    app.multiselect(key="qtime_step_descriptions").set_value(
        ["TP_OUT->TP_IN"]
    ).run()
    app.button(key="qtime_search").click().run()

    assert app.error[0].value == "Q-Time 数据读取失败，请联系系统管理员确认数据库权限。"
    assert not app.get("plotly_chart")


def test_qtime_dashboard_explains_an_empty_result() -> None:
    fixture_path = Path(__file__).parents[5] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    app.multiselect(key="qtime_step_descriptions").set_value(
        ["Shipping->Cutting"]
    ).run()
    app.button(key="qtime_search").click().run()

    assert app.info[0].value == "当前筛选条件下暂无 Q-Time 数据。"
    assert not app.get("plotly_chart")
