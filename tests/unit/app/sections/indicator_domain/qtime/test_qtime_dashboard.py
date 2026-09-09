from pathlib import Path
import json
from types import SimpleNamespace
import pytest

from streamlit.testing.v1 import AppTest

from app.manager.session_manager import SessionManager
from src.indicator_domain.application.qtime.dtos import QTimeStepOption


M3_DE_TO_M3_STR = QTimeStepOption("M3_DE->M3_STR", "15500", "15600")
PSI_ELA_TO_PSI_PHT = QTimeStepOption("PSI_ELA->PSI_PHT", "11300", "11400")
SHIPPING_TO_CUTTING = QTimeStepOption("Shipping->Cutting", "2X999", "31000")


def test_empty_stale_result_warns_instead_of_claiming_no_current_data():
    fixture = Path(__file__).parents[6] / "tests/e2e/fixtures/qtime_app.py"
    app = AppTest.from_file(str(fixture)).run()
    app.query_params["fixture_health"] = "stale"
    app.multiselect(key="qtime_step_descriptions").set_value([SHIPPING_TO_CUTTING]).run()
    app.button(key="qtime_search").click().run()
    assert not app.exception
    assert any("旧快照" in item.value for item in app.warning)
    assert not app.success
    assert not any("当前筛选条件下暂无" in item.value for item in app.info)


def test_previous_day_session_result_requires_query_again(monkeypatch):
    from datetime import date, timedelta
    from app.sections.indicator_domain.qtime import dashboard

    fixture = Path(__file__).parents[6] / "tests/e2e/fixtures/qtime_app.py"
    app = AppTest.from_file(str(fixture)).run()
    app.button(key="qtime_search").click().run()
    assert app.get("plotly_chart")

    class Tomorrow(date):
        @classmethod
        def today(cls):
            return date.today() + timedelta(days=1)

    monkeypatch.setattr(dashboard, "date", Tomorrow, raising=False)
    app.run()
    assert not app.get("plotly_chart")
    assert any("点击“查询”" in item.value for item in app.info)


def test_qtime_dashboard_gates_results_until_the_user_queries() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    assert app.subheader[0].value == "北极星QTime监控"
    assert [widget.key for widget in app.multiselect] == [
        "qtime_products",
        "qtime_step_descriptions",
    ]
    assert not app.date_input
    assert app.selectbox(key="qtime_shop").label == "厂别"
    assert app.multiselect(key="qtime_step_descriptions").label == "站点"
    assert app.info[0].value == "请选择筛选条件并点击“查询”。"

    app.button(key="qtime_search").click().run()

    assert not app.exception
    assert not app.info
    assert len(app.dataframe) == 1
    assert len(app.get("plotly_chart")) == 2
    figures = [json.loads(chart.proto.spec) for chart in app.get("plotly_chart")]
    assert "M626" in figures[0]["layout"]["title"]["text"]
    assert "M678" in figures[1]["layout"]["title"]["text"]
    assert all(len(figure["data"][0]["x"]) == 12 for figure in figures)


@pytest.mark.parametrize("success", [True, False])
def test_refresh_reloads_full_window_and_clears_results_only_on_success(monkeypatch, success):
    from app.sections.indicator_domain.qtime import dashboard
    state = {dashboard.RESULT_STATE_KEY: "old", dashboard.SIGNATURE_STATE_KEY: "old"}
    monkeypatch.setattr(dashboard.st, "session_state", state)
    calls = []
    service = SimpleNamespace(refresh_snapshots=lambda **kwargs: (
        calls.append(kwargs) or [SimpleNamespace(refreshed_from_database=success)]
    ))
    assert dashboard.refresh_qtime_data(service) is success
    assert calls == [{"full_refresh": True}]
    assert bool(state) is not success


def test_failed_manual_refresh_marks_previously_fresh_session_result_stale(monkeypatch):
    import pandas as pd
    from app.sections.indicator_domain.qtime import dashboard
    from src.indicator_domain.application.qtime.service import QTimeMonitoringResult
    from src.shared_kernel.data_health import make_data_health

    old = QTimeMonitoringResult(
        pd.DataFrame({"lot_id": ["L1"]}), pd.DataFrame(), pd.DataFrame(),
        pd.DataFrame(), None, make_data_health("fresh", refreshed_at="2026-09-01"),
    )
    state = {dashboard.RESULT_STATE_KEY: old, dashboard.SIGNATURE_STATE_KEY: "same"}
    monkeypatch.setattr(dashboard.st, "session_state", state)
    service = SimpleNamespace(refresh_snapshots=lambda **kwargs: [SimpleNamespace(refreshed_from_database=False)])
    assert dashboard.refresh_qtime_data(service) is False
    assert state[dashboard.RESULT_STATE_KEY].data_health["status"] == "stale"
    assert state[dashboard.RESULT_STATE_KEY].data_health["refreshed_at"] == "2026-09-01"
    assert state[dashboard.RESULT_STATE_KEY].details["lot_id"].tolist() == ["L1"]


def test_qtime_dashboard_labels_station_paths_with_original_codes() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    assert app.multiselect(key="qtime_step_descriptions").options[:2] == [
        "15500 → 15600｜M3_DE->M3_STR",
        "11300 → 11400｜PSI_ELA->PSI_PHT",
    ]


@pytest.mark.parametrize("admin", [None, "false", "true"])
def test_qtime_dashboard_renders_the_alert_center_and_decoration_admin(admin) -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()
    if admin is not None:
        app.query_params["admin"] = admin
    app.button(key="qtime_search").click().run()

    expander_labels = [expander.label for expander in app.expander]
    assert "Q-Time 超规预警中心" in expander_labels
    assert ("开发者后台：Q-Time 超规数据修饰" in expander_labels) is (admin == "true")
    assert any("2 条已确认超规" in message.value for message in app.error)


def test_qtime_dashboard_supports_multiple_paths() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    app.multiselect(key="qtime_step_descriptions").set_value(
        [M3_DE_TO_M3_STR, PSI_ELA_TO_PSI_PHT]
    ).run()
    app.button(key="qtime_search").click().run()

    assert not app.exception
    assert len(app.dataframe) == 1
    assert len(app.get("plotly_chart")) == 4


def test_station_expander_wraps_four_products_into_two_rows():
    from src.indicator_domain.application.qtime.cached_monitoring import get_qtime_cached_funcs
    for cached in get_qtime_cached_funcs():
        cached.clear()
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    script = fixture_path.read_text(encoding="utf-8").replace(
        'FIXTURE_PRODUCTS = ("M626", "M678")',
        'FIXTURE_PRODUCTS = ("M626", "M678", "Z517", "Z553")',
    )
    # The inline test already runs with the repository on sys.path.
    script = script[script.index("from app.sections.indicator_domain.qtime.dashboard"):]
    app = AppTest.from_string(
        "from pathlib import Path\nimport pandas as pd\nimport streamlit as st\n" + script
    ).run()
    app.button(key="qtime_search").click().run()
    assert not app.exception
    station = next(expander for expander in app.expander if expander.label == M3_DE_TO_M3_STR.label)
    assert station.proto.expanded
    assert len(station.get("plotly_chart")) == 4
    columns = station.get("column")
    assert len(columns) == 6
    assert [len(column.get("plotly_chart")) for column in columns] == [1, 1, 1, 1, 0, 0]
    for cached in get_qtime_cached_funcs():
        cached.clear()


def test_qtime_dashboard_invalidates_stale_results_when_filters_change() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()
    app.button(key="qtime_search").click().run()
    assert len(app.get("plotly_chart")) == 2

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
def test_qtime_dashboard_product_filter_defaults_to_all_and_leads_the_filter_row() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()

    product_filter = app.multiselect(key="qtime_products")
    assert product_filter.label == "产品"
    assert list(product_filter.options) == list(SessionManager.AVAILABLE_PRODUCTS)
    assert list(product_filter.value) == list(SessionManager.AVAILABLE_PRODUCTS)
    # 筛选顺序：产品 → 厂别 → 站点 → 查询
    assert app.multiselect[0].key == "qtime_products"
    assert app.selectbox[0].key == "qtime_shop"
    assert app.multiselect[1].key == "qtime_step_descriptions"


def test_qtime_dashboard_filters_results_to_selected_products_in_memory() -> None:
    """厂别全量结果按选中产品内存过滤：details/alerts/decoration 三帧同步收缩。"""
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()
    app.multiselect(key="qtime_products").set_value(["M626"]).run()
    app.button(key="qtime_search").click().run()

    assert not app.exception
    result = app.session_state["qtime_report_result"]
    for frame_name in ("details", "alerts", "decoration"):
        frame = getattr(result, frame_name)
        assert not frame.empty, frame_name
        assert set(frame["prodcode"].unique()) == {"M626"}, frame_name
    assert any("1 条已确认超规" in message.value for message in app.error)
    assert len(app.get("plotly_chart")) == 1


def test_qtime_dashboard_disables_query_when_no_product_selected() -> None:
    fixture_path = Path(__file__).parents[6] / "tests" / "e2e" / "fixtures" / "qtime_app.py"
    app = AppTest.from_file(str(fixture_path)).run()
    app.multiselect(key="qtime_products").set_value([]).run()

    assert app.button(key="qtime_search").disabled is True
