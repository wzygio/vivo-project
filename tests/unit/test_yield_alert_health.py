"""Displayed historical facts must not certify the current monitoring state."""

import ast
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest


@pytest.mark.parametrize("statuses", [["fresh", "stale"], ["unknown", "fresh"]])
def test_uncertain_source_displays_unknown_without_normal_claims(statuses):
    app = AppTest.from_string(
        "from app.components.alert_center import render_alert_center\n"
        f"statuses = {statuses!r}\n"
        "render_alert_center([], {}, [], 10, "
        "source_current=all(status == 'fresh' for status in statuses))\n"
    ).run()
    assert not app.exception
    assert not app.success
    assert any("当前状态未知" in item.value for item in app.warning)
    captions = " ".join(item.value for item in app.caption)
    assert "全部在规格线内" not in captions
    assert "无异常升高" not in captions


def test_historical_alerts_are_preserved_with_unknown_current_state():
    app = AppTest.from_string(
        "from app.components.alert_center import render_alert_center\n"
        "render_alert_center(['历史趋势升高'], {}, [], 10, source_current=False)\n"
    ).run()
    assert not app.exception
    assert not app.success
    assert any("当前状态未知" in item.value for item in app.warning)
    assert any("历史" in item.value for item in app.error)
    assert any("历史趋势升高" in item.value for item in app.markdown)


def test_existing_fresh_callers_keep_normal_result():
    app = AppTest.from_string(
        "from app.components.alert_center import render_alert_center\n"
        "render_alert_center([], {}, [], 10)\n"
    ).run()
    assert not app.exception
    assert app.success[0].value == "系统监测正常"


def test_yield_dashboard_passes_all_displayed_source_health_to_alert_center():
    page = Path(__file__).parents[2] / "app/pages/入库不良率分析看板.py"
    tree = ast.parse(page.read_text(encoding="utf-8"))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id == "render_alert_center"]
    assert len(calls) == 1
    arguments = {keyword.arg: keyword.value for keyword in calls[0].keywords}
    assert "source_current" in arguments
    expression = ast.Expression(arguments["source_current"])
    for statuses, expected in [(["fresh", "fresh"], True), (["fresh", "stale"], False),
                               (["unknown", "fresh"], False), (["partial"], False)]:
        assert eval(compile(expression, str(page), "eval"), {
            "displayed_health": [{"status": status} for status in statuses],
        }) is expected
