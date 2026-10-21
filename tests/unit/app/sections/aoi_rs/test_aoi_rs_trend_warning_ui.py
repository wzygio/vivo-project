"""User-visible trend warnings, data availability and automatic charts."""

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest


def test_center_shows_only_daily_rule_name_and_strips_internal_columns() -> None:
    app = AppTest.from_string('''
from datetime import date
from types import SimpleNamespace
import pandas as pd
from app.sections.inline_domain.aoi_rs.trend_alerts import render_aoi_rs_trend_alerts
evaluation = pd.DataFrame([dict(
    prod_code="M678", factory="ARRAY", step_id="S", rs_code="A",
    target_date="2026-10-18", target_density=.17, previous_date="2026-10-16", previous_density=.13,
    two_days_before_date="2026-10-13", two_days_before_density=.10, baseline_density=.10, target_sheet_qty=100,
    valid_days=3, baseline_days=1, daily_evaluable=True,
    daily_alert=True, is_alert=True, flag="private",
    raw_value="private",
)])
report = SimpleNamespace(trend_evaluation_df=evaluation, indicators_df=pd.DataFrame())
render_aoi_rs_trend_alerts(report, reference_date=date(2026,10,19), step_desc_map={"S":"检测站"})
''').run()
    assert not app.exception
    assert len(app.dataframe) == 1
    table = app.dataframe[0].value
    assert table.columns.tolist() == [
        "厂别", "站点", "RS Code", "当日", "当日密度", "T-1有效日", "T-1密度",
        "T-2有效日", "T-2密度", "基线5日均值", "预警原因",
    ]
    assert "flag" not in table and "raw_value" not in table and "prod_code" not in table
    assert table["预警原因"].tolist() == ["当日超规"]
    assert table["站点"].tolist() == ["S 检测站"]
    assert table["当日"].tolist() == ["2026-10-18"]
    assert table["T-1有效日"].tolist() == ["2026-10-16"]
    assert table["T-2有效日"].tolist() == ["2026-10-13"]
    assert "基线5日均值" in table
    assert "基线7日均值" not in table
    assert "前一天密度" not in table and "前5日均值" not in table
    assert not app.caption and not app.warning
    assert [item.value for item in app.info] == ["暂无图像。"]
    assert app.expander[0].label == "趋势预警中心（当日 2026-10-18）"


@pytest.mark.parametrize("scenario,chart_count,success,info,error", [
    ("有预警", 3, False, False, False),
    ("跨日预警", 3, False, False, False),
    ("无预警", 0, True, False, False),
    ("数据不足", 0, False, True, False),
    ("加载失败", 0, False, False, True),
])
def test_fixture_states_and_exact_automatic_chart_scope(scenario, chart_count, success, info, error):
    fixture = Path(__file__).resolve().parents[4] / "e2e/fixtures/aoi_rs_trend_app.py"
    app = AppTest.from_file(str(fixture)).run()
    app.selectbox[0].select(scenario).run()
    assert not app.exception
    assert len(app.get("plotly_chart")) == chart_count
    assert bool(app.success) == success
    assert bool(app.info) == info
    assert bool(app.error) == error
    if chart_count:
        assert app.dataframe[0].value["RS Code"].tolist() == ["A1PPS"]
        if scenario == "跨日预警":
            assert app.dataframe[0].value["T-1有效日"].tolist() == ["2026-10-15"]
            assert app.dataframe[0].value["T-2有效日"].tolist() == ["2026-10-12"]
        assert not app.caption and not app.warning and not app.info
        keys = [chart.proto.id for chart in app.get("plotly_chart")]
        assert len(set(keys)) == 3
        assert all("aoi_rs_trend_alert" in key for key in keys)
        assert not any("稳定样例" in expander.label for expander in app.expander)
