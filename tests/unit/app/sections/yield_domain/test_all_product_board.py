"""Observable multi-product Group trends through the Streamlit interface."""

import json

import pandas as pd
from streamlit.testing.v1 import AppTest


def trend_data():
    return {
        period: pd.DataFrame([
            {"time_period": label, "defect_group": group,
             "defect_rate": rate, "total_panels": 1000, "flag": "INTERNAL"}
            for label in labels
            for group, rate in (("Array_Line", .01), ("OLED_Mura", .02))
        ])
        for period, labels in {
            "monthly": ["2026-06", "2026-07", "2026-08", "2026-09"],
            "weekly": ["2026-W36", "2026-W37", "2026-W38", "2026-W39"],
            "daily": [f"2026-09-{day}" for day in range(16, 25)],
        }.items()
    }


def test_identical_products_have_independent_group_controls_and_chart_ids():
    def app(data):
        from app.sections.yield_domain.yield_dashboard import render_macro_trend_section
        render_macro_trend_section(data, key_prefix="product_A")
        render_macro_trend_section(data, key_prefix="product_B")

    at = AppTest.from_function(app, args=(trend_data(),)).run()
    assert not at.exception
    assert len(at.get("plotly_chart")) == 6
    assert len({chart.proto.id for chart in at.get("plotly_chart")}) == 6
    at.multiselect(key="product_A_group_sel").set_value(["Array_Line"]).run()
    assert not at.exception
    assert at.multiselect(key="product_B_group_sel").value == ["Array_Line", "OLED_Mura"]
    charts = [json.loads(chart.proto.spec) for chart in at.get("plotly_chart")]
    assert [len(chart["data"][0]["x"]) for chart in charts[:3]] == [3, 3, 7]
    assert {trace["name"] for trace in charts[0]["data"] if trace["type"] == "bar"} == {"Array_Line"}
    assert {trace["name"] for trace in charts[3]["data"] if trace["type"] == "bar"} == {"Array_Line", "OLED_Mura"}
    assert "INTERNAL" not in "".join(chart.proto.spec for chart in at.get("plotly_chart"))


def test_board_query_scope_and_product_failures_are_isolated(monkeypatch):
    from app.sections.yield_domain import all_product_board as board

    enabled = ["B", "A", "FAILED"]
    calls = []

    def load(product, **kwargs):
        calls.append(product)
        if product == "FAILED":
            raise OSError("secret file / config / admin=true")
        from src.yield_domain.core.mwd_trend.period_trend import aggregate_periods
        from datetime import datetime
        year, month = datetime.now().year, datetime.now().month
        monthly = pd.DataFrame([[f'{year}-{m:02}', level, 'G', code, code, .01]
            for m in range(1, month + 1) for level, code in [('Group', ''), ('Code', 'C')]],
            columns=['month', 'level', 'defect_group', 'defect_code', 'defect_desc', 'defect_rate'])
        inputs = pd.DataFrame({'month': monthly.month.unique(), 'total_panels': 1000})
        rates, volumes = aggregate_periods(monthly, inputs, year, month)
        if product == 'A':
            rates.loc[rates.level.eq('Group'), 'defect_rate'] = float('nan')
        return {'rates': rates.assign(flag='INTERNAL'), 'volumes': volumes.assign(secret='INTERNAL')}

    monkeypatch.setattr(board.ConfigLoader, "get_enabled_products", lambda: list(enabled))
    monkeypatch.setattr(board, "load_product_period_trends", load)

    def app():
        from app.sections.yield_domain.all_product_board import render_all_product_yield_board
        render_all_product_yield_board()

    at = AppTest.from_function(app).run()
    assert not at.exception
    assert calls == []
    assert at.multiselect[0].options == enabled
    at.button(key="all_product_yield_query").click().run()
    assert not at.exception
    assert calls == enabled
    assert len(at.get("plotly_chart")) == 2
    assert len(at.error) == 1
    assert len(at.tabs) == 3
    assert 'INTERNAL' not in ''.join(chart.proto.spec for chart in at.get('plotly_chart'))
    messages = " ".join(item.value for kind in (at.error, at.warning, at.info, at.caption) for item in kind)
    assert all(token not in messages for token in ("secret", "admin=true", "INTERNAL", "config", "维护", "修饰", "flag"))
    assert len(at.warning) == 1
    assert all(json.loads(chart.proto.spec)['layout']['xaxis']['type'] == 'linear' for chart in at.get('plotly_chart'))

    calls.clear()
    at.multiselect(key="all_product_yield_products").set_value(["A", "B"]).run()
    assert calls == []
    assert len(at.get("plotly_chart")) == 2
    before = json.loads(at.get('plotly_chart')[0].proto.spec)
    at.multiselect(key='all_product_yield_codes').set_value([board.ALL_CODES, 'C']).run()
    assert at.multiselect(key='all_product_yield_codes').value == ['C']
    assert calls == []
    at.multiselect(key='all_product_yield_codes').set_value([]).run()
    assert calls == []
    assert json.loads(at.get('plotly_chart')[0].proto.spec) == before
    assert all(len(frame.value) == 2 for frame in at.dataframe)

    # Registry changes prune obsolete selections before querying or rendering.
    enabled[:] = ["B"]
    calls.clear()
    at.run()
    assert not at.exception
    assert calls == []
    assert at.multiselect(key="all_product_yield_products").value == ["B"]
