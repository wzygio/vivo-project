from datetime import date
import json
from pathlib import Path

from streamlit.testing.v1 import AppTest

from src.indicator_domain.application.qtime.chamber_cache import clear_chamber_cache

FIXTURE = Path(__file__).parent / 'fixtures/chamber_database_app.py'


def points(app):
    return [point for chart in app.get('plotly_chart')
            for trace in json.loads(chart.proto.spec)['data'] for point in trace['y']]


def test_database_page_date_changes_query_results_and_clears_stale_charts():
    clear_chamber_cache()
    app = AppTest.from_file(str(FIXTURE)).run()
    assert not app.exception and not points(app)
    app.button(key='chamber_search').click().run()
    assert not app.exception and sorted(points(app)) == [120, 1000]
    app.date_input(key='chamber_end_date').set_value(date(2026, 9, 2)).run()
    assert not points(app)
    app.date_input(key='chamber_start_date').set_value(date(2026, 9, 2)).run()
    app.button(key='chamber_search').click().run()
    assert not app.exception and points(app) == [40]


def test_database_page_safe_failure_and_empty_states():
    for scenario, expected in [('failure','蒸镀单腔停留时间数据读取失败，请稍后重试。'),
                               ('empty','当前筛选条件下暂无单腔停留时间数据。')]:
        clear_chamber_cache()
        app = AppTest.from_file(str(FIXTURE))
        app.query_params['scenario'] = scenario
        app.run()
        app.button(key='chamber_search').click().run()
        assert not app.exception and not points(app)
        messages = [item.value for item in app.error] + [item.value for item in app.info]
        assert expected in messages
