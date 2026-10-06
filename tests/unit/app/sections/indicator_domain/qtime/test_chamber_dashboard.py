from pathlib import Path
import json

import pandas as pd
from streamlit.testing.v1 import AppTest

from src.indicator_domain.application.qtime.chamber_cache import clear_chamber_cache

FIXTURE = Path(__file__).resolve().parents[6] / 'tests/e2e/fixtures/chamber_qtime_app.py'


def chart_products(app):
    return {trace['name'] for chart in app.get('plotly_chart')
            for trace in json.loads(chart.proto.spec)['data']}


def test_chamber_filters_empty_means_all_and_refresh_resets_results():
    clear_chamber_cache()
    app = AppTest.from_file(str(FIXTURE)).run()
    assert not app.exception
    assert len(app.dataframe) == 0
    app.button(key='chamber_search').click().run()
    assert not app.exception
    assert not app.metric and not app.dataframe and not app.warning
    assert chart_products(app) == {'M626', 'M678'}
    captions = ' '.join(item.value for item in app.caption)
    assert '空选表示全部' not in captions and '数据覆盖' not in captions
    points = [value for chart in app.get('plotly_chart')
              for trace in json.loads(chart.proto.spec)['data'] for value in trace['y']]
    assert sorted(points) == [120, 1000]
    assert [item.label for item in app.expander] == ['3CEE001 - PT → OC1', '3CEE002 - PT → OC1']
    assert not app.get('download_button')
    app.multiselect(key='chamber_products').set_value(['M678']).run()
    assert chart_products(app) == {'M678'}
    app.multiselect(key='chamber_lines').set_value(['3CEE001']).run()
    assert len(app.dataframe) == 0
    assert any('暂无单腔' in item.value for item in app.info)
    app.multiselect(key='chamber_lines').set_value([]).run()
    app.multiselect(key='chamber_names').set_value([]).run()
    assert not app.dataframe
    assert len(app.get('plotly_chart')) == 11
    assert len(app.expander) == 11
    app.button(key='fixture_refresh').click().run()
    assert not app.get('plotly_chart')
    assert not app.exception


def test_chamber_failure_and_empty_state_do_not_leave_prior_success_visible():
    clear_chamber_cache()
    app = AppTest.from_file(str(FIXTURE)).run()
    app.button(key='chamber_search').click().run()
    assert len(app.get('plotly_chart')) == 2
    app.query_params['scenario'] = 'failure'
    app.run()
    app.button(key='chamber_search').click().run()
    assert len(app.dataframe) == 0
    assert not app.get('plotly_chart')
    assert not app.exception
    assert app.error[0].value == '蒸镀单腔停留时间数据读取失败，请稍后重试。'
    app.query_params['scenario'] = 'empty'
    app.run()
    app.button(key='chamber_search').click().run()
    assert len(app.dataframe) == 0
    assert any('暂无单腔' in item.value for item in app.info)
    assert not app.exception


def test_changed_enabled_products_clear_results_and_obsolete_selection():
    clear_chamber_cache()
    app = AppTest.from_file(str(FIXTURE)).run()
    app.multiselect(key='chamber_products').set_value(['M626']).run()
    app.button(key='chamber_search').click().run()
    assert chart_products(app) == {'M626'}
    app.query_params['restricted'] = 'true'
    app.run()
    assert app.multiselect(key='chamber_products').value == []
    assert app.multiselect(key='chamber_products').options == ['M678（CPD2455）']
    assert len(app.dataframe) == 0
    app.button(key='chamber_search').click().run()
    assert chart_products(app) == {'M678'}


def test_chamber_frontend_excludes_october_even_from_existing_session_results():
    clear_chamber_cache()
    app = AppTest.from_file(str(FIXTURE)).run()
    app.button(key='chamber_search').click().run()
    report = app.session_state['chamber_qtime_view_model']
    sample = report['details'].loc[
        report['details']['chamber'].eq('PT->OC1')
        & report['details']['prod_code'].eq('M626')
    ].iloc[[0]].copy()
    details = pd.concat([
        sample.assign(entry_time=pd.Timestamp(timestamp), glass_id=glass)
        for timestamp, glass in [
            ('2026-09-30 23:59:59.999999', 'SEPTEMBER'),
            ('2026-10-01 00:00:00', 'OCTOBER_BOUNDARY'),
            ('2026-10-05 12:00:00', 'OCTOBER_LATER'),
        ]
    ], ignore_index=True)
    app.session_state['chamber_qtime_view_model'] = {**report, 'details': details}
    app.run()

    assert not app.exception
    charts = [json.loads(chart.proto.spec) for chart in app.get('plotly_chart')]
    assert len(charts) == 1
    assert [row[1] for chart in charts for trace in chart['data']
            for row in trace['customdata']] == ['SEPTEMBER']
    assert charts[0]['layout']['xaxis']['ticktext'] == ['09-30 23时']
    pd.testing.assert_frame_equal(
        app.session_state['chamber_qtime_view_model']['details'], details,
    )

    app.session_state['chamber_qtime_view_model'] = {**report, 'details': details.iloc[1:].copy()}
    app.run()
    assert not app.exception and not app.get('plotly_chart')
    assert any('暂无单腔' in item.value for item in app.info)
