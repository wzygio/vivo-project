from pathlib import Path

from streamlit.testing.v1 import AppTest

from src.indicator_domain.application.qtime.chamber_cache import clear_chamber_cache

FIXTURE = Path(__file__).resolve().parents[6] / 'tests/e2e/fixtures/chamber_qtime_app.py'


def test_chamber_filters_empty_means_all_and_refresh_resets_results():
    clear_chamber_cache()
    app = AppTest.from_file(str(FIXTURE)).run()
    assert not app.exception
    assert len(app.dataframe) == 0
    app.button(key='chamber_search').click().run()
    assert not app.exception
    assert app.metric[0].value == '3'
    assert app.metric[1].value == '2'
    assert app.metric[2].value == '1'
    assert app.metric[3].value == '50.00%'
    assert set(app.dataframe[1].value['产品型号']) == {'M626', 'M678'}
    assert 'DISABLED_GLASS' not in app.dataframe[1].value['GlassID'].tolist()
    app.multiselect(key='chamber_products').set_value(['M678']).run()
    assert app.metric[0].value == '2'
    assert app.metric[3].value == '0.00%'
    app.multiselect(key='chamber_lines').set_value(['3CEE001']).run()
    assert len(app.dataframe) == 0
    assert '暂无单腔' in app.info[0].value
    app.multiselect(key='chamber_lines').set_value([]).run()
    app.multiselect(key='chamber_names').set_value([]).run()
    assert len(app.dataframe[1].value) == 22
    app.button[0].click().run()
    assert len(app.dataframe) == 0
    assert not app.exception


def test_chamber_failure_and_empty_state_do_not_leave_prior_success_visible():
    clear_chamber_cache()
    app = AppTest.from_file(str(FIXTURE)).run()
    app.button(key='chamber_search').click().run()
    assert len(app.dataframe) == 2
    app.query_params['scenario'] = 'failure'
    app.run()
    app.button(key='chamber_search').click().run()
    assert len(app.dataframe) == 0
    assert not app.exception
    assert app.error[0].value == '蒸镀单腔停留时间数据读取失败，请稍后重试。'
    app.query_params['scenario'] = 'empty'
    app.run()
    app.button(key='chamber_search').click().run()
    assert len(app.dataframe) == 0
    assert '暂无单腔' in app.info[0].value
    assert not app.exception


def test_changed_enabled_products_clear_results_and_obsolete_selection():
    clear_chamber_cache()
    app = AppTest.from_file(str(FIXTURE)).run()
    app.multiselect(key='chamber_products').set_value(['M626']).run()
    app.button(key='chamber_search').click().run()
    assert set(app.dataframe[1].value['产品型号']) == {'M626'}
    app.query_params['restricted'] = 'true'
    app.run()
    assert app.multiselect(key='chamber_products').value == []
    assert app.multiselect(key='chamber_products').options == ['M678']
    assert len(app.dataframe) == 0
    app.button(key='chamber_search').click().run()
    assert set(app.dataframe[1].value['产品型号']) == {'M678'}
