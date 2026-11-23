"""Preloaded report controls belong to their local UI, not the source loader."""
import pandas as pd
from streamlit.testing.v1 import AppTest


def fragment_of(app, widget):
    return app.session_state._state._new_widget_state.widget_metadata[widget.id].fragment_id


def test_lifetime_filters_and_query_share_a_fragment(monkeypatch):
    from app.sections.iqc_domain.lifetime import lifetime_dashboard as dashboard
    monkeypatch.setattr(dashboard.ConfigLoader, 'get_enabled_products', lambda **kw: ['Z553'])
    monkeypatch.setattr(dashboard, '_render_trends', lambda frame, specs: None)
    monkeypatch.setattr(dashboard, '_render_details', lambda frame: None)
    monkeypatch.setattr(dashboard, 'load_lifetime_specs', dict)
    monkeypatch.setattr(dashboard, 'read_alert_result', lambda *args: {'state': 'ok'})
    app = AppTest.from_string('''
import pandas as pd
from app.sections.iqc_domain.lifetime.lifetime_dashboard import render_lifetime_dashboard
class Service:
    def get_report(self):
        return pd.DataFrame({'产品型号':['Z553'], '产品状态':['量产'], '批次号':['B']})
render_lifetime_dashboard(Service())
''').run()
    assert not app.exception
    widgets = [app.multiselect(key=key) for key in (
        'iqc_lifetime_products', 'iqc_lifetime_statuses', 'iqc_lifetime_batches')]
    widgets.append(app.button(key='iqc_lifetime_query'))
    ids = {fragment_of(app, widget) for widget in widgets}
    assert None not in ids and len(ids) == 1
    app.button(key='iqc_lifetime_query').click().run()
    app.multiselect(key='iqc_lifetime_batches').select('B').run()
    assert not app.exception
    assert app.session_state['iqc_lifetime_submitted_filters'][1] == []
    assert any('上次查询结果' in caption.value for caption in app.caption)


def test_point_test_product_selector_uses_loaded_report_fragment(monkeypatch):
    from app.sections.indicator_domain.point_test import dashboard
    monkeypatch.setattr(dashboard.ConfigLoader, 'get_enabled_products', lambda: ['Z553'])
    app = AppTest.from_string('''
import pandas as pd
from app.sections.indicator_domain.point_test.dashboard import render_point_test_dashboard
class Service:
    def read_report(self):
        return {'Z553': {'detail': pd.DataFrame(), 'summary':pd.DataFrame()}}
render_point_test_dashboard(Service())
''').run()
    assert not app.exception
    assert fragment_of(app, app.selectbox(key='point_test_product')) is not None


def test_parts_filters_do_not_own_source_loading_or_alert_publication(monkeypatch):
    from app.sections.equipment_domain import parts_dashboard as dashboard
    monkeypatch.setattr(dashboard, 'render_parts_metrics', lambda **kw: None)
    monkeypatch.setattr(dashboard, 'render_parts_table', lambda frame: None)
    app = AppTest.from_string('''
import pandas as pd
from app.sections.equipment_domain.parts_dashboard import render_parts_report_interaction
render_parts_report_interaction(pd.DataFrame({
    '厂别':['ARRAY', 'OLED'],'设备类型':['PVD', 'CVD'],
    '备件类型':['Target', 'Mask'],'预警状态':['正常', '正常'],
}), last_update='2026-11-16')
''').run()
    assert not app.exception
    ids = {fragment_of(app, app.multiselect(key=key)) for key in (
        'parts_filter_factories', 'parts_filter_equipment_types', 'parts_filter_part_types')}
    assert None not in ids and len(ids) == 1
    app.multiselect(key='parts_filter_equipment_types').select('PVD').run()
    app.multiselect(key='parts_filter_part_types').select('Target').run()
    app.multiselect(key='parts_filter_factories').select('OLED').run()
    assert not app.exception
    assert app.multiselect(key='parts_filter_equipment_types').value == []
    assert app.multiselect(key='parts_filter_part_types').value == []


def test_evaporation_manual_controls_do_not_repeat_automatic_producer(monkeypatch):
    from datetime import date
    from app.sections.iqc_domain.eva_materials import evaporation_dashboard as dashboard
    calls = []
    monkeypatch.setattr(dashboard.ConfigLoader, 'get_enabled_products', lambda **kw: ['Z553'])
    monkeypatch.setattr(dashboard.ConfigLoader, 'get_work_order_types', lambda: [])
    monkeypatch.setattr(dashboard, '_prepare_automatic_alerts', lambda *a, **kw: (
        calls.append(kw) or ('Saved alerts', date(2026, 11, 9), date(2026, 11, 15), False)))
    monkeypatch.setattr(dashboard, '_render_saved_automatic_alerts', lambda *a, **kw: None)
    app = AppTest.from_string('''
from app.sections.iqc_domain.eva_materials.evaporation_dashboard import render_evaporation_dashboard
render_evaporation_dashboard(object(), alert_store=object())
''').run()
    assert not app.exception and len(calls) == 1
    ids = {fragment_of(app, widget) for widget in (
        app.multiselect(key='iqc_evap_products'), app.date_input(key='iqc_evap_dates'),
        app.button(key='iqc_evap_query'))}
    assert None not in ids and len(ids) == 1
