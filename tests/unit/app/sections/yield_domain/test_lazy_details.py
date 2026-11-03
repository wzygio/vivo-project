from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.sections.yield_domain import lazy_details, yield_dashboard
from app.sections.yield_domain.chart_data import prepare_lot_chart_index, build_chart_data_signature
from src.yield_domain.application.report_cache import YieldSourceGenerationChanged


def test_real_expanders_do_no_work_until_open_and_memo_tracks_data(monkeypatch):
    seen = []

    def build(*args, **kwargs):
        seen.append(args[5])
        return {'code': args[5]}

    monkeypatch.setattr(yield_dashboard, '_build_compact_render_payload', build)
    monkeypatch.setattr(yield_dashboard, 'render_compact_payload_contents',
                        lambda payload: None)
    app = AppTest.from_string('''
import streamlit as st
from app.sections.yield_domain.lazy_details import render_lazy_alert_code_expanders
from app.sections.yield_domain.yield_dashboard import _state_key_fragment
def toggle_outer():
    st.session_state['yield_automatic_details_P'] = True
def open_card():
    toggle_outer()
    st.session_state['yield_detail_open_P_' + _state_key_fragment('alert:P', 'G', 'C1')] = True
def new_data():
    st.session_state['version'] = 'v2'
    open_card()
def close():
    st.session_state['yield_automatic_details_P'] = False
st.button('Toggle automatic', on_click=toggle_outer)
st.button('Open C1', on_click=open_card)
st.button('Refresh same', on_click=open_card)
st.button('New data', on_click=new_data)
st.button('Close automatic', on_click=close)
def load():
    st.session_state['loads'] = st.session_state.get('loads', 0) + 1
    return {'sheet_data': {}, 'mapping_data': None, 'errors': []}
render_lazy_alert_code_expanders(
    pairs=[('G', 'C1'), ('G', 'C2')], warning_lines={}, mwd_code_data={}, lot_data={},
    detail_loader=load, hotspot_scripts=[], product_code='P', mapping_layout=None,
    data_signature=st.session_state.get('version', 'v1'), chart_key_prefix='alert',
)
''').run()
    assert not app.exception
    assert not seen
    app.button[0].click().run()
    assert not app.exception
    assert not seen
    key = 'yield_detail_open_P_' + yield_dashboard._state_key_fragment('alert:P', 'G', 'C1')
    app.button[1].click().run()
    assert not app.exception
    assert seen == ['C1'] and app.session_state['loads'] == 1
    app.button[2].click().run()
    assert seen == ['C1']
    assert app.session_state['loads'] == 2
    app.button[3].click().run()
    assert seen == ['C1', 'C1']
    before = app.session_state['loads']
    app.button[4].click().run()
    assert app.session_state['loads'] == before


def test_detail_failure_is_local_and_safe(monkeypatch):
    monkeypatch.setattr(lazy_details.YieldAnalysisService, 'get_sheet_defect_rates',
                        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError('SECRET SQL')))
    mapping = pd.DataFrame({'ok': [1]})
    monkeypatch.setattr(lazy_details.YieldAnalysisService, 'get_mapping_data', lambda *_args, **_kwargs: mapping)
    data = lazy_details.load_report_details(None, None, source_signature='x', cache_context={
        'analysis_start_date': '2026-08-01', 'analysis_end_date': '2026-11-03',
        'modifier_signature': 'm', 'warning_signature': 'w', 'rate_override_signature': 'r',
    })
    assert data['sheet_data'] is None
    assert data['mapping_data'] is mapping
    assert data['errors'] == ['Sheet 分布暂时无法加载，请稍后重试。']
    assert 'SECRET' not in str(data['errors'])


def test_source_change_is_not_swallowed_or_mixed_with_captured_details(monkeypatch):
    invalidate = Mock()
    monkeypatch.setattr(lazy_details.YieldAnalysisService, 'invalidate_data_health', invalidate)
    monkeypatch.setattr(lazy_details.YieldAnalysisService, 'get_data_health',
                        lambda *_args, **_kwargs: (_ for _ in ()).throw(YieldSourceGenerationChanged()))
    sheet = Mock()
    monkeypatch.setattr(lazy_details.YieldAnalysisService, 'get_sheet_defect_rates', sheet)
    with pytest.raises(YieldSourceGenerationChanged):
        lazy_details.load_report_details(None, None, source_signature='old', expected_health={'status': 'fresh'},
            cache_context={'analysis_start_date': '2026-08-01', 'analysis_end_date': '2026-11-03',
                           'modifier_signature': 'm', 'warning_signature': 'w', 'rate_override_signature': 'r'})
    sheet.assert_not_called()
    invalidate.assert_called_once()


def test_lot_index_is_exactly_the_old_per_code_projection():
    prepare_lot_chart_index.clear()
    rows = pd.DataFrame({'lot_id': ['L2', 'L1', 'L3', 'L4'], 'defect_desc': ['C1', 'C2', 'C1', 'C2'],
        'warehousing_time': ['20261231', '20270104', '20261228', '20270103'],
        'defect_rate': [.01, .02, .03, 0.0], 'defect_panel_count': [1, 2, 3, 0]})
    lots = {'code_level_details': {'G': rows}}
    index = prepare_lot_chart_index(lots, 'source-v1')
    for code in ('C1', 'C2'):
        pd.testing.assert_frame_equal(index[code], yield_dashboard._prepare_lot_code_dataframe(lots, code), check_exact=True)


def test_chart_version_changes_with_data_under_same_manual_revision():
    first = {'weekly': pd.DataFrame({'defect_rate': [.01]})}
    second = {'weekly': pd.DataFrame({'defect_rate': [.02]})}
    assert build_chart_data_signature('manual-v1', first) != build_chart_data_signature('manual-v1', second)


def test_native_mapping_cache_tracks_version_and_layout_without_storing_figures(monkeypatch):
    from app.sections.yield_domain.chart_data import prepare_mapping_chart_matrices

    prepare_mapping_chart_matrices.clear()
    calls = []
    original = yield_dashboard.parse_panel_id_to_coords
    monkeypatch.setattr(yield_dashboard, 'parse_panel_id_to_coords',
                        lambda *args: calls.append(args) or original(*args))
    mapping = pd.DataFrame({'panel_id': ['L00000001011AA0'] * 2, 'batch_no': ['26/10/28'] * 2,
                            'defect_group': ['G'] * 2, 'defect_desc': ['C'] * 2,
                            'batch_total_input': [190] * 2})
    first = prepare_mapping_chart_matrices(mapping, 'G', 'C', [], 'P', None, 'v1')
    second = prepare_mapping_chart_matrices(mapping, 'G', 'C', [], 'P', None, 'v1')
    assert len(calls) == 2
    pd.testing.assert_frame_equal(first[1]['26/10/28'], second[1]['26/10/28'])
    prepare_mapping_chart_matrices(mapping, 'G', 'C', [], 'P', None, 'v2')
    assert len(calls) == 4
    custom = prepare_mapping_chart_matrices(mapping, 'G', 'C', [], 'P',
        {'row_labels': ['1A', '2A'], 'column_labels': ['A0']}, 'v2')
    assert len(calls) == 6 and custom[1]['26/10/28'].shape == (2, 1)


def test_sheet_sort_ignores_array_time_until_feature_is_enabled(monkeypatch):
    sheet = {'group_level_summary_for_table': pd.DataFrame({'sheet_id': ['S2', 'S1'],
             'lot_id': ['L', 'L'], 'warehousing_time': ['20261101'] * 2,
             'array_input_time': ['2026-10-01', '2026-10-02']}), 'code_level_details': {}}
    monkeypatch.setattr(yield_dashboard, 'get_array_time_query_enabled', lambda: False)
    assert yield_dashboard._prepare_sheet_chart_dataframe(sheet, 'L', 'G', 'C')['sheet_id'].tolist() == ['S1', 'S2']
    monkeypatch.setattr(yield_dashboard, 'get_array_time_query_enabled', lambda: True)
    assert yield_dashboard._prepare_sheet_chart_dataframe(sheet, 'L', 'G', 'C')['sheet_id'].tolist() == ['S2', 'S1']


def test_source_revision_is_independent_from_indicator_display_revision(monkeypatch):
    from app.components import indicator_cache
    from app.sections.yield_domain.group_trend_data import build_yield_source_signature, YIELD_SOURCE_INDICATOR

    revisions = {}
    monkeypatch.setattr(indicator_cache, 'get_indicator_product_revision',
                        lambda indicator, product, **kwargs: revisions.get((indicator, product), '0'))
    p, q = build_yield_source_signature('P'), build_yield_source_signature('Q')
    revisions[('yield_lot_oos', 'P')] = 'display-refresh'
    assert build_yield_source_signature('P') == p
    revisions[(YIELD_SOURCE_INDICATOR, 'P')] = 'report-refresh'
    assert build_yield_source_signature('P') != p
    assert build_yield_source_signature('Q') == q
