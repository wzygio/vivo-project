"""Parts and evaporation rerun counters against production UI, without external I/O."""
from datetime import date
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


@st.cache_resource
def install_fixture(mode):
    patch = pytest.MonkeyPatch()
    from app.components import page_header
    from app.manager.session_manager import SessionManager
    from src.shared_kernel.infrastructure import db_handler
    patch.setattr(SessionManager, 'get_active_config', lambda: {})
    patch.setattr(db_handler, 'DatabaseManager', lambda: object())
    patch.setattr(page_header, 'render_page_header', lambda *a, **kw: None)
    if mode == 'parts':
        from src.equipment_domain.application import parts_service
        from app.sections.equipment_domain import parts_alerts, parts_dashboard

        def load(**kwargs):
            st.session_state['parts_loads'] = st.session_state.get('parts_loads', 0) + 1
            return SimpleNamespace(report_df=pd.DataFrame({
                '厂别': ['ARRAY', 'OLED'], '设备类型': ['PVD', 'CVD'],
                '备件类型': ['Target', 'Mask'], '预警状态': ['正常', '正常'],
            }), last_update='2026-11-16')

        def publish(*args, **kwargs):
            st.session_state['parts_publications'] = st.session_state.get('parts_publications', 0) + 1

        def table(frame):
            st.caption(f"Parts loads: {st.session_state['parts_loads']}")
            st.caption(f"Parts publications: {st.session_state['parts_publications']}")
            st.dataframe(frame, hide_index=True)

        patch.setattr(parts_service, 'build_parts_report_cache_context', lambda *a: dict(
            as_of_date='2026-11-16', baseline_signature='fixture',
            runtime_config_signature='fixture', cvd_signature='fixture'))
        patch.setattr(parts_service.PartsReportService, 'get_report_data', staticmethod(load))
        patch.setattr(parts_alerts, 'render_parts_alerts', publish)
        patch.setattr(parts_dashboard, 'render_parts_metrics', lambda **kw: None)
        patch.setattr(parts_dashboard, 'render_parts_table', table)
    else:
        from app.sections.iqc_domain.eva_materials import evaporation_dashboard as ui
        patch.setattr(ui.ConfigLoader, 'get_enabled_products', lambda **kw: ['Z553', 'Z517'])
        patch.setattr(ui.ConfigLoader, 'get_work_order_types', lambda: [])

        def prepare(*args, **kwargs):
            st.session_state['evap_publications'] = st.session_state.get('evap_publications', 0) + 1
            return 'Saved alerts', date(2026, 11, 9), date(2026, 11, 15), False

        def saved(*args, **kwargs):
            st.caption(f"Evap publications: {st.session_state['evap_publications']}")

        def load(*args):
            st.session_state['evap_queries'] = st.session_state.get('evap_queries', 0) + 1
            return pd.DataFrame({'产品型号': ['Z553']})

        def results(frame):
            st.caption(f"Evap queries: {st.session_state['evap_queries']}")

        patch.setattr(ui, '_prepare_automatic_alerts', prepare)
        patch.setattr(ui, '_render_saved_automatic_alerts', saved)
        patch.setattr(ui, '_load_report', load)
        patch.setattr(ui, '_render_results', results)
    return patch


mode = st.query_params.get('report', 'parts')
_patch = install_fixture(mode)
st.button('Fixture full page reload', key='fixture_full_reload')
if mode == 'parts':
    runpy.run_path(str(ROOT / 'app/pages/关键备件报表.py'), run_name='__main__')
else:
    from app.sections.iqc_domain.eva_materials.evaporation_dashboard import render_evaporation_dashboard
    render_evaporation_dashboard(object(), alert_store=object())
