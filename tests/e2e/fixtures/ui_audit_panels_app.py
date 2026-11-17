"""No-production-I/O fixture for warning and document fragment boundaries."""
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
import streamlit as st

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'src'))


@st.cache_resource
def install(mode):
    patch=pytest.MonkeyPatch()
    if mode=='documents':
        from tests.unit.app.pages.test_project_documents_page import document_dirs
        scratch=ROOT/'output/tmp/ui-audit-documents'
        scratch.mkdir(parents=True,exist_ok=True)
        document_dirs.__wrapped__(scratch,patch)
    else:
        from tests.unit.app.pages.test_warning_page_split import isolated_pages
        from app.sections.inline_domain.monitor import cpk_monitor_dashboard as cpk, cpm_monitor_dashboard as cpm
        from src.inline_domain import composition
        isolated_pages.__wrapped__(patch)

        class Service:
            def __init__(self, metric):
                self.metric=metric

            def build_dashboard(self,**kwargs):
                key=f'fixture_{self.metric}_queries'
                st.session_state[key]=st.session_state.get(key,0)+1
                return object()

        patch.setattr(composition,'build_cpk_monitor_service',lambda:Service('cpk'))
        patch.setattr(composition,'build_cpm_monitor_service',lambda:Service('cpm'))
        patch.setattr(cpk,'render_cpk_monitor_results',lambda _:st.success(
            f"CPK queries: {st.session_state['fixture_cpk_queries']}"))
        patch.setattr(cpm,'render_cpm_monitor_results',lambda _:st.success(
            f"CPM queries: {st.session_state['fixture_cpm_queries']}"))
    return patch


mode=st.query_params.get('mode','warning')
_patch=install(mode)
st.session_state['fixture_page_runs']=st.session_state.get('fixture_page_runs',0)+1
st.caption(f"Page runs: {st.session_state['fixture_page_runs']}")
st.button('Fixture full page reload')
name='专项资料-台账周报.py' if mode=='documents' else '历史预警看板.py'
runpy.run_path(str(ROOT/'app/pages'/name),run_name='__main__')
