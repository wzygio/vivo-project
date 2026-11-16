"""BySheet/ByLot page fixture with no DB and visible source-load counters.

Start a fresh fixture process per grain; ?grain=Sheet (default) or ?grain=Lot.
The parent summary and each child show independent counters for browser checks.
"""
from pathlib import Path
import runpy
import sys

import pytest
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src'))


@st.cache_resource
def install_fixture(grain):
    from tests.unit.app.pages.test_yield_detail_interaction import detail_page
    from app.sections.yield_domain import table_details

    patches = pytest.MonkeyPatch()
    scratch = ROOT / 'output/tmp/yield-detail-interaction'
    scratch.mkdir(parents=True, exist_ok=True)
    payload, calls = detail_page.__wrapped__(patches, scratch)
    counters = {'summary': 0, 'details': 0, 'code': 0}
    prefix = grain.lower()
    summary = getattr(table_details, f'render_{prefix}_group_summary_section')
    details = getattr(table_details, f'render_{prefix}_code_details_section').__wrapped__
    code = getattr(table_details, f'render_{prefix}_top20_section').__wrapped__

    def summary_counted(*args):
        counters['summary'] += 1
        st.caption(f"Fixture summary: {counters['summary']}; loads: {len(calls)}")
        return summary(*args)

    @st.fragment
    def details_counted(*args):
        counters['details'] += 1
        st.caption(f"Fixture details: {counters['details']}; loads: {len(calls)}")
        details(*args)

    @st.fragment
    def code_counted(*args):
        counters['code'] += 1
        st.caption(f"Fixture code: {counters['code']}; loads: {len(calls)}")
        code(*args)

    patches.setattr(table_details, f'render_{prefix}_group_summary_section', summary_counted)
    patches.setattr(table_details, f'render_{prefix}_code_details_section', details_counted)
    patches.setattr(table_details, f'render_{prefix}_top20_section', code_counted)
    return patches, counters, calls


grain = 'Lot' if st.query_params.get('grain') == 'Lot' else 'Sheet'
patches, counters, calls = install_fixture(grain)
st.button('Fixture full page reload')
runpy.run_path(str(ROOT / 'app/pages' / f'入库不良率By{grain}明细表.py'), run_name='__main__')
