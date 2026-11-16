"""Actual CTQ/AOI pages with isolated producers and fragment rerun counters."""
import runpy
import sys
from pathlib import Path

import pytest
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


@st.cache_resource
def install(scope):
    from tests.unit.app.pages.test_inline_report_fragments import install_inline_page_fakes
    patches = pytest.MonkeyPatch()
    return install_inline_page_fakes(scope, patches), patches


scope = st.query_params.get("scope", "ctq")
if scope not in {"ctq", "aoi_rs", "aoi_tt"}:
    st.stop()
state, _patches = install(scope)
st.button("Fixture full page reload")
page = next((ROOT / "app/pages").glob(scope.upper() + "*.py"))
runpy.run_path(str(page), run_name="__main__")
