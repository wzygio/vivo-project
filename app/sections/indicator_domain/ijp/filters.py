"""Shared region selector and single-row IJP filter layout."""

import streamlit as st
from streamlit.delta_generator import DeltaGenerator

REGIONS = ("AA区", "孔区")


def get_ijp_region() -> str:
    region = st.session_state.get("ijp_region", REGIONS[0])
    region = {"边框": "AA区", "孔区（大孔）": "孔区"}.get(region, region)
    if region not in REGIONS:
        region = REGIONS[0]
    if st.session_state.get("ijp_region") != region:
        st.session_state["ijp_region"] = region
    return region


def render_ijp_filter_columns() -> list[DeltaGenerator]:
    """Render region first and return the four adjacent business filter slots."""
    get_ijp_region()
    region_column, *filter_columns = st.columns(5)
    with region_column:
        st.selectbox("监控区域", REGIONS, key="ijp_region")
    return filter_columns
