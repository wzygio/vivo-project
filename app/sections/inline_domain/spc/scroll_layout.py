"""Keep SPC's long report in the browser document's native scroll flow."""

import streamlit as st


SPC_PAGE_SCROLL_CSS = """
<style>
/* The filter key scopes these rules to the SPC page, including after navigation. */
html:has(.st-key-spc_factory_filter) {
    height: auto !important;
    overflow-y: auto !important;
}
body:has(.st-key-spc_factory_filter) {
    height: auto !important;
    min-height: 100vh;
    overflow: visible !important;
}
body:has(.st-key-spc_factory_filter) [data-testid="stApp"],
body:has(.st-key-spc_factory_filter) [data-testid="stAppViewContainer"] {
    position: relative !important;
    height: auto !important;
    min-height: 100vh;
    overflow: visible !important;
}
body:has(.st-key-spc_factory_filter) [data-testid="stMain"] {
    height: auto !important;
    overflow: visible !important;
}
body:has(.st-key-spc_factory_filter) [data-testid="stSidebar"] {
    position: sticky;
    top: 0;
    height: 100vh;
    max-height: 100vh;
    align-self: flex-start;
}
</style>
"""


def render_spc_page_scroll_layout() -> None:
    """Use one native page scrollbar without changing chart event handlers."""
    st.html(SPC_PAGE_SCROLL_CSS)
