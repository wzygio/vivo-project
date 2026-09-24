"""All enabled products' Group trends, isolated from the warning matrix UI."""

import logging

import streamlit as st

from app.sections.yield_domain.group_trend_data import load_product_group_trends
from app.sections.yield_domain.yield_dashboard import render_macro_trend_section
from src.shared_kernel.config import ConfigLoader


logger = logging.getLogger(__name__)
PRODUCTS_KEY = "all_product_yield_products"
LOADED_KEY = "all_product_yield_loaded"


@st.fragment
def render_product_yield_trends(product_code: str) -> None:
    """Group interaction reruns only this product and reuses application data."""
    if product_code not in ConfigLoader.get_enabled_products():
        return
    with st.container(border=True):
        st.markdown(f"**{product_code}**")
        try:
            with st.spinner(f"正在加载 {product_code} 良率趋势..."):
                payload = load_product_group_trends(product_code)
        except Exception:
            # Product isolation is a UI boundary: retain diagnostics server-side.
            logger.exception("Unable to load Group trends for %s", product_code)
            st.error("该产品良率数据加载失败，请稍后重新查询。")
            return
        status = payload["data_health"].get("status", "unknown")
        if status == "unavailable":
            st.error("该产品入库数据暂不可用，请稍后重新查询。")
            return
        if status != "fresh":
            st.warning("数据更新或完整性尚未确认，以下趋势仅供参考。")
        trends = payload["trends"]
        if not trends or all(frame is None or frame.empty for frame in trends.values()):
            st.info("该产品暂无入库良率趋势数据。")
            return
        render_macro_trend_section(trends, key_prefix=f"all_product_yield_{product_code}")


@st.fragment
def render_all_product_yield_board() -> None:
    """Query gate and enabled scope belong to this board, not the page header."""
    st.subheader("全产品良率看板")
    products = ConfigLoader.get_enabled_products()
    if not products:
        st.info("暂无可展示的产品。")
        return
    if PRODUCTS_KEY in st.session_state:
        st.session_state[PRODUCTS_KEY] = [
            product for product in st.session_state[PRODUCTS_KEY] if product in products
        ]
    selection_col, query_col = st.columns([5, 1], vertical_alignment="bottom")
    with selection_col:
        selected = st.multiselect(
            "产品型号", products, key=PRODUCTS_KEY,
            placeholder="全部产品", help="留空时展示全部产品。",
        )
    with query_col:
        if st.button("查询", key="all_product_yield_query", type="primary"):
            st.session_state[LOADED_KEY] = True
    if not st.session_state.get(LOADED_KEY):
        return
    st.caption("Group 入库不良率趋势 · 最近 3 个月 / 3 周 / 7 天")
    for product in products:
        if not selected or product in selected:
            render_product_yield_trends(product)
