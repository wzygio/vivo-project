"""Query, filter and paginate the WMS evaporation report."""

import logging
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from app.sections.iqc_domain.report_table import build_report_table
from app.manager.session_manager import SessionManager
from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
from src.iqc_domain.composition import build_evaporation_service
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)
PAGE_SIZE = 100


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=32, show_spinner=False)
def fetch_report_payload(
    start: date, end: date, forward_signature: str, cutoff_signature: str,
    product_code: str | None = None,
) -> pd.DataFrame:
    """Only native payloads cross the cache; signatures partition time policies."""
    return build_evaporation_service().get_report(start, end, product_code=product_code)


def _render_results(frame: pd.DataFrame, signature: tuple) -> None:
    st.caption(f"共 {len(frame)} 条记录 · 左右滚动查看全部测点和检验结果")
    if frame.empty:
        st.info("没有符合筛选条件的记录，请调整筛选条件。")
        return
    if frame[["IQC结果", "COA结果"]].isna().any(axis=None):
        st.caption("结果空白表示尚未提供检验结论。")
    view_signature = (signature, len(frame))
    page_count = (len(frame) + PAGE_SIZE - 1) // PAGE_SIZE
    if st.session_state.get("iqc_evap_view") != view_signature:
        st.session_state["iqc_evap_page"] = 1
        st.session_state["iqc_evap_view"] = view_signature
    page = st.number_input(
        "页码", min_value=1, max_value=page_count, step=1, key="iqc_evap_page",
        help=f"共 {page_count} 页，每页最多 {PAGE_SIZE} 条。",
    )
    start = (int(page) - 1) * PAGE_SIZE
    st.caption(f"第 {page} / {page_count} 页 · 第 {start + 1}–{min(start + PAGE_SIZE, len(frame))} 条")
    st.html(build_report_table(frame.iloc[start:start + PAGE_SIZE], "inspection"))


def render_evaporation_dashboard(service: EvaporationReportService | None = None) -> None:
    """Injected services bypass the shared production cache entirely."""
    today = date.today()
    products = list(dict.fromkeys([*ConfigLoader.get_enabled_products(), "通用"]))
    product_key = "iqc_evap_product"
    preferred = st.session_state.get(SessionManager.KEY_PRODUCT)
    if st.session_state.get(product_key) not in products:
        st.session_state[product_key] = preferred if preferred in products else products[0]
    with st.container(border=True):
        date_column, product_column, query_column = st.columns([3, 2, 0.7], vertical_alignment="bottom")
        with date_column:
            dates = st.date_input(
                "检验日期范围", value=(today - timedelta(days=30), today),
                max_value=today, key="iqc_evap_dates",
            )
        with product_column:
            product = st.selectbox("产品型号", products, key=product_key)
        with query_column:
            query = st.button("查询", type="primary", key="iqc_evap_query", width="stretch")
    if len(dates) != 2:
        st.session_state.pop("iqc_evap_active", None)
        st.info("请选择完整的开始和结束日期。")
        return
    signature = (
        *dates, ConfigLoader.get_data_forward_policy().signature,
        ConfigLoader.get_report_cutoff_policy().signature, product,
    )
    if query:
        st.session_state["iqc_evap_active"] = signature
    if st.session_state.get("iqc_evap_active") != signature:
        st.info("请选择检验日期范围和产品型号并点击“查询”。")
        return
    try:
        with st.spinner("正在读取蒸镀材料检验数据…"):
            frame = (
                service.get_report(*dates, product_code=product) if service is not None
                else fetch_report_payload(*signature)
            )
    except Exception as exc:
        logger.error("IQC_REPORT_UNAVAILABLE exception_type=%s", type(exc).__name__)
        st.session_state.pop("iqc_evap_active", None)
        st.error("蒸镀材料检验数据暂时无法读取，请稍后重新查询。")
        return
    _render_results(frame, signature)
