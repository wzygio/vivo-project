"""Query, filter and paginate the WMS evaporation report."""

import logging
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from app.sections.iqc_domain.report_table import build_report_table
from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
from src.iqc_domain.composition import build_evaporation_service
from src.iqc_domain.core.eva_materials.evaporation import FILTER_COLUMNS, filter_report
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)
PAGE_SIZE = 100


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=32, show_spinner=False)
def fetch_report_payload(
    start: date, end: date, forward_signature: str, cutoff_signature: str,
) -> pd.DataFrame:
    """Only native payloads cross the cache; signatures partition time policies."""
    return build_evaporation_service().get_report(start, end)


def _render_filters(frame: pd.DataFrame) -> dict[str, list[str]]:
    selections = {}
    with st.container(border=True):
        for row in (FILTER_COLUMNS[:3], FILTER_COLUMNS[3:]):
            for column, container in zip(row, st.columns(3)):
                options = sorted(frame[column].dropna().unique().tolist())
                key = f"iqc_evap_{column}"
                if key in st.session_state:
                    st.session_state[key] = [v for v in st.session_state[key] if v in options]
                with container:
                    selections[column] = st.multiselect(
                        column, options, placeholder="全部", key=key,
                    )
    return selections


def _render_results(frame: pd.DataFrame, signature: tuple) -> None:
    selections = _render_filters(frame)
    filtered = filter_report(frame, selections)
    st.caption(f"共 {len(filtered)} / {len(frame)} 条记录 · 左右滚动查看全部测点和检验结果")
    if filtered.empty:
        st.info("没有符合筛选条件的记录，请调整筛选条件。")
        return
    if filtered[["IQC结果", "COA结果"]].isna().any(axis=None):
        st.caption("结果空白表示尚未提供检验结论。")
    view_signature = (signature, tuple((k, tuple(v)) for k, v in selections.items()), len(filtered))
    page_count = (len(filtered) + PAGE_SIZE - 1) // PAGE_SIZE
    if st.session_state.get("iqc_evap_view") != view_signature:
        st.session_state["iqc_evap_page"] = 1
        st.session_state["iqc_evap_view"] = view_signature
    page = st.number_input(
        "页码", min_value=1, max_value=page_count, step=1, key="iqc_evap_page",
        help=f"共 {page_count} 页，每页最多 {PAGE_SIZE} 条。",
    )
    start = (int(page) - 1) * PAGE_SIZE
    st.caption(f"第 {page} / {page_count} 页 · 第 {start + 1}–{min(start + PAGE_SIZE, len(filtered))} 条")
    st.html(build_report_table(filtered.iloc[start:start + PAGE_SIZE], "inspection"))


def render_evaporation_dashboard(service: EvaporationReportService | None = None) -> None:
    """Injected services bypass the shared production cache entirely."""
    today = date.today()
    with st.container(border=True):
        dates = st.date_input(
            "检验日期范围", value=(today - timedelta(days=30), today),
            max_value=today, key="iqc_evap_dates",
        )
        query = st.button("查询", type="primary", key="iqc_evap_query")
    if len(dates) != 2:
        st.session_state.pop("iqc_evap_active", None)
        st.info("请选择完整的开始和结束日期。")
        return
    signature = (
        *dates, ConfigLoader.get_data_forward_policy().signature,
        ConfigLoader.get_report_cutoff_policy().signature,
    )
    if query:
        st.session_state["iqc_evap_active"] = signature
    if st.session_state.get("iqc_evap_active") != signature:
        st.info("请选择检验日期范围并点击“查询”。")
        return
    try:
        with st.spinner("正在读取蒸镀材料检验数据…"):
            frame = (
                service.get_report(*dates) if service is not None
                else fetch_report_payload(*signature)
            )
    except Exception as exc:
        logger.error("IQC_REPORT_UNAVAILABLE exception_type=%s", type(exc).__name__)
        st.session_state.pop("iqc_evap_active", None)
        st.error("蒸镀材料检验数据暂时无法读取，请稍后重新查询。")
        return
    _render_results(frame, signature)
