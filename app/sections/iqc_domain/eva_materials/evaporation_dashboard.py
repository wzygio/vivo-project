"""Query and display the WMS evaporation report."""

import logging
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
from src.iqc_domain.composition import build_evaporation_service
from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS, RESULT_RULE_VERSION
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=32, show_spinner=False)
def fetch_report_payload(
    start: date, end: date, forward_signature: str, cutoff_signature: str,
    *, result_rule_version: str = RESULT_RULE_VERSION,
) -> pd.DataFrame:
    """Cache organic-material payloads by time policies and decision-rule version."""
    return build_evaporation_service().get_report(start, end)


def _render_results(frame: pd.DataFrame) -> None:
    st.caption(f"共 {len(frame)} 条记录 · 左右滚动查看全部测点和检验结果")
    if frame.empty:
        st.info("没有符合筛选条件的记录，请调整筛选条件。")
        return
    st.caption("IQC／COA结果按测点判定：任一测点为NG则为NG，否则为OK；缺失测点按OK处理。")
    numeric_columns = ["规格上限", "规格下限", *[f"IQC-{i}" for i in range(1, 6)], *[f"COA-{i}" for i in range(1, 6)]]
    st.dataframe(
        frame.loc[:, list(REPORT_COLUMNS)], hide_index=True, width="stretch", height=600, placeholder="",
        column_config={
            "序号": st.column_config.NumberColumn(format="%d", pinned=True),
            "报检日期": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss"),
            "检验时间": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss"),
            **{column: st.column_config.NumberColumn(format="%.3f") for column in numeric_columns},
        },
    )


def render_evaporation_dashboard(service: EvaporationReportService | None = None) -> None:
    """Injected services bypass the shared production cache entirely."""
    today = date.today()
    with st.container(border=True):
        date_column, query_column = st.columns([5, 0.7], vertical_alignment="bottom")
        with date_column:
            dates = st.date_input(
                "检验日期范围", value=(today - timedelta(days=30), today),
                max_value=today, key="iqc_evap_dates",
            )
        with query_column:
            query = st.button("查询", type="primary", key="iqc_evap_query", width="stretch")
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
                else fetch_report_payload(*signature, result_rule_version=RESULT_RULE_VERSION)
            )
    except Exception as exc:
        logger.error("IQC_REPORT_UNAVAILABLE exception_type=%s", type(exc).__name__)
        st.session_state.pop("iqc_evap_active", None)
        st.error("蒸镀材料检验数据暂时无法读取，请稍后重新查询。")
        return
    _render_results(frame)
