"""Query and display the WMS evaporation report."""

import logging
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from app.components.product_labels import get_product_label_formatter
from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
from src.iqc_domain.application.eva_materials.alerts import (
    EvaporationAlertStore, publish_evaporation_alerts,
)
from src.iqc_domain.composition import build_evaporation_service, build_evaporation_alert_store
from src.iqc_domain.core.eva_materials.alerts import previous_week_window
from src.iqc_domain.core.eva_materials.evaporation import (
    PRODUCT_SCOPE_RULE_VERSION, REPORT_COLUMNS, RESULT_RULE_VERSION,
)
from src.shared_kernel.config import ConfigLoader
from app.sections.shared.alert_results import read_alert_result

logger = logging.getLogger(__name__)


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=32, show_spinner=False)
def fetch_report_payload(
    start: date, end: date, forward_signature: str, cutoff_signature: str,
    products: tuple[str, ...], enabled_products: tuple[str, ...], work_order_types: tuple[str, ...],
    *, result_rule_version: str = RESULT_RULE_VERSION,
    product_scope_rule_version: str = PRODUCT_SCOPE_RULE_VERSION,
) -> pd.DataFrame:
    """Cache only public, filtered facts; scope and policy changes separate entries."""
    return build_evaporation_service(
        enabled_products=enabled_products, work_order_types=work_order_types,
    ).get_report(start, end, products)


def _render_results(frame: pd.DataFrame) -> None:
    st.caption(f"共 {len(frame)} 条记录 · 左右滚动查看全部测点和检验结果")
    if frame.empty:
        st.info("没有符合筛选条件的记录，请调整筛选条件。")
        return
    st.caption("IQC／COA结果按测点判定：任一测点为NG则为NG，否则为OK；缺失测点按OK处理。")
    _render_report_table(frame)


def _render_report_table(frame: pd.DataFrame, *, height: int = 600) -> None:
    numeric_columns = ["规格上限", "规格下限", *[f"IQC-{i}" for i in range(1, 6)], *[f"COA-{i}" for i in range(1, 6)]]
    st.dataframe(
        frame.loc[:, list(REPORT_COLUMNS)], hide_index=True, width="stretch", height=height, placeholder="",
        column_config={
            "序号": st.column_config.NumberColumn(format="%d", pinned=True),
            "报检日期": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss"),
            "检验时间": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss"),
            **{column: st.column_config.NumberColumn(format="%.3f") for column in numeric_columns},
        },
    )


def _load_report(service: EvaporationReportService | None, signature: tuple) -> pd.DataFrame:
    if service is not None:
        return service.get_report(signature[0], signature[1], signature[4])
    return fetch_report_payload(
        *signature, result_rule_version=RESULT_RULE_VERSION,
        product_scope_rule_version=PRODUCT_SCOPE_RULE_VERSION,
    )


def _render_automatic_alerts(
    service: EvaporationReportService | None, *, today: date,
    enabled: tuple[str, ...], work_orders: tuple[str, ...],
    alert_store: EvaporationAlertStore | None,
) -> None:
    start, end = previous_week_window(today)
    week = start.isocalendar()
    title = f'蒸镀材料超规预警中心（上一周 {week.year}-W{week.week:02d}）'
    update_failed = False
    try:
        signature = (
            start, end, ConfigLoader.get_data_forward_policy().signature,
            ConfigLoader.get_report_cutoff_policy().signature, enabled, enabled, work_orders,
        )
        frame = _load_report(service, signature)
        publish_evaporation_alerts(
            frame, store=alert_store if alert_store is not None else build_evaporation_alert_store(),
            start=start, end=end, products=enabled,
        )
    except Exception as exc:
        logger.error('IQC_ALERT_UPDATE_UNAVAILABLE exception_type=%s', type(exc).__name__)
        update_failed = True
    _render_saved_automatic_alerts(today, title, start, end, update_failed=update_failed)


def _render_saved_automatic_alerts(
    today: date, title: str, start: date, end: date, *, update_failed: bool = False,
) -> None:
    result = read_alert_result('iqc_evaporation_ng', '', today)
    alerts = result['alerts']
    with st.expander(title, expanded=not alerts.empty or update_failed or result['state'] == 'error'):
        st.caption(f'检验日期：{start:%Y-%m-%d} 至 {end:%Y-%m-%d} · 全部已启用产品')
        if update_failed:
            st.warning('蒸镀材料预警数据暂时无法更新；以下状态以已保存结果为准。')
        if result['state'] == 'error':
            st.error(result['message'])
        elif not alerts.empty:
            st.error(f'检测到 {len(alerts)} 条蒸镀材料超规预警（IQC或COA结果为NG），请关注。')
            _render_report_table(alerts, height=400)
        elif result['state'] == 'no_data':
            st.info('上一周暂无蒸镀材料检验数据。')
        else:
            st.success('上一周未发现 IQC 或 COA 结果为 NG 的记录。')


def render_evaporation_dashboard(
    service: EvaporationReportService | None = None, *,
    alert_store: EvaporationAlertStore | None = None,
) -> None:
    """Injected services bypass the shared production cache entirely."""
    today = date.today()
    previous_month_start = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    try:
        enabled = tuple(ConfigLoader.get_enabled_products(strict=True))
        work_orders = tuple(ConfigLoader.get_work_order_types())
        product_label = get_product_label_formatter()
    except Exception as exc:
        logger.error("IQC_REPORT_CONFIGURATION_UNAVAILABLE exception_type=%s", type(exc).__name__)
        st.session_state.pop("iqc_evap_active", None)
        st.error("蒸镀材料检验筛选条件暂时无法加载，请稍后重试。")
        start, end = previous_week_window(today)
        _render_saved_automatic_alerts(today, '蒸镀材料超规预警中心', start, end)
        return
    if not enabled:
        st.session_state.pop("iqc_evap_active", None)
        st.session_state.pop("iqc_evap_products", None)
        st.info("暂无可查询的产品型号。")
        return
    saved = st.session_state.get("iqc_evap_products", [])
    st.session_state["iqc_evap_products"] = [code for code in saved if code in enabled]
    with st.container(border=True):
        date_column, product_column, query_column = st.columns([3, 2, 0.7], vertical_alignment="bottom")
        with date_column:
            dates = st.date_input(
                "检验日期范围", value=(previous_month_start, today),
                max_value=today, key="iqc_evap_dates",
            )
        with product_column:
            selected = st.multiselect(
                "产品型号", options=enabled, key="iqc_evap_products", placeholder="全部已启用产品",
                format_func=product_label,
            )
        with query_column:
            query = st.button("查询", type="primary", key="iqc_evap_query", width="stretch")
    _render_automatic_alerts(
        service, today=today, enabled=enabled, work_orders=work_orders, alert_store=alert_store,
    )
    if len(dates) != 2:
        st.session_state.pop("iqc_evap_active", None)
        st.info("请选择完整的开始和结束日期。")
        return
    signature = (
        *dates, ConfigLoader.get_data_forward_policy().signature,
        ConfigLoader.get_report_cutoff_policy().signature,
        tuple(code for code in enabled if not selected or code in selected), enabled, work_orders,
    )
    if query:
        st.session_state["iqc_evap_active"] = signature
    if st.session_state.get("iqc_evap_active") != signature:
        st.info("请选择检验日期范围和产品型号并点击“查询”。")
        return
    try:
        with st.spinner("正在读取蒸镀材料检验数据…"):
            frame = _load_report(service, signature)
    except Exception as exc:
        logger.error("IQC_REPORT_UNAVAILABLE exception_type=%s", type(exc).__name__)
        st.session_state.pop("iqc_evap_active", None)
        st.error("蒸镀材料检验数据暂时无法读取，请稍后重新查询。")
        return
    with st.container(key='iqc_evap_manual_results'):
        _render_results(frame)
