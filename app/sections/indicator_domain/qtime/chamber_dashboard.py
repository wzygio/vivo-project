"""Independent chamber monitoring section below the existing station report."""

from collections.abc import Callable
from datetime import date

import pandas as pd
import streamlit as st

from app.components.product_labels import get_product_label_formatter
from app.charts.indicator_domain.qtime.chamber_chart import build_chamber_figure
from src.indicator_domain.application.qtime.chamber_cache import cached_chamber_report, clear_chamber_cache
from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.core.qtime.chamber import CHAMBERS, exclude_chamber_outliers

RESULT_KEY = 'chamber_qtime_view_model'
SIGNATURE_KEY = 'chamber_qtime_signature'
# Temporary frontend cutoff requested for the chamber monitoring report.
DISPLAY_CUTOFF = pd.Timestamp('2026-10-01')


def clear_chamber_result() -> bool:
    st.session_state.pop(RESULT_KEY, None)
    st.session_state.pop(SIGNATURE_KEY, None)
    return True


def refresh_chamber_data() -> bool:
    clear_chamber_cache()
    return clear_chamber_result()


def _multiselect(
    label: str, options: list[str], *, key: str, default: list[str],
    format_func: Callable[[str], str] = str,
) -> list[str]:
    if key in st.session_state:
        previous = st.session_state[key]
        valid = [value for value in previous if value in options]
        if valid != previous:
            st.session_state[key] = valid
        return st.multiselect(label, options, key=key, placeholder='全部', format_func=format_func)
    return st.multiselect(
        label, options, default=default, key=key, placeholder='全部', format_func=format_func,
    )


@st.fragment
def render_chamber_dashboard(service: ChamberQTimeService) -> None:
    st.subheader('蒸镀腔室QTime监控', anchor=False, text_alignment='center')
    with st.container(border=True):
        columns = st.columns([1.4, 1.2, 2.4, 0.7], vertical_alignment='bottom')
        with columns[0]:
            products = _multiselect(
                '产品型号', list(service.enabled_products), key='chamber_products', default=[],
                format_func=get_product_label_formatter(),
            )
        with columns[1]:
            lines = _multiselect('线体', ['3CEE001', '3CEE002'], key='chamber_lines', default=[])
        with columns[2]:
            chambers = _multiselect('腔室', list(CHAMBERS), key='chamber_names', default=[CHAMBERS[0]])
        with columns[3]:
            query = st.button('查询单腔', type='primary', width='stretch', key='chamber_search')
    signature = (service.cache_signature(), date.today().isoformat())
    if st.session_state.get(SIGNATURE_KEY) != signature:
        clear_chamber_result()
    if query:
        clear_chamber_result()
        try:
            with st.spinner('正在读取单腔停留时间…'):
                st.session_state[RESULT_KEY] = cached_chamber_report(
                    service, as_of=date.today(),
                )
            st.session_state[SIGNATURE_KEY] = signature
        except QTimeDataAccessError:
            st.error('蒸镀单腔停留时间数据读取失败，请稍后重试。')
            return
    report = st.session_state.get(RESULT_KEY)
    if report is None:
        st.info('请选择筛选条件并点击“查询单腔”。')
        return
    # Reapply enabled scope on every rerun before constructing frontend payloads.
    details = report['details']
    selected = details.loc[
        details['prod_code'].isin(products or service.enabled_products)
        & details['prod_code'].isin(service.enabled_products)
        & details['line'].isin(lines or ['3CEE001', '3CEE002'])
        & details['chamber'].isin(chambers or CHAMBERS)
        & details['entry_time'].lt(DISPLAY_CUTOFF)
    ].copy()
    selected = exclude_chamber_outliers(selected)
    if selected.empty:
        st.info('当前筛选条件下暂无单腔停留时间数据。')
        return
    _render_results(selected, chambers or list(CHAMBERS), service.enabled_products)


def _render_results(details: pd.DataFrame, chambers: list[str], products: tuple[str, ...]) -> None:
    for line, line_details in details.groupby('line', sort=True):
        for chamber in chambers:
            subset = line_details.loc[line_details['chamber'].eq(chamber)]
            if subset.empty:
                continue
            with st.expander(f"{line} - {chamber.replace('->', ' → ')}", expanded=True):
                if subset['duration_seconds'].notna().any():
                    st.plotly_chart(
                        build_chamber_figure(subset, chamber=chamber, product_order=products),
                        width='stretch', key=f'chamber_chart_{line}_{chamber}',
                    )
                else:
                    st.info('当前组别暂无有效测量值。')
