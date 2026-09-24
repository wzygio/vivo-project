"""Independent chamber monitoring section below the existing station report."""

from datetime import date

import pandas as pd
import streamlit as st

from app.charts.indicator_domain.qtime.chamber_chart import build_chamber_figure
from src.indicator_domain.application.qtime.chamber_cache import cached_chamber_report, clear_chamber_cache
from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.core.qtime.chamber import CHAMBERS, summarize_chambers

RESULT_KEY = 'chamber_qtime_view_model'
SIGNATURE_KEY = 'chamber_qtime_signature'
PUBLIC_COLUMNS = {
    'prod_code': '产品型号', 'line': '线体', 'glass_id': 'GlassID', 'entry_time': '进片时间',
    'chamber': '腔室', 'duration_seconds': '停留时间（秒）', 'target_seconds': '目标值（秒）', 'status': '状态',
}
SUMMARY_COLUMNS = {
    'prod_code': '产品型号', 'line': '线体', 'chamber': '腔室', 'measured_count': '有效测量数',
    'missing_count': '缺失数', 'exceeded_count': '超限数', 'mean_seconds': '平均时间（秒）',
    'max_seconds': '最长时间（秒）', 'target_seconds': '目标值（秒）', 'exceeded_ratio': '超限率',
}


def clear_chamber_result() -> bool:
    st.session_state.pop(RESULT_KEY, None)
    st.session_state.pop(SIGNATURE_KEY, None)
    return True


def refresh_chamber_data() -> bool:
    clear_chamber_cache()
    return clear_chamber_result()


def _multiselect(label: str, options: list[str], *, key: str, default: list[str]) -> list[str]:
    if key in st.session_state:
        previous = st.session_state[key]
        valid = [value for value in previous if value in options]
        if valid != previous:
            st.session_state[key] = valid
        return st.multiselect(label, options, key=key, placeholder='全部')
    return st.multiselect(label, options, default=default, key=key, placeholder='全部')


def render_chamber_dashboard(service: ChamberQTimeService) -> None:
    st.subheader('蒸镀单腔停留时间监控', anchor=False, text_alignment='center')
    with st.container(border=True):
        columns = st.columns([1.4, 1.2, 2.4, 0.7], vertical_alignment='bottom')
        with columns[0]:
            products = _multiselect('产品型号', list(service.enabled_products), key='chamber_products', default=[])
        with columns[1]:
            lines = _multiselect('线体', ['3CEE001', '3CEE002'], key='chamber_lines', default=[])
        with columns[2]:
            chambers = _multiselect('腔室', list(CHAMBERS), key='chamber_names', default=[CHAMBERS[0]])
        with columns[3]:
            query = st.button('查询单腔', type='primary', width='stretch', key='chamber_search')
    st.caption('空选表示全部；按进片时间展示，空缺测量不计入超限率。')
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
    # Reapply enabled scope on every rerun before any frontend or CSV payload.
    details = report['details']
    selected = details.loc[
        details['prod_code'].isin(products or service.enabled_products)
        & details['prod_code'].isin(service.enabled_products)
        & details['line'].isin(lines or ['3CEE001', '3CEE002'])
        & details['chamber'].isin(chambers or CHAMBERS)
    ].copy()
    if report['unmatched_glasses'] or report['ambiguous_glasses']:
        st.warning('部分玻璃的产品归属尚未确认，已从统计中排除；当前结果仅覆盖已确认产品的数据。')
    if report['invalid_rows']:
        st.warning('部分记录缺少玻璃编号或有效进片时间，未纳入统计。')
    if selected.empty:
        st.info('当前筛选条件下暂无单腔停留时间数据。')
        return
    _render_results(selected, chambers or list(CHAMBERS))


def _render_results(details: pd.DataFrame, chambers: list[str]) -> None:
    minimum, maximum = details['entry_time'].min(), details['entry_time'].max()
    st.caption(f'数据覆盖：{minimum:%Y-%m-%d %H:%M} 至 {maximum:%Y-%m-%d %H:%M}')
    valid = details['duration_seconds'].notna().sum()
    exceeded = details['status'].eq('超限').sum()
    with st.container(horizontal=True):
        st.metric('玻璃数', f"{details[['line', 'glass_id']].drop_duplicates().shape[0]:,}", border=True)
        st.metric('有效测量数', f'{valid:,}', border=True)
        st.metric('超限数', f'{exceeded:,}', border=True)
        st.metric('超限率', f'{exceeded / valid:.2%}' if valid else '—', border=True)
    for chamber in chambers:
        subset = details.loc[details['chamber'].eq(chamber)]
        if not subset.empty:
            st.plotly_chart(build_chamber_figure(subset, chamber=chamber), width='stretch', key=f'chamber_chart_{chamber}')
    summary = summarize_chambers(details).rename(columns=SUMMARY_COLUMNS)
    st.dataframe(summary, hide_index=True, width='stretch',
                 column_config={'超限率': st.column_config.NumberColumn(format='percent')})
    public = details[list(PUBLIC_COLUMNS)].rename(columns=PUBLIC_COLUMNS)
    with st.expander('单腔停留时间明细', expanded=False):
        st.dataframe(public, hide_index=True, width='stretch')
        st.download_button('下载单腔明细', public.to_csv(index=False).encode('utf-8-sig'),
                           file_name='蒸镀单腔停留时间.csv', mime='text/csv', key='chamber_download')
