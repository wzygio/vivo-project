"""Query-gated large-hole presentation with native public session payloads."""

import pandas as pd
import streamlit as st

from app.charts.indicator_domain.ijp_hole.chart import build_hole_figure
from app.sections.indicator_domain.ijp.filters import render_ijp_filter_columns
from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.errors import IjpDataAccessError
from src.indicator_domain.application.ijp_hole.service import IjpHoleReportService
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.report_cutoff_config import load_report_cutoff_policy

RESULT_KEY = 'ijp_hole_result'
VIEW_LABELS = {'玻璃': 'glass', 'Total': 'total', '按天': 'day'}


def render_ijp_hole_dashboard(service: IjpHoleReportService) -> None:
    st.subheader('OLED IJP 孔区溢流监控（大孔）', anchor=False, text_alignment='center')
    start, end = service.get_reporting_window()
    with st.container(border=True):
        st.caption(f'数据范围：{start:%Y/%m/%d} 至 {end:%Y/%m/%d}（上月 1 日至今天）')
        filter_columns = render_ijp_filter_columns()
        try:
            options = service.get_filter_options(tuple(st.session_state.get('ijp_hole_products', ())))
        except IjpDataAccessError:
            st.session_state.pop(RESULT_KEY, None)
            st.error('IJP 孔区筛选项暂时无法读取，请稍后重试。')
            return
        values = {}
        fields = [('产品型号', 'products', 'product_codes'), ('线体', 'lines', 'lines'),
                  ('CODE', 'codes', 'codes'), ('批次', 'picis', 'picis')]
        for column, (label, key, option) in zip(filter_columns, fields, strict=True):
            widget_key = 'ijp_hole_' + key
            available = options[option]
            current = list(st.session_state.get(widget_key, ()))
            retained = [item for item in current if item in available]
            if current != retained:
                st.session_state[widget_key] = retained
            with column:
                values[key] = tuple(st.multiselect(label, available, key=widget_key))
        clicked = st.button('查询', type='primary', width='stretch', key='ijp_hole_search')
    signature = (
        start, end, *values.values(), service.enabled_products, service.work_order_types,
        ConfigLoader.get_data_forward_policy().signature, load_report_cutoff_policy().signature,
    )
    if clicked:
        st.session_state.pop(RESULT_KEY, None)
        try:
            report = service.get_report(IjpQuery(
                start_time=start, end_time=end, product_codes=values['products'],
                lines=values['lines'], codes=values['codes'], picis=values['picis'],
            ))
            st.session_state[RESULT_KEY] = {'signature': signature, 'report': report}
        except IjpDataAccessError:
            st.error('IJP 孔区数据暂时无法读取，请稍后重试。')
            return
    stored = st.session_state.get(RESULT_KEY)
    if not stored or stored['signature'] != signature:
        st.info('请选择筛选条件并点击“查询”。')
        return
    view_label = st.radio('统计视图', list(VIEW_LABELS), horizontal=True, key='ijp_hole_view')
    view = VIEW_LABELS[view_label]
    frame = stored['report'][view]
    if frame.empty:
        st.info('当前筛选条件下暂无 IJP 大孔数据。')
        return
    render_hole_charts(frame, view)


def render_hole_charts(frame: pd.DataFrame, view: str) -> None:
    for product, product_frame in frame.groupby('productcode', sort=False):
        with st.expander(f'产品：{product}', expanded=True):
            for line, line_frame in product_frame.groupby('line', sort=True):
                with st.expander(f'线体：{line}', expanded=True):
                    printers = list(line_frame.printer.unique())
                    for start in range(0, len(printers), 3):
                        row = printers[start:start + 3]
                        for column, printer in zip(st.columns(len(row)), row, strict=True):
                            with column:
                                st.plotly_chart(
                                    build_hole_figure(line_frame[line_frame.printer.eq(printer)],
                                                      title=str(printer), view=view),
                                    width='stretch', key=f'ijp_hole_{view}_{product}_{line}_{printer}',
                                )
