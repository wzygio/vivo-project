"""Two business filters, grouped curves and paginated measurement details."""

import logging

import pandas as pd
import streamlit as st

from app.charts.iqc_domain.lifetime import build_lifetime_chart
from app.sections.iqc_domain.report_table import build_report_table
from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
from src.iqc_domain.composition import build_lifetime_service
from src.iqc_domain.core.lifetime.lifetime import MEASUREMENT_COLUMNS, filter_report
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)
PAGE_SIZE = 100


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=1, show_spinner=False)
def fetch_report_payload() -> pd.DataFrame:
    """Cache only anonymous public facts. Failed reads are never cached."""
    return build_lifetime_service().get_report()


def _select(label: str, options: list[str], key: str) -> list[str]:
    if key in st.session_state:
        st.session_state[key] = [v for v in st.session_state[key] if v in options]
    return st.multiselect(label, options, placeholder='全部', key=key)


def _render_filters(frame: pd.DataFrame) -> tuple[list[str], list[str]]:
    with st.container(border=True):
        product_column, batch_column = st.columns(2)
        with product_column:
            products = _select('产品型号', sorted(frame['产品型号'].unique().tolist()),
                               'iqc_lifetime_products')
        with batch_column:
            scope = filter_report(frame, products, [])
            batches = _select('批次号', sorted(scope['批次号'].unique().tolist()),
                              'iqc_lifetime_batches')
    return products, batches


def _render_trends(frame: pd.DataFrame) -> None:
    st.subheader('寿命数据曲线图')
    order = {'W': 0, 'R': 1, 'G': 2, 'B': 3}
    chart_index = 0
    for (product, batch), scope in frame.groupby(['产品型号', '批次号'], sort=True):
        st.text(f'{product} · {batch}')
        screens = sorted(scope['测试画面'].unique(), key=lambda x: (order.get(x, 4), x))
        for offset in range(0, len(screens), 2):
            for screen, column in zip(screens[offset:offset + 2], st.columns(2)):
                with column.container(border=True):
                    group = scope.loc[scope['测试画面'].eq(screen)]
                    st.text(str(screen))
                    st.caption(f'{group["样品编号"].nunique()} 个样品 · {len(group)} 个测点')
                    if group['效率衰减'].notna().any():
                        st.plotly_chart(build_lifetime_chart(group), width='stretch',
                                        key=f'iqc_lifetime_chart_{chart_index}',
                                        config={'displayModeBar': False})
                    else:
                        st.info('该组暂无有效效率衰减测点。')
                    chart_index += 1


def _render_details(frame: pd.DataFrame, selections: tuple) -> None:
    st.subheader('寿命测试明细')
    page_count = (len(frame) + PAGE_SIZE - 1) // PAGE_SIZE
    signature = (selections, len(frame))
    if st.session_state.get('iqc_lifetime_view') != signature:
        st.session_state['iqc_lifetime_page'] = 1
        st.session_state['iqc_lifetime_view'] = signature
    page = st.number_input('页码', min_value=1, max_value=page_count, step=1,
                           key='iqc_lifetime_page')
    start = (int(page) - 1) * PAGE_SIZE
    st.caption(f'共 {len(frame)} 条记录 · 第 {page} / {page_count} 页 · 每页 {PAGE_SIZE} 条')
    st.html(build_report_table(frame.iloc[start:start + PAGE_SIZE], 'lifetime'))


def render_lifetime_dashboard(service: LifetimeReportService | None = None) -> None:
    if st.button('刷新数据', key='iqc_lifetime_refresh') and service is None:
        fetch_report_payload.clear()
    try:
        with st.spinner('正在读取寿命测试数据…'):
            frame = service.get_report() if service is not None else fetch_report_payload()
    except Exception as exc:
        logger.error('LIFETIME_REPORT_UNAVAILABLE exception_type=%s', type(exc).__name__)
        st.error('寿命测试数据暂时无法读取，请稍后刷新重试。')
        return
    products, batches = _render_filters(frame)
    selected = filter_report(frame, products, batches)
    if selected.empty:
        st.info('没有符合筛选条件的寿命测试记录，请调整筛选条件或稍后刷新。')
        return
    st.caption('样品编号按产品型号、批次号、测试画面分别编排；衰减值按比例展示。')
    if selected[MEASUREMENT_COLUMNS].isna().any(axis=None):
        st.warning('部分测量值缺失，明细保留空白，趋势图在缺失测点处断开。')
    _render_trends(selected)
    _render_details(selected, (tuple(products), tuple(batches)))
