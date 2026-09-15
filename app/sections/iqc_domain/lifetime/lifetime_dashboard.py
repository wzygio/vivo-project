"""Enabled product scope, measurement filters and grouped lifetime presentation."""

import logging

import pandas as pd
import streamlit as st

from app.charts.iqc_domain.lifetime import build_lifetime_chart
from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
from src.iqc_domain.composition import build_lifetime_service
from src.iqc_domain.core.lifetime.lifetime import MEASUREMENT_COLUMNS, filter_report
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=1, show_spinner=False)
def fetch_report_payload() -> pd.DataFrame:
    """Cache only anonymous public facts. Failed reads are never cached."""
    return build_lifetime_service().get_report()


def _select(label: str, options: list[str], key: str) -> list[str]:
    if key in st.session_state:
        st.session_state[key] = [v for v in st.session_state[key] if v in options]
    return st.multiselect(label, options, placeholder='全部', key=key)


def _render_filters(
    frame: pd.DataFrame, enabled_products: list[str],
) -> tuple[list[str], list[str], list[str]]:
    with st.container(border=True):
        product_column, status_column, batch_column = st.columns(3)
        with product_column:
            products = _select('产品型号', enabled_products,
                               'iqc_lifetime_products')
        with status_column:
            scope = filter_report(frame, products, [])
            statuses = _select('产品状态', sorted(scope['产品状态'].dropna().unique().tolist()),
                               'iqc_lifetime_statuses')
        with batch_column:
            scope = filter_report(scope, [], [], statuses)
            batches = _select('批次号', sorted(scope['批次号'].unique().tolist()),
                              'iqc_lifetime_batches')
    return products, batches, statuses


def _render_trends(frame: pd.DataFrame) -> None:
    st.subheader('寿命数据曲线图')
    order = {'W': 0, 'R': 1, 'G': 2, 'B': 3}
    chart_index = 0
    for (product, batch), scope in frame.groupby(['产品型号', '批次号'], sort=True):
        screens = sorted(scope['测试画面'].unique(), key=lambda x: (order.get(x, 4), x))
        with st.expander(f'{product} · {batch}', expanded=True):
            for offset in range(0, len(screens), 4):
                for screen, column in zip(screens[offset:offset + 4], st.columns(4)):
                    with column:
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


def _render_details(frame: pd.DataFrame) -> None:
    st.subheader('寿命测试明细')
    st.caption(f'共 {len(frame)} 条记录')
    st.dataframe(frame.reset_index(drop=True), hide_index=True, width='stretch')


def render_lifetime_dashboard(service: LifetimeReportService | None = None) -> None:
    try:
        with st.spinner('正在读取寿命测试数据…'):
            frame = service.get_report() if service is not None else fetch_report_payload()
    except Exception as exc:
        logger.error('LIFETIME_REPORT_UNAVAILABLE exception_type=%s', type(exc).__name__)
        st.error('寿命测试数据暂时无法读取，请稍后刷新重试。')
        return
    enabled_products = list(dict.fromkeys(ConfigLoader.get_enabled_products()))
    # Apply the allowlist even when "all" is selected; never send disabled facts to UI.
    frame = frame.loc[frame['产品型号'].isin(enabled_products)].copy()
    products, batches, statuses = _render_filters(frame, enabled_products)
    selected = filter_report(frame, products, batches, statuses)
    if selected.empty:
        st.info('没有符合筛选条件的寿命测试记录，请调整筛选条件或稍后刷新。')
        return
    st.caption('样品编号按产品型号、批次号、测试画面分别编排；衰减值按比例展示。')
    if selected[MEASUREMENT_COLUMNS].isna().any(axis=None):
        st.warning('部分测量值缺失，明细保留空白，趋势图在缺失测点处断开。')
    _render_trends(selected)
    _render_details(selected)
