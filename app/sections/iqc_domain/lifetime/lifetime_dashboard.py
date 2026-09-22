"""Enabled product scope, measurement filters and grouped lifetime presentation."""

import logging

import pandas as pd
import streamlit as st

from app.charts.iqc_domain.lifetime import build_lifetime_chart
from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
from src.iqc_domain.composition import build_lifetime_service
from src.iqc_domain.core.lifetime.lifetime import filter_report
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
        submitted = st.button('查询', type='primary', key='iqc_lifetime_query')
    applied_key = 'iqc_lifetime_applied_filters'
    if submitted or applied_key not in st.session_state:
        st.session_state[applied_key] = (products, batches, statuses)
    # Draft selectors remain linked, while results use the last submitted filters.
    products, batches, statuses = st.session_state[applied_key]
    products = [value for value in products if value in enabled_products]
    scope = filter_report(frame, products, [])
    statuses = [value for value in statuses if value in scope['产品状态'].dropna().unique()]
    scope = filter_report(scope, [], [], statuses)
    batches = [value for value in batches if value in scope['批次号'].unique()]
    st.session_state[applied_key] = (products, batches, statuses)
    return products, batches, statuses


def _render_trends(frame: pd.DataFrame) -> None:
    st.subheader('寿命数据曲线图')
    for index, ((product, batch), scope) in enumerate(
        frame.groupby(['产品型号', '批次号'], sort=True)
    ):
        with st.expander(f'{product} · {batch}', expanded=True):
            for metric in ('效率衰减', '亮度衰减'):
                _render_metric_charts(scope, metric, index)


def _render_metric_charts(scope: pd.DataFrame, metric: str, group_index: int) -> None:
    order = {'W': 0, 'R': 1, 'G': 2, 'B': 3}
    screens = sorted(scope['测试画面'].unique(), key=lambda x: (order.get(x, 4), x))
    with st.expander(metric, expanded=True):
        for offset in range(0, len(screens), 4):
            for screen, column in zip(screens[offset:offset + 4], st.columns(4)):
                group = scope.loc[scope['测试画面'].eq(screen)]
                with column:
                    _render_screen_chart(group, str(screen), metric,
                                         f'{group_index}_{metric}_{screen}')


def _render_screen_chart(
    group: pd.DataFrame, screen: str, metric: str, chart_index: str,
) -> None:
    st.text(screen)
    st.caption(f'{group["样品编号"].nunique()} 个样品 · {len(group)} 个测点')
    if group[metric].notna().any():
        st.plotly_chart(build_lifetime_chart(group, metric=metric), width='stretch',
                        key=f'iqc_lifetime_chart_{chart_index}',
                        config={'displayModeBar': False})
    else:
        st.info(f'该组暂无有效{metric}测点。')


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
    _render_trends(selected)
    _render_details(selected)
