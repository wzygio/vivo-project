"""Enabled product scope, measurement filters and grouped lifetime presentation."""

import logging
from collections.abc import Callable
from datetime import date

import pandas as pd
import streamlit as st

from app.components.product_labels import get_product_label_formatter
from app.charts.iqc_domain.lifetime import build_lifetime_chart
from app.sections.shared.alert_results import read_alert_result
from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
from src.iqc_domain.composition import build_lifetime_service, load_lifetime_specs
from src.iqc_domain.core.lifetime.alerts import LIFETIME_THRESHOLD, evaluate_lifetime_specs
from src.iqc_domain.core.lifetime.lifetime import filter_report
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=1, show_spinner=False)
def fetch_report_payload(enabled_products: tuple[str, ...] | None = None) -> pd.DataFrame:
    """Cache anonymous public facts including JNCD. Failed reads are never cached."""
    service = (build_lifetime_service() if enabled_products is None
               else build_lifetime_service(enabled_products=enabled_products))
    return service.get_report()


def _select(
    label: str, options: list[str], key: str, *, format_func: Callable[[str], str] = str,
) -> list[str]:
    if key in st.session_state:
        st.session_state[key] = [v for v in st.session_state[key] if v in options]
    return st.multiselect(label, options, placeholder='全部', key=key, format_func=format_func)


def _render_filters(
    frame: pd.DataFrame, enabled_products: list[str],
) -> tuple[list[str], list[str], list[str]] | None:
    with st.container(border=True):
        product_column, status_column, batch_column = st.columns(3)
        with product_column:
            products = _select(
                '产品型号', enabled_products, 'iqc_lifetime_products',
                format_func=get_product_label_formatter(),
            )
        with status_column:
            scope = filter_report(frame, products, [])
            statuses = _select('产品状态', sorted(scope['产品状态'].dropna().unique().tolist()),
                               'iqc_lifetime_statuses')
        with batch_column:
            scope = filter_report(scope, [], [], statuses)
            batches = _select('批次号', sorted(scope['批次号'].unique().tolist()),
                              'iqc_lifetime_batches')
        submitted = st.button('查询', type='primary', key='iqc_lifetime_query')
    applied_key = 'iqc_lifetime_submitted_filters'
    if submitted:
        st.session_state[applied_key] = (products, batches, statuses)
    if applied_key not in st.session_state:
        return None
    draft = (products, batches, statuses)
    # Draft selectors remain linked, while results use the last submitted filters.
    products, batches, statuses = st.session_state[applied_key]
    products = [value for value in products if value in enabled_products]
    scope = filter_report(frame, products, [])
    statuses = [value for value in statuses if value in scope['产品状态'].dropna().unique()]
    scope = filter_report(scope, [], [], statuses)
    batches = [value for value in batches if value in scope['批次号'].unique()]
    st.session_state[applied_key] = (products, batches, statuses)
    if draft != (products, batches, statuses):
        st.caption('筛选条件已修改，当前仍展示上次查询结果；点击“查询”更新。')
    return products, batches, statuses


def _render_trends(frame: pd.DataFrame, specs: dict[tuple[str, str], float]) -> None:
    st.subheader('寿命数据曲线图')
    for index, ((product, batch), scope) in enumerate(
        frame.groupby(['产品型号', '批次号'], sort=True)
    ):
        with st.expander(f'{product} · {batch}', expanded=True):
            evaluations = evaluate_lifetime_specs(scope, specs)
            for metric in ('效率衰减', '亮度衰减', 'JNCD'):
                _render_metric_charts(scope, metric, index, specs, evaluations)


def _render_metric_charts(
    scope: pd.DataFrame, metric: str, group_index: int,
    specs: dict[tuple[str, str], float], evaluations: pd.DataFrame,
) -> None:
    if metric == 'JNCD':
        scope = scope.loc[scope['测试画面'].eq('W')]
    order = {'W': 0, 'R': 1, 'G': 2, 'B': 3}
    screens = sorted(scope['测试画面'].unique(), key=lambda x: (order.get(x, 4), x))
    with st.expander(metric, expanded=True):
        if scope.empty:
            st.info('该组暂无 W 画面测点。')
            return
        for offset in range(0, len(screens), 2):
            for screen, column in zip(screens[offset:offset + 2], st.columns(2)):
                group = scope.loc[scope['测试画面'].eq(screen)]
                hour = specs.get((group['产品型号'].iloc[0], screen)) if metric != 'JNCD' else None
                points = evaluations.loc[
                    evaluations['指标'].eq(metric) & evaluations['测试画面'].eq(screen)
                ]
                with column:
                    _render_screen_chart(group, str(screen), metric,
                                         f'{group_index}_{metric}_{screen}', hour, points)


def _render_screen_chart(
    group: pd.DataFrame, screen: str, metric: str, chart_index: str,
    spec_hour: float | None, spec_points: pd.DataFrame,
) -> None:
    st.text(screen)
    st.caption(f'{group["样品编号"].nunique()} 个样品 · {len(group)} 个测点')
    if group[metric].notna().any():
        figure = build_lifetime_chart(group, metric=metric, spec_hour=spec_hour, spec_points=spec_points)
        st.plotly_chart(figure, width='stretch',
                        key=f'iqc_lifetime_chart_{chart_index}',
                        config={'displayModeBar': False, 'scrollZoom': False})
    else:
        st.info(f'该组暂无有效{metric}测点。')


def _render_details(frame: pd.DataFrame) -> None:
    st.subheader('寿命测试明细')
    st.caption(f'共 {len(frame)} 条记录')
    st.dataframe(frame.reset_index(drop=True), hide_index=True, width='stretch')


def _render_alert_center(result: dict) -> None:
    state = result['state']
    with st.expander('寿命测试预警中心', expanded=state in ('alert', 'error')):
        st.caption(f'全部已启用产品 · 效率衰减或亮度衰减在规格时间点低于 {LIFETIME_THRESHOLD:g} 时预警。')
        if state == 'error':
            st.error(result['message'])
        elif state == 'alert':
            alerts = result['alerts']
            st.error(f'发现 {len(alerts)} 个低于 {LIFETIME_THRESHOLD:g} 的规格点。')
            st.dataframe(alerts, hide_index=True, width='stretch', column_config={
                '规格点数值': st.column_config.NumberColumn(format='plain'),
            })
        elif state == 'no_data':
            st.info('暂无可判定的寿命测试规格点。')
        else:
            st.success(f'未发现低于 {LIFETIME_THRESHOLD:g} 的规格点。')


def render_lifetime_dashboard(
    service: LifetimeReportService | None = None, *, saved_alert_result: dict | None = None,
) -> None:
    _render_alert_center(saved_alert_result if saved_alert_result is not None
                         else read_alert_result('iqc_lifetime_oos', '', date.today()))
    try:
        enabled_products = list(dict.fromkeys(ConfigLoader.get_enabled_products(strict=True)))
        with st.spinner('正在读取寿命测试数据…'):
            frame = (service.get_report() if service is not None
                     else fetch_report_payload(tuple(enabled_products)))
    except Exception as exc:
        logger.error('LIFETIME_REPORT_UNAVAILABLE exception_type=%s', type(exc).__name__)
        st.error('寿命测试数据暂时无法读取，请稍后刷新重试。')
        return
    # Apply the allowlist even when "all" is selected; never send disabled facts to UI.
    frame = frame.loc[frame['产品型号'].isin(enabled_products)].copy()
    try:
        specs = load_lifetime_specs()
    except Exception as exc:
        logger.error('LIFETIME_SPECS_UNAVAILABLE exception_type=%s', type(exc).__name__)
        st.warning('寿命规格暂时无法读取，曲线仅展示测试数据和预测线。')
        specs = {}
    render_lifetime_interaction(frame, enabled_products, specs)


@st.fragment
def render_lifetime_interaction(
    frame: pd.DataFrame, enabled_products: list[str], specs: dict[tuple[str, str], float],
) -> None:
    """Reuse the page's loaded facts while editing the linked draft selectors."""
    filters = _render_filters(frame, enabled_products)
    if filters is None:
        st.info('请点击“查询”查看寿命测试数据。')
        return
    products, batches, statuses = filters
    selected = filter_report(frame, products, batches, statuses)
    if selected.empty:
        st.info('没有符合筛选条件的寿命测试记录，请调整筛选条件或稍后刷新。')
        return
    st.caption('样品编号按产品型号、批次号、测试画面分别编排；衰减值按比例展示。')
    with st.container(key='iqc_lifetime_manual_results'):
        _render_trends(selected, specs)
        _render_details(selected)
