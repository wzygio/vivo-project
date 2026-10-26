"""Product tabs for half-year, quarter and monthly loss trends."""
import logging
from datetime import datetime

import pandas as pd
import streamlit as st

from app.charts.yield_domain.period_trend_chart import create_period_trend_chart
from app.components.product_labels import get_product_label_formatter
from app.sections.yield_domain.period_trend_data import export_period_tables, load_product_period_trends
from src.shared_kernel.config import ConfigLoader
from src.yield_domain.core.mwd_trend.period_trend import period_months, project_report

logger = logging.getLogger(__name__)
PRODUCTS_KEY = 'all_product_yield_products'
LOADED_KEY = 'all_product_yield_loaded'
DATA_KEY = 'all_product_yield_period_data'
ALL_CODES = '全部 Defect Code'


def _select(label, options, key, **kwargs):
    previous_options = st.session_state.get(key + '_options')
    if key in st.session_state:
        old = st.session_state[key]
        st.session_state[key] = options if previous_options is not None and set(old) == set(previous_options) else [v for v in old if v in options]
    st.session_state[key + '_options'] = options
    default = options if key not in st.session_state else None
    return st.multiselect(label, options, default=default, key=key, **kwargs)


def _normalize_codes():
    key = 'all_product_yield_codes'
    values = st.session_state[key]
    previous = st.session_state.get(key + '_previous', [])
    if ALL_CODES in values:
        st.session_state[key] = [v for v in values if v != ALL_CODES] if ALL_CODES in previous and len(values) > 1 else [ALL_CODES]


def _code_select(options):
    """Represent the full Code catalog with one chip instead of hundreds."""
    key = 'all_product_yield_codes'
    if key in st.session_state:
        values = st.session_state[key]
        if key + '_previous' not in st.session_state and values and set(values) == set(options):
            values = [ALL_CODES]
        st.session_state[key] = [v for v in values if v == ALL_CODES or v in options]
    selected = st.multiselect('Defect Code', [ALL_CODES, *options],
        default=[ALL_CODES] if key not in st.session_state else None, key=key,
        on_change=_normalize_codes, help='默认全部；选择具体 Code 可切换为明细筛选。')
    st.session_state[key + '_previous'] = list(selected)
    return options if ALL_CODES in selected else selected


@st.fragment
def render_all_product_yield_board() -> None:
    st.subheader('全产品良率看板')
    products = ConfigLoader.get_enabled_products()
    if not products:
        st.info('暂无可展示的产品。')
        return
    now = datetime.now()
    first, second = st.columns([5, 1], vertical_alignment='bottom')
    with first:
        selected = _select('产品型号', products, PRODUCTS_KEY, format_func=get_product_label_formatter())
    with second:
        query = st.button('查询', key='all_product_yield_query', type='primary')
    if query:
        payloads = {}
        with st.spinner('正在加载不良率趋势…'):
            for product in selected:
                try:
                    payloads[product] = load_product_period_trends(product)
                except Exception:
                    logger.exception('Unable to load period trends for %s', product)
                    payloads[product] = None
        st.session_state[DATA_KEY] = payloads
        st.session_state[LOADED_KEY] = True
        st.session_state['all_product_yield_period_year'] = now.year
    payloads = st.session_state.get(DATA_KEY, {})
    if not st.session_state.get(LOADED_KEY) or DATA_KEY not in st.session_state:
        st.caption('选择产品后查询年度、季度和月度不良率。')
        return
    if st.session_state.get('all_product_yield_period_year') != now.year:
        st.info('年度已切换，请重新查询。')
        return
    scoped = {p: payloads[p] for p in products if p in selected and p in payloads}
    if set(selected) - set(payloads):
        st.info('产品选择已变更，请查询以加载新增产品。')
    valid = [v['rates'] for v in scoped.values() if v is not None]
    catalog = pd.concat(valid, ignore_index=True) if valid else pd.DataFrame(columns=['defect_group', 'defect_code'])
    left, right = st.columns(2)
    with left:
        groups = _select('Defect Group', sorted(catalog.defect_group.dropna().unique()), 'all_product_yield_groups')
    with right:
        codes = _code_select(sorted(catalog.loc[catalog.defect_group.isin(groups), 'defect_code'].dropna().loc[lambda s: s.ne('')].unique()))
    periods = list(period_months(now.year, now.month))
    a, b, c = st.columns(3)
    with a:
        halves = _select('年度（上下半年）', [p for p in periods if '-H' in p], 'all_product_yield_halves')
    with b:
        quarters = _select('季度', [p for p in periods if '-Q' in p], 'all_product_yield_quarters')
    with c:
        months = _select('月度', [p for p in periods if '-H' not in p and '-Q' not in p], 'all_product_yield_months')
    chosen = [p for p in periods if p in halves + quarters + months]
    st.caption('总不良与堆叠图按所选 Group 汇总；Code 仅筛选明细。季度、半年按月过货数加权。')
    tables, charts = {}, {}
    for product, payload in scoped.items():
        if payload is not None:
            tables[product], charts[product] = project_report(payload['rates'], payload['volumes'], groups, codes, chosen)
    if tables:
        st.download_button('下载汇总表', export_period_tables(tables), file_name=f'不良率趋势_{now:%Y%m%d}.xlsx',
                           mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', key='all_product_yield_download')
    if not scoped:
        st.info('请选择产品后查询。')
        return
    tabs = st.tabs(list(scoped))
    for tab, (product, payload) in zip(tabs, scoped.items()):
        with tab:
            if payload is None:
                st.error('该产品不良率数据加载失败，请稍后重新查询。')
                continue
            table, chart = tables[product], charts[product]
            if not chosen or not groups:
                st.info('请选择时间周期和 Defect Group。')
                continue
            if chart.defect_rate.isna().any():
                st.warning('部分周期数据暂不可用，空白表示暂无数据。')
            st.plotly_chart(create_period_trend_chart(chart, chosen, product), width='stretch', key=f'all_product_yield_period_chart_{product}')
            configs = {p: st.column_config.NumberColumn(p, format='percent', width='small') for p in chosen}
            st.dataframe(table, column_config=configs, hide_index=True, width='stretch', key=f'all_product_yield_period_table_{product}')
