"""Automatic previous-week IJP alert center, independent of query controls."""

import logging

import pandas as pd
import streamlit as st

from app.charts.indicator_domain.ijp.period_chart import build_ijp_period_figure
from app.charts.indicator_domain.ijp_hole.chart import build_hole_figure
from app.utils.ijp_labels import format_ijp_code, get_ijp_code_descriptions
from src.indicator_domain.application.ijp.ports import IjpAlertSource, IjpAlertReport
from src.indicator_domain.core.ijp.alerts import previous_week_range
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.report_cutoff_config import load_report_cutoff_policy
from app.sections.shared.alert_results import read_alert_result

logger = logging.getLogger(__name__)
GROUP_COLUMNS = ['productcode', 'line', 'printer']


@st.cache_data(ttl=900, max_entries=32, show_spinner=False)
def load_ijp_alert_report(_service: IjpAlertSource, signature: tuple[object, ...]) -> IjpAlertReport:
    # Native containers/DataFrames only. Exceptions never become normal results.
    return _service.get_alert_report()


def render_ijp_alert_center(service: IjpAlertSource, region: str, *, produce: bool = True) -> None:
    start, end = service.get_reporting_window()
    previous_start, previous_end = previous_week_range(end.date())
    week = previous_start.isocalendar()
    title = f'超规预警中心（{region}，上一周 {week.year}-W{week.week:02d}）'
    payload = None
    update_failed = False
    if produce:
        try:
            signature = (
                'ijp-alert-v1', region, start, end, type(service).__module__, type(service).__qualname__,
                tuple(ConfigLoader.get_enabled_products()), tuple(ConfigLoader.get_work_order_types()),
                repr(ConfigLoader.load_domain_config('indicator_domain')),
                ConfigLoader.get_data_forward_policy().signature, load_report_cutoff_policy().signature,
                str(ConfigLoader.get_domain_resource_path('indicator_domain', 'ijp_alerts', 'ijp/ijp_alerts.xlsx')),
            )
            payload = load_ijp_alert_report(service, signature)
        except Exception:
            logger.exception('IJP automatic alert calculation or snapshot publication failed: %s', region)
            update_failed = True
    result = read_alert_result('ijp_hole_oos' if region == '孔区' else 'ijp_aa_oos', '', end.date())
    alerts = result['alerts']
    descriptions = get_ijp_code_descriptions(region)
    with st.expander(title, expanded=not alerts.empty or update_failed or result['state'] == 'error'):
        if update_failed:
            st.warning('IJP 预警数据暂时无法更新；以下状态以已保存结果为准。')
        if result['state'] == 'error':
            st.error(result['message'])
        elif not alerts.empty:
            st.error(f'检测到 {len(alerts)} 条 IJP 超规预警，请关注。')
            _render_alert_table(alerts, descriptions)
        elif result['state'] == 'no_data':
            st.info('上一周暂无 IJP 监控数据。')
        else:
            st.success('上一周未发现 IJP 超规数据。')
    if alerts.empty or payload is None:
        return
    try:
        _render_alert_charts(payload['ratios'], alerts, region)
    except Exception:
        logger.exception('IJP optional alert charts could not be rendered: %s', region)
        st.info('预警图像暂不可用。')


def _render_alert_table(alerts: pd.DataFrame, descriptions: dict[str, str]) -> None:
    display = alerts.copy()
    display['rs_code'] = display.rs_code.map(lambda code: format_ijp_code(code, descriptions))
    display['ratio'] = display.ratio.map(lambda value: f'{value:.2%}')
    display['minimum'] = display.minimum.map(lambda value: f'≥ {value:.0%}')
    st.dataframe(display.rename(columns={
        'region': '监控区域', 'productcode': '产品型号', 'line': '线体',
        'printer': 'Printer', 'rs_code': 'CODE', 'period_start': '开始日期',
        'period_end': '结束日期', 'ratio': '占比', 'minimum': '规格',
    }), hide_index=True, width='stretch')


def _render_alert_charts(frame: pd.DataFrame, alerts: pd.DataFrame, region: str) -> None:
    with st.expander('自动预警指标图像', expanded=True):
        keys = pd.MultiIndex.from_frame(alerts[GROUP_COLUMNS].drop_duplicates())
        matched = frame.loc[pd.MultiIndex.from_frame(frame[GROUP_COLUMNS]).isin(keys)]
        build = build_hole_figure if region == '孔区' else build_ijp_period_figure
        for group, rows in matched.groupby(GROUP_COLUMNS, sort=True):
            st.plotly_chart(build(rows, title=' / '.join(group)), width='stretch',
                            key=f'ijp_alert_{region}_{"_".join(group)}')
