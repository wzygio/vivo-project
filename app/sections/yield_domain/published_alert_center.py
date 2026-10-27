"""Yield alerts consume committed results independently of report-only data."""

from datetime import date

import pandas as pd
import streamlit as st

from app.sections.shared.alert_results import read_alert_result

_TREND_COLUMNS = {
    'level': '监控层级', 'defect_group': '缺陷组', 'defect_desc': '缺陷名称',
    'time_period': '周期', 'curr_rate': '当前不良率', 'prev_rate': '上一期不良率',
    'rules': '预警原因', 'period_scope': '周期类型',
}


def render_saved_yield_alerts(product: str, reference_date: date) -> dict[str, list[dict]]:
    """Use the same published-result reader as matrix state and details."""
    output: dict[str, list[dict]] = {'lot_records': [], 'trend_records': []}
    for kind, row, title in (
        ('lot', 'yield_lot_oos', 'Lot 超规预警中心（最近过货日向前30天）'),
        ('trend', 'yield_trend_fluctuation', '趋势预警中心（月／周）'),
    ):
        selection = {'time_period': 30} if kind == 'lot' else {}
        result = read_alert_result(row, product, reference_date, **selection)
        records = result['records']
        with st.expander(title, expanded=result['state'] in {'alert', 'error'}):
            if result['state'] == 'error':
                st.error(result['message'])
            elif result['state'] == 'no_data':
                st.info('当前周期暂无可用的入库数据。')
            elif records.empty:
                st.success('当前周期未发现预警。')
            else:
                st.error(f'检测到 {len(records)} 条预警，请关注。')
                if kind == 'trend':
                    public = records.reindex(columns=list(_TREND_COLUMNS)).rename(columns=_TREND_COLUMNS)
                    st.dataframe(public, hide_index=True, width='stretch', column_config={
                        '当前不良率': st.column_config.NumberColumn(format='percent'),
                        '上一期不良率': st.column_config.NumberColumn(format='percent'),
                    })
                else:
                    st.dataframe(result['alerts'], hide_index=True, width='stretch')
        if result['state'] in {'ok', 'alert'}:
            output[f'{kind}_records'] = records.astype(object).where(pd.notna(records), None).to_dict('records')
    return output
