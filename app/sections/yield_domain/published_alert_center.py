"""Yield alerts consume committed results independently of report-only data."""

from datetime import date

import pandas as pd
import streamlit as st

from app.sections.shared.alert_results import read_alert_result
from app.utils.admin_mode import is_admin_mode
from src.yield_domain.composition import build_yield_alert_results

_TREND_COLUMNS = {
    'level': '监控层级', 'defect_group': '缺陷组', 'defect_desc': '缺陷名称',
    'time_period': '周期', 'curr_rate': '当前不良率', 'prev_rate': '上一期不良率',
    'rules': '预警原因', 'period_scope': '周期类型',
}


@st.fragment
def _render_decisions(product: str, kind: str) -> None:
    if not is_admin_mode():
        return
    store = build_yield_alert_results().store
    with st.expander("预警修饰管理"):
        st.caption("False 放行当前预警，True 修饰，Delete 从公开结果中排除。请使用最新下载的台账，保留版本工作表。"
                   "上传只更新所列业务键。旧版表修改已有决定时请只提交一个业务键。")
        try:
            payload = store.decision_workbook(product, kind)
        except (OSError, ValueError, KeyError):
            st.error("决策表暂时无法读取。")
            return
        key = f"yield_{product}_{kind}"
        st.download_button("下载决策台账", payload, f"{product}_{kind}_预警决策.xlsx", key=key+"_download",
                           on_click="ignore")
        upload = st.file_uploader("上传决策台账", type=["xlsx"], key=key+"_upload")
        if upload is not None and st.button("保存决策", key=key+"_save"):
            try:
                store.update_decisions(product, kind, upload.getvalue())
            except ValueError as exc:
                st.error(f"决策未保存，请核对内容或重新下载最新台账。{exc}")
            except (OSError, KeyError):
                st.error("无法保存决策，请核对业务键和 flag，并确认工作簿可写。")
            else:
                st.rerun()


def render_saved_yield_alerts(product: str, reference_date: date) -> dict[str, list[dict]]:
    """Use the same published-result reader as matrix state and details."""
    output: dict[str, list[dict]] = {'lot_records': [], 'trend_records': []}
    for kind, row, title in (
        ('lot', 'yield_lot_oos', 'Lot 超规预警中心（周度，ISO周）'),
        ('trend', 'yield_trend_fluctuation', '趋势预警中心（周度，ISO周）'),
    ):
        result = read_alert_result(row, product, reference_date)
        records = result['records']
        with st.expander(title, expanded=result['state'] in {'alert', 'error'}):
            if is_admin_mode():
                _render_decisions(product, kind)
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
