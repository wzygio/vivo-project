import json

import pytest
from streamlit.testing.v1 import AppTest

from app.sections.indicator_domain.ijp.alert_center import load_ijp_alert_report


@pytest.mark.parametrize('region', ['AA区', '孔区'])
@pytest.mark.parametrize('mode', ['alert', 'normal', 'empty', 'failure'])
def test_automatic_alerts_without_query_and_safe_public_output(region, mode):
    load_ijp_alert_report.clear()
    source = '''
from datetime import datetime, date
import pandas as pd
from app.sections.indicator_domain.ijp.alert_center import render_ijp_alert_center
from src.indicator_domain.core.ijp.alerts import build_weekly_alerts
class Service:
    def get_reporting_window(self):
        return datetime(2026, 9, 1), datetime(2026, 10, 26)
    def get_alert_report(self):
        if MODE == 'failure':
            raise PermissionError('SELECT secret_table; admin=true')
        code = 'C3DM1' if REGION == 'AA区' else 'C3RA1'
        column = 'display_ratio' if REGION == 'AA区' else 'ratio'
        frame = pd.DataFrame([dict(productcode='M1', line='L1', printer=printer,
            period='10/19–10/25（周）', period_start='2026-10-19', period_end='2026-10-25',
            rs_code=code, has_data=MODE != 'empty', **{column: ratio})
            for printer, ratio in [('P1', .8 if MODE == 'alert' else .9), ('P2', .95)]])
        return {'alerts': build_weekly_alerts(frame, REGION, date(2026, 10, 26)), 'ratios': frame}
render_ijp_alert_center(Service(), REGION)
'''
    app = AppTest.from_string(f'REGION = {region!r}\nMODE = {mode!r}\n' + source).run()
    assert not app.exception and not app.button
    assert '2026-W43' in app.expander[0].label
    if mode == 'alert':
        assert app.expander[0].proto.expanded
        assert len(app.dataframe[0].value) == 1
        assert app.dataframe[0].value['Printer'].tolist() == ['P1']
        charts = app.get('plotly_chart')
        assert len(charts) == 1
        spec = json.loads(charts[0].proto.spec)
        assert spec['layout']['title']['text'] == 'M1 / L1 / P1'
        assert all(word not in charts[0].proto.spec for word in ('raw_ratio', 'code_num', 'in_window'))
    elif mode == 'normal':
        assert app.success[0].value == '上一周未发现 IJP 超规数据。'
    elif mode == 'empty':
        assert app.info[0].value == '上一周暂无 IJP 监控数据。'
    else:
        assert app.error[0].value == 'IJP 预警数据暂时无法更新，请稍后重试。'
        assert not app.dataframe and not app.get('plotly_chart')
    load_ijp_alert_report.clear()


def test_configured_annotations_are_shared_by_selectors_and_charts(monkeypatch):
    from app.utils.ijp_labels import get_ijp_code_descriptions, format_ijp_code
    from app.charts.indicator_domain.ijp_hole.chart import build_hole_figure
    from src.shared_kernel.config import ConfigLoader
    import pandas as pd

    monkeypatch.setattr(ConfigLoader, 'load_domain_config', lambda domain: {
        'ijp_hole': {'code_descriptions': {'C3RA0': '自定义说明'}},
        'ijp': {'code_descriptions': {'C3DM1': '自定义AA'}},
    })
    descriptions = get_ijp_code_descriptions('孔区')
    assert format_ijp_code('C3RA0', descriptions) == 'C3RA0：自定义说明'
    assert format_ijp_code('C3RA1', descriptions) == 'C3RA1'
    assert get_ijp_code_descriptions('AA区')['C3DM1'] == '自定义AA'
    frame = pd.DataFrame([dict(period='W1', period_start='2026-10-19', period_end='2026-10-25',
        rs_code='C3RA0', ratio=0, has_data=True)])
    trace = build_hole_figure(frame, title='P1').data[0]
    assert trace.name == 'C3RA0：自定义说明'
    assert '自定义说明' in trace.hovertemplate
