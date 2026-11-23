from datetime import date

import pandas as pd
import pytest

from src.iqc_domain.application.lifetime.alerts import publish_lifetime_alerts
from src.iqc_domain.composition import load_lifetime_specs
from src.iqc_domain.core.lifetime.alerts import evaluate_lifetime_specs, lifetime_spec_alerts
from src.iqc_domain.infrastructure.lifetime.workbooks import LifetimeAlertWorkbook, read_lifetime_specs


def curves():
    return pd.DataFrame({
        '产品型号': 'M678', '测试画面': 'W', '批次号': 'B01', '样品编号': 1,
        '测试时间': [100, 200, 300, 400],
        '效率衰减': [1, 0.96, 0.98, 0.97], '亮度衰减': [0.95] * 4,
        'JNCD': [0.2] * 4,
    })


def test_extracted_hours_match_source_including_reused_panels():
    specs = load_lifetime_specs()
    rows = {
        'M678': [660, 535, 665, 420], 'Z571': [660, 535, 665, 420],
        'M626': [620, 370, 580, 610], 'Z559': [620, 370, 580, 610],
        'M673': [357, 240, 512, 344], 'Z517': [650, 410, 700, 595],
        'Z576': [464, 422, 544, 350],
    }
    assert specs == {(p, s): h for p, row in rows.items() for s, h in zip('WRGB', row)}


@pytest.mark.parametrize('hour,expected,method', [
    (200, 0.96, '实测'), (250, 0.97, '插值'), (660, 0.9518, '预测'),
])
def test_spec_value_uses_sample_curve_in_precedence_order(hour, expected, method):
    evaluations = evaluate_lifetime_specs(curves(), {('M678', 'W'): hour})
    assert evaluations['指标'].tolist() == ['效率衰减', '亮度衰减']
    assert evaluations['规格点数值'].tolist() == pytest.approx([expected, 0.95])
    assert evaluations['取值方式'].tolist() == [method, method]
    assert lifetime_spec_alerts(evaluations).empty


def test_alerts_keep_strictly_low_measured_values_even_after_forecast_anchor():
    report = pd.concat([curves(), curves().iloc[[0]].assign(测试时间=660, 效率衰减=0.94999)])
    specs = {('M678', 'W'): 660}
    alerts = lifetime_spec_alerts(evaluate_lifetime_specs(report, specs))
    assert alerts[['指标', '规格点数值', '取值方式']].values.tolist() == [['效率衰减', 0.94999, '实测']]
    assert evaluate_lifetime_specs(curves().iloc[:3], specs).empty
    assert evaluate_lifetime_specs(curves(), {('M678', 'R'): 400}).empty


def test_regression_at_exact_decimal_threshold_does_not_raise_roundoff_alert():
    report = curves().assign(效率衰减=[1.37, 1.30, 1.23, 1.16])
    evaluated = evaluate_lifetime_specs(report, {('M678', 'W'): 700})
    assert evaluated['规格点数值'].tolist() == [0.95, 0.95]
    assert lifetime_spec_alerts(evaluated).empty


def test_publish_scoped_lifetime_values_then_clear(tmp_path):
    source = curves().assign(效率衰减=0.94)
    report = pd.concat([source, source.assign(产品型号='M626')])
    specs = {('M678', 'W'): 660, ('M626', 'W'): 620}
    store = LifetimeAlertWorkbook(tmp_path / 'renamed.xlsx')
    params = dict(store=store, reference_date=date.today(), products=('M678',))
    alerts = publish_lifetime_alerts(report, specs, **params)
    assert alerts[['产品型号', '规格时间(H)', '规格点数值']].values.tolist() == [['M678', 660, 0.94]]
    assert pd.read_excel(store.path, sheet_name='__meta__').iloc[0].alert_count == 1
    publish_lifetime_alerts(report.iloc[:0], specs, **params)
    sheets = pd.read_excel(store.path, sheet_name=None)
    assert sheets['寿命预警'].empty and sheets['__meta__'].iloc[0].alert_count == 0


def test_malformed_maintained_spec_is_not_an_empty_publication(tmp_path):
    path = tmp_path / 'spec.xlsx'
    pd.DataFrame([{'产品型号': 'M678'}]).to_excel(path, sheet_name='800nit寿命规格', index=False)
    with pytest.raises(ValueError, match='LIFETIME_SPEC_SCHEMA_INVALID'):
        read_lifetime_specs(path)
