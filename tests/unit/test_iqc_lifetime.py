"""Public lifetime service behavior, independent of database and Streamlit."""

import pandas as pd
import pytest

from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
from src.iqc_domain.core.lifetime.lifetime import LifetimeDataInvalid, filter_report


def source_frame():
    records = []
    for screen in ('W', 'R', 'G', 'B'):
        for panel in ('private-b', 'private-a'):
            for time, decay in [('100', '0.98'), ('0', '1.0'), ('20', '0.99')]:
                records.append(dict(
                    product_group='M678', product_id='屏体', wo_name='量产',
                    date_key='2026/3/10', panel_id=panel, test_time=time, bu=screen,
                    luminance='123.15', lumi_decay='1.0084', cie_x='0.1376',
                    cie_xy='0.0472', iss='150.66', curr_decay='0.9972',
                    efficiency='8.027367', eff_decay=decay,
                ))
    return pd.DataFrame(records)


class MemorySource:
    def __init__(self, frame):
        self.frame = frame

    def read_report(self):
        return self.frame.copy()


def test_public_measurements_and_anonymous_numbers_are_stable():
    raw = source_frame()
    raw['ext_user'] = 'private-operator'
    raw.attrs['internal'] = 'private-metadata'
    result = LifetimeReportService(MemorySource(raw)).get_report()
    assert len(result) == 24
    assert len(result.columns) == 15
    assert result['样品编号'].tolist()[:6] == [1, 1, 1, 2, 2, 2]
    assert result['测试时间'].tolist()[:3] == [0, 20, 100]
    assert result['效率衰减'].tolist()[:3] == [1.0, 0.99, 0.98]
    assert result['亮度衰减'].iloc[0] == 1.0084
    assert 'private' not in result.to_json() and not result.attrs
    assert 'panel_id' not in result.columns
    shuffled = LifetimeReportService(MemorySource(raw.sample(frac=1, random_state=9))).get_report()
    pd.testing.assert_frame_equal(result, shuffled)
    assert 'panel_id' in raw.columns


def test_empty_missing_measurements_duplicates_and_more_than_five_samples():
    raw = source_frame()
    empty = LifetimeReportService(MemorySource(raw.iloc[:0])).get_report()
    assert empty.empty and len(empty.columns) == 15
    raw.loc[0, 'eff_decay'] = '/'
    raw.loc[1, 'iss'] = None
    raw.loc[2, 'luminance'] = 'inf'
    extra = raw.iloc[[0]].copy()
    extra['panel_id'] = 'private-z'
    for number in range(6):
        extra['panel_id'] = f'private-z{number}'
        raw = pd.concat([raw, extra], ignore_index=True)
    result = LifetimeReportService(MemorySource(pd.concat([raw, raw]))).get_report()
    assert len(result) == len(raw)
    assert result['样品编号'].max() == 8
    assert result['Iss'].isna().sum() == 1
    assert result['亮度'].isna().sum() == 1
    assert result['效率衰减'].isna().sum() == 7


@pytest.mark.parametrize('column,value', [('test_time', '-1'), ('test_time', 'bad'),
                                         ('panel_id', None), ('bu', '  ')])
def test_invalid_identity_or_time_is_not_a_successful_report(column, value):
    raw = source_frame()
    raw.loc[0, column] = value
    with pytest.raises(LifetimeDataInvalid):
        LifetimeReportService(MemorySource(raw)).get_report()


def test_conflicting_measurements_are_not_averaged():
    raw = source_frame()
    conflict = raw.iloc[[0]].copy()
    conflict['eff_decay'] = '0.5'
    with pytest.raises(LifetimeDataInvalid):
        LifetimeReportService(MemorySource(pd.concat([raw, conflict]))).get_report()


def test_group_numbers_do_not_change_on_filtering_or_cross_group_overlap():
    raw = source_frame()
    other = raw.iloc[[0]].copy()
    other['product_group'] = 'M999'
    other['date_key'] = 'other-batch'
    report = LifetimeReportService(MemorySource(pd.concat([raw, other]))).get_report()
    selected = filter_report(report, ['M999'], ['other-batch'])
    assert selected['样品编号'].tolist() == [1]
    assert filter_report(report, ['M678'], ['other-batch']).empty


@pytest.mark.parametrize('metric,source_column', [
    ('效率衰减', 'eff_decay'), ('亮度衰减', 'lumi_decay'),
])
def test_chart_keeps_numeric_time_missing_gaps_and_anonymous_hover(metric, source_column):
    from app.charts.iqc_domain.lifetime import build_lifetime_chart
    raw = source_frame()
    raw.loc[raw.test_time.eq('20'), source_column] = None
    report = LifetimeReportService(MemorySource(raw)).get_report()
    group = report[report['测试画面'].eq('W')]
    chart = build_lifetime_chart(group, metric=metric)
    assert len(chart.data) == 2
    assert chart.data[0].name == '1'
    assert list(chart.data[0].x) == [0, 20, 100]
    assert pd.isna(chart.data[0].y[1])
    assert chart.data[0].connectgaps is False
    assert 'private' not in chart.to_json() and 'panel_id' not in chart.to_json()
    assert chart.layout.xaxis.type == 'linear'
    assert chart.layout.xaxis.dtick == 100
    assert chart.layout.xaxis.tick0 == 0
    assert chart.layout.xaxis.tickformat == '.0f'
    assert chart.layout.yaxis.title.text == metric
    assert metric in chart.data[0].hovertemplate
    assert chart.data[0].y[0] == group.iloc[0][metric]
