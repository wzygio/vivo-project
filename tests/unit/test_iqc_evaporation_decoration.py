"""A shared report projection keeps chart/table values aligned, judgments intact."""

from datetime import date

import pandas as pd
import pytest

from app.charts.iqc_domain.evaporation import build_evaporation_figure
from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
from src.iqc_domain.core.eva_materials.evaporation import decorate_measurements
from tests.unit.test_iqc_evaporation import MemorySource, source_frame


@pytest.mark.parametrize('characteristic', ['DSC-Tg中点', 'HPLC-A', 'HPLC-A+B', 'HPLC-B'])
def test_first_points_are_clamped_to_the_inner_band_without_changing_facts(characteristic):
    source = pd.concat([source_frame()] * 7, ignore_index=True)
    source['cha_item'] = characteristic
    source['low_spec'], source['upp_spec'] = 10, 20
    source['iqc_1'] = [9, 10, 11, 15, 19, 20, 21]
    source['coa_1'] = [21, 20, 19, 15, 11, 10, None]
    source['iqc_2'] = 99
    before = source.copy(deep=True)
    decorated = decorate_measurements(source)
    assert decorated['iqc_1'].tolist() == [11, 11, 11, 15, 19, 19, 19]
    assert decorated['coa_1'].iloc[:6].tolist() == [19, 19, 19, 15, 11, 11]
    assert pd.isna(decorated['coa_1'].iloc[6])
    pd.testing.assert_frame_equal(
        decorated.drop(columns=['iqc_1', 'coa_1']), before.drop(columns=['iqc_1', 'coa_1']),
    )
    pd.testing.assert_frame_equal(source, before)
    pd.testing.assert_frame_equal(decorate_measurements(decorated), decorated)


@pytest.mark.parametrize('low,upper', [(None, 20), (10, None), (20, 10), (10, 10),
                                      (float('-inf'), 20), (10, float('inf'))])
def test_missing_or_invalid_span_is_not_invented(low, upper):
    source = source_frame().assign(low_spec=low, upp_spec=upper, iqc_1=25, coa_1=5)
    result = decorate_measurements(source)
    pd.testing.assert_frame_equal(result, source)


def test_unsupported_characteristics_and_missing_points_are_unchanged():
    source = source_frame().assign(cha_item='DSC-Tg起始点', iqc_1=100, coa_1=0)
    pd.testing.assert_frame_equal(decorate_measurements(source), source)
    empty = source_frame().assign(iqc_1=None, coa_1=None)
    pd.testing.assert_frame_equal(decorate_measurements(empty), empty)


def test_report_and_chart_share_decorated_values_but_keep_original_judgments():
    source = source_frame().assign(iqc_1=65, coa_1=61)
    before = source.copy(deep=True)
    report = EvaporationReportService(MemorySource(source), enabled_products=('M678',)).get_report(
        date(2026, 8, 1), date(2026, 8, 31),
    )
    assert report.loc[0, 'IQC-1'] == pytest.approx(63.8)
    assert report.loc[0, 'COA-1'] == pytest.approx(62.2)
    assert report.loc[0, ['IQC结果', 'COA结果']].tolist() == ['NG', 'NG']
    figure = build_evaporation_figure(report, characteristic='HPLC-A')
    assert figure.data[0].y == pytest.approx([63.8])
    assert figure.data[1].y == pytest.approx([62.2])
    pd.testing.assert_frame_equal(source, before)
