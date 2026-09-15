from datetime import date

import pandas as pd
import pytest

from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService


class MemorySource:
    def __init__(self, frame):
        self.frame = frame
        self.windows = []

    def read_report(self, start, end):
        self.windows.append((start, end))
        return self.frame.copy(deep=True)


def source_frame():
    return pd.DataFrame([{
        'prod_code': '通用', 'materialtype': '量产',
        'requestdate': pd.Timestamp('2026-08-20 17:03:07'),
        'checkeddate': pd.Timestamp('2026-08-27 12:56:26'),
        'mitemname': '100024720', 'mitemdesc': '有机材料_GP-RH1080',
        'check_user': '检验员', 'mater_type': '有机', 'factory': 'V3',
        'cha_item': 'HPLC-A', 'spec_req1': '>=', 'low_spec': 62.0,
        'spec_req2': '<=', 'upp_spec': 64.0,
        'iqc_1': 63.081, 'iqc_2': None, 'iqc_3': None,
        'iqc_4': None, 'iqc_5': None, 'coa_1': 62.0,
        'coa_2': None, 'coa_3': None, 'coa_4': None, 'coa_5': None,
        'iqc_result': 'NG', 'coa_result': None,
        'ticketno': 'INTERNAL', 'flag': True,
    }])


def test_report_preserves_result_blanks_and_pairs_specifications():
    source = MemorySource(source_frame())
    original = source.frame.copy(deep=True)
    report = EvaporationReportService(source).get_report(date(2026, 8, 1), date(2026, 8, 31))
    assert source.windows == [(date(2026, 8, 1), date(2026, 8, 31))]
    assert report.shape == (1, 27)
    assert report.loc[0, '规格样式1'] == '<='
    assert report.loc[0, '规格上限'] == 64
    assert report.loc[0, '规格样式2'] == '>='
    assert report.loc[0, '规格下限'] == 62
    assert report.loc[0, 'IQC结果'] == 'NG'
    assert pd.isna(report.loc[0, 'COA结果'])
    assert pd.isna(report.loc[0, 'IQC-2'])
    assert 'flag' not in report and 'ticketno' not in report
    pd.testing.assert_frame_equal(source.frame, original)


@pytest.mark.parametrize('start,end', [
    (date(2026, 9, 2), date(2026, 9, 1)), ('2026-09-01', date(2026, 9, 2)),
])
def test_invalid_dates_do_not_read_source(start, end):
    source = MemorySource(source_frame())
    with pytest.raises(ValueError, match='IQC_DATE_WINDOW_INVALID'):
        EvaporationReportService(source).get_report(start, end)
    assert not source.windows


def test_empty_report_has_full_schema_and_missing_columns_fail():
    empty = source_frame().iloc[:0]
    result = EvaporationReportService(MemorySource(empty)).get_report(date(2026, 8, 1), date(2026, 8, 31))
    assert result.shape == (0, 27)
    with pytest.raises(ValueError, match='IQC_REPORT_SCHEMA_INVALID'):
        EvaporationReportService(MemorySource(pd.DataFrame())).get_report(date(2026, 8, 1), date(2026, 8, 31))


def test_product_selection_excludes_common_and_other_products_without_mutation():
    source = pd.concat([
        source_frame(), source_frame().assign(prod_code='M626'),
        source_frame().assign(prod_code='M678'), source_frame().assign(prod_code=None),
    ], ignore_index=True)
    original = source.copy(deep=True)
    service = EvaporationReportService(MemorySource(source))
    report = service.get_report(date(2026, 8, 1), date(2026, 8, 31), product_code='M626')
    assert len(report) == 1
    assert report['产品型号'].tolist() == ['M626']
    assert '项目名' not in report
    assert service.get_report(date(2026, 8, 1), date(2026, 8, 31), product_code='Z571').empty
    pd.testing.assert_frame_equal(source, original)
