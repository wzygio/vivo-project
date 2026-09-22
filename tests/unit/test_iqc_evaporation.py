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


def test_report_computes_results_preserves_measurement_blanks_and_pairs_specifications():
    source = MemorySource(source_frame())
    original = source.frame.copy(deep=True)
    report = EvaporationReportService(source).get_report(date(2026, 8, 1), date(2026, 8, 31))
    assert source.windows == [(date(2026, 8, 1), date(2026, 8, 31))]
    assert report.shape == (1, 27)
    assert report.loc[0, '规格样式1'] == '<='
    assert report.loc[0, '规格上限'] == 64
    assert report.loc[0, '规格样式2'] == '>='
    assert report.loc[0, '规格下限'] == 62
    assert report.loc[0, 'IQC结果'] == 'OK'
    assert report.loc[0, 'COA结果'] == 'OK'
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


@pytest.mark.parametrize('prefix', ['iqc', 'coa'])
@pytest.mark.parametrize('point', range(1, 6))
def test_any_failing_point_sets_only_its_overall_result(prefix, point):
    frame = source_frame().assign(iqc_result='OK', coa_result='OK')
    frame.loc[0, f'{prefix}_{point}'] = 65
    report = EvaporationReportService(MemorySource(frame)).get_report(date(2026, 8, 1), date(2026, 8, 31))
    assert report.loc[0, 'IQC结果'] == ('NG' if prefix == 'iqc' else 'OK')
    assert report.loc[0, 'COA结果'] == ('NG' if prefix == 'coa' else 'OK')


def test_missing_measurements_use_case_else_ok_without_filling_values():
    frame = source_frame()
    for prefix in ('iqc', 'coa'):
        for point in range(1, 6):
            frame[f'{prefix}_{point}'] = None
    report = EvaporationReportService(MemorySource(frame)).get_report(date(2026, 8, 1), date(2026, 8, 31))
    assert report[['IQC结果', 'COA结果']].iloc[0].tolist() == ['OK', 'OK']
    assert report[[f'{prefix}-{point}' for prefix in ('IQC', 'COA') for point in range(1, 6)]].isna().all(axis=None)


def test_measurement_decisions_match_original_sql_case_for_boundaries_and_nulls():
    import itertools
    from pathlib import Path
    import re
    import sqlite3
    from src.iqc_domain.core.eva_materials.evaporation import measurement_results

    reference = (Path(__file__).resolve().parents[2] / 'docs/project_files/iqc_domain/蒸镀材料体系报表-sql语句.txt').read_text(encoding='utf-8')
    cases = re.findall(r'CASE\s+-- 下限违规.*?END AS (?:IQC|COA)_[1-5]_RESULT', reference, flags=re.DOTALL)
    assert len(cases) == 10
    rows = []
    for side, operator, boundary, value in itertools.product(
        ['low', 'upp'], ['>=', '<=', '>', '<', '=', 'unknown', None], [62.0, None], [61.0, 62.0, 63.0, None],
    ):
        row = source_frame().iloc[0].to_dict()
        row.update(spec_req1=None, low_spec=None, spec_req2=None, upp_spec=None)
        row['spec_req1' if side == 'low' else 'spec_req2'] = operator
        row[f'{side}_spec'] = boundary
        for prefix in ('iqc', 'coa'):
            for point in range(1, 6):
                row[f'{prefix}_{point}'] = value
        rows.append(row)
    frame = pd.DataFrame(rows)
    columns = ['spec_req1', 'low_spec', 'spec_req2', 'upp_spec', *[f'{prefix}_{i}' for prefix in ('iqc', 'coa') for i in range(1, 6)]]
    with sqlite3.connect(':memory:') as connection:
        frame[columns].to_sql('measurements', connection, index=False)
        expected = pd.read_sql_query('SELECT ' + ', '.join(cases) + ' FROM measurements', connection)
    expected.columns = expected.columns.str.lower()
    pd.testing.assert_frame_equal(measurement_results(frame), expected)
