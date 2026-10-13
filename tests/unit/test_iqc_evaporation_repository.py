from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.iqc_domain.application.eva_materials.evaporation import IqcSourceUnavailable
from src.iqc_domain.infrastructure.eva_materials.evaporation_repository import EvaporationRepository
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy
from tests.unit.test_iqc_evaporation import source_frame


def test_bound_source_window_shift_once_and_latest_day_cutoff(monkeypatch):
    source = pd.concat([source_frame()] * 4, ignore_index=True).assign(prod_code='通用')
    source['checkeddate'] = pd.to_datetime([
        '2026-08-26 23:59:59', '2026-08-27 12:00:00',
        '2026-08-27 12:00:00.000001', '2026-08-28 00:00:00',
    ], format='mixed')
    original = source.copy(deep=True)
    calls = []

    def read(sql, connection, params):
        calls.append((str(sql), params))
        if 'material_batches' in str(sql):
            return pd.DataFrame({'material_no': ['100024720'], 'prod_code': ['M678'], 'batch_no': ['20260801']})
        return source.copy(deep=True)

    monkeypatch.setattr(pd, 'read_sql_query', read)
    monkeypatch.setattr(ReportCutoffPolicy, 'boundary', lambda self, now=None: pd.Timestamp('2026-08-31 12:00:00'))
    repo = EvaporationRepository(SimpleNamespace(engine=MagicMock()), DataForwardPolicy(True, 4), ReportCutoffPolicy(), ('P',))
    result = repo.read_report(date(2026, 8, 31), date(2026, 8, 31), ('M678',))
    assert len(result) == 1
    assert result.iloc[0].prod_code == 'M678'
    assert result.iloc[0].batch_no == '20260801'
    assert result.iloc[0].checkeddate == pd.Timestamp('2026-08-31 12:00:00')
    assert result.iloc[0].requestdate == pd.Timestamp('2026-08-24 17:03:07')
    assert calls[0][1] == {'start_time': pd.Timestamp('2026-08-27'), 'end_time': pd.Timestamp('2026-08-28')}
    assert ':start_time' in calls[0][0] and '2026-08-27' not in calls[0][0]
    assert calls[1][1] == {'materials': ('100024720',), 'products': ('M678',), 'work_orders': ('P',)}
    pd.testing.assert_frame_equal(repo.read_report(date(2026, 8, 31), date(2026, 8, 31), ('M678',)), result)
    pd.testing.assert_frame_equal(source, original)


def test_historical_end_day_remains_complete(monkeypatch):
    source = source_frame().assign(checkeddate=pd.Timestamp('2026-08-27 23:59:59.999999'))
    monkeypatch.setattr(pd, 'read_sql_query', lambda sql, *args, **kwargs: (
        pd.DataFrame({'material_no': ['100024720'], 'prod_code': ['M678'], 'batch_no': ['20260801']})
        if 'material_batches' in str(sql) else source.copy()
    ))
    monkeypatch.setattr(ReportCutoffPolicy, 'boundary', lambda self, now=None: pd.Timestamp('2026-08-31 12:00:00'))
    repo = EvaporationRepository(SimpleNamespace(engine=MagicMock()), DataForwardPolicy(), ReportCutoffPolicy(), ('P',))
    assert len(repo.read_report(date(2026, 8, 27), date(2026, 8, 27), ('M678',))) == 1


@pytest.mark.parametrize('engine', [None, MagicMock()])
def test_failure_never_becomes_empty_success(monkeypatch, engine, caplog):
    def fail(*args, **kwargs):
        raise RuntimeError('credential-like-content')
    monkeypatch.setattr(pd, 'read_sql_query', fail)
    repo = EvaporationRepository(SimpleNamespace(engine=engine), DataForwardPolicy(), ReportCutoffPolicy(), ('P',))
    with pytest.raises(IqcSourceUnavailable, match='^IQC_SOURCE_UNAVAILABLE$'):
        repo.read_report(date(2026, 8, 1), date(2026, 8, 31), ('M678',))


@pytest.mark.parametrize('products,expected_products,expected_batches', [
    (('M678', 'M626'), 'M678、M626', '20260801、20260802'),
    (('M626',), 'M626', '20260802'),
])
def test_material_union_projects_selected_products_and_cycles_without_multiplying_facts(
    monkeypatch, products, expected_products, expected_batches,
):
    source = pd.concat([source_frame()] * 2 + [source_frame().assign(mitemname='UNMATCHED')], ignore_index=True).assign(prod_code='通用')
    original = source.copy(deep=True)
    membership = pd.DataFrame({
        'material_no': ['100024720'] * 4,
        'prod_code': ['M626', 'M678', 'M678', 'DISABLED'],
        'batch_no': ['20260802', '20260801', '20260801', '20260803'],
    })
    original_membership = membership.copy(deep=True)
    monkeypatch.setattr(pd, 'read_sql_query', lambda sql, *args, **kwargs: (
        membership if 'material_batches' in str(sql) else source.copy()
    ))
    repo = EvaporationRepository(SimpleNamespace(engine=MagicMock()), DataForwardPolicy(), ReportCutoffPolicy(), ('P',))
    report = repo.read_report(date(2026, 8, 1), date(2026, 8, 31), products)
    assert len(report) == 2
    assert report.prod_code.tolist() == [expected_products] * 2
    assert report.batch_no.tolist() == [expected_batches] * 2
    pd.testing.assert_frame_equal(source, original)
    pd.testing.assert_frame_equal(membership, original_membership)


@pytest.mark.parametrize('mode', ['empty_source', 'empty_membership', 'missing_material'])
def test_empty_report_keeps_product_and_cycle_schema(monkeypatch, mode):
    source = source_frame()
    if mode == 'empty_source':
        source = source.iloc[:0]
    elif mode == 'missing_material':
        source = source.assign(mitemname=None)
    monkeypatch.setattr(pd, 'read_sql_query', lambda sql, *args, **kwargs: (
        pd.DataFrame(columns=['material_no', 'prod_code', 'batch_no'])
        if 'material_batches' in str(sql) else source.copy()
    ))
    repo = EvaporationRepository(SimpleNamespace(engine=MagicMock()), DataForwardPolicy(), ReportCutoffPolicy(), ('P',))
    report = repo.read_report(date(2026, 8, 1), date(2026, 8, 31), ('M678',))
    assert report.empty
    assert {'prod_code', 'batch_no'} <= set(report.columns)


def test_bridge_failure_does_not_return_unfiltered_materials(monkeypatch, caplog):
    def read(sql, *args, **kwargs):
        if 'material_batches' in str(sql):
            raise PermissionError('private relationship source')
        return source_frame()
    monkeypatch.setattr(pd, 'read_sql_query', read)
    repo = EvaporationRepository(SimpleNamespace(engine=MagicMock()), DataForwardPolicy(), ReportCutoffPolicy(), ('P',))
    with pytest.raises(IqcSourceUnavailable, match='^IQC_SOURCE_UNAVAILABLE$'):
        repo.read_report(date(2026, 8, 1), date(2026, 8, 31), ('M678',))
    assert 'credential-like-content' not in caplog.text


@pytest.mark.parametrize('membership', [
    pd.DataFrame({'material_no': ['100024720'], 'prod_code': ['M678']}),
    pd.DataFrame({'material_no': ['100024720'], 'prod_code': ['M678'], 'batch_no': [None]}),
])
def test_missing_cycle_fails_instead_of_displaying_wms_product(monkeypatch, membership):
    monkeypatch.setattr(pd, 'read_sql_query', lambda sql, *args, **kwargs: (
        membership.copy() if 'material_batches' in str(sql) else source_frame().assign(prod_code='通用')
    ))
    repo = EvaporationRepository(SimpleNamespace(engine=MagicMock()), DataForwardPolicy(), ReportCutoffPolicy(), ('P',))
    with pytest.raises(IqcSourceUnavailable, match='^IQC_SOURCE_UNAVAILABLE$'):
        repo.read_report(date(2026, 8, 1), date(2026, 8, 31), ('M678',))
