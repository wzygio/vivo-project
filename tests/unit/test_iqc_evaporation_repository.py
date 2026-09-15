from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.iqc_domain.application.evaporation import IqcSourceUnavailable
from src.iqc_domain.infrastructure.evaporation_repository import EvaporationRepository
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy
from tests.unit.test_iqc_evaporation import source_frame


def test_bound_source_window_shift_once_and_latest_day_cutoff(monkeypatch):
    source = pd.concat([source_frame()] * 4, ignore_index=True)
    source['checkeddate'] = pd.to_datetime([
        '2026-08-26 23:59:59', '2026-08-27 12:00:00',
        '2026-08-27 12:00:00.000001', '2026-08-28 00:00:00',
    ], format='mixed')
    original = source.copy(deep=True)
    calls = []

    def read(sql, connection, params):
        calls.append((str(sql), params))
        return source.copy(deep=True)

    monkeypatch.setattr(pd, 'read_sql_query', read)
    monkeypatch.setattr(ReportCutoffPolicy, 'boundary', lambda self, now=None: pd.Timestamp('2026-08-31 12:00:00'))
    repo = EvaporationRepository(SimpleNamespace(engine=MagicMock()), DataForwardPolicy(True, 4), ReportCutoffPolicy())
    result = repo.read_report(date(2026, 8, 31), date(2026, 8, 31))
    assert len(result) == 1
    assert result.iloc[0].checkeddate == pd.Timestamp('2026-08-31 12:00:00')
    assert result.iloc[0].requestdate == pd.Timestamp('2026-08-24 17:03:07')
    assert calls[0][1] == {'start_time': pd.Timestamp('2026-08-27'), 'end_time': pd.Timestamp('2026-08-28')}
    assert ':start_time' in calls[0][0] and '2026-08-27' not in calls[0][0]
    pd.testing.assert_frame_equal(repo.read_report(date(2026, 8, 31), date(2026, 8, 31)), result)
    pd.testing.assert_frame_equal(source, original)


def test_historical_end_day_remains_complete(monkeypatch):
    source = source_frame().assign(checkeddate=pd.Timestamp('2026-08-27 23:59:59.999999'))
    monkeypatch.setattr(pd, 'read_sql_query', lambda *args, **kwargs: source.copy())
    monkeypatch.setattr(ReportCutoffPolicy, 'boundary', lambda self, now=None: pd.Timestamp('2026-08-31 12:00:00'))
    repo = EvaporationRepository(SimpleNamespace(engine=MagicMock()), DataForwardPolicy(), ReportCutoffPolicy())
    assert len(repo.read_report(date(2026, 8, 27), date(2026, 8, 27))) == 1


@pytest.mark.parametrize('engine', [None, MagicMock()])
def test_failure_never_becomes_empty_success(monkeypatch, engine, caplog):
    def fail(*args, **kwargs):
        raise RuntimeError('credential-like-content')
    monkeypatch.setattr(pd, 'read_sql_query', fail)
    repo = EvaporationRepository(SimpleNamespace(engine=engine), DataForwardPolicy(), ReportCutoffPolicy())
    with pytest.raises(IqcSourceUnavailable, match='^IQC_SOURCE_UNAVAILABLE$'):
        repo.read_report(date(2026, 8, 1), date(2026, 8, 31))
    assert 'credential-like-content' not in caplog.text
