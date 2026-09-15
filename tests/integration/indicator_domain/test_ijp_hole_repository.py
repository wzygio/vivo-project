from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.errors import IjpDataAccessError
from src.indicator_domain.application.ijp_hole.service import IjpHoleReportService
from src.indicator_domain.infrastructure.ijp_hole.repository import IjpHoleRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy
from tests.e2e.fixtures.ijp_hole_data import build_hole_engine


@pytest.fixture
def repository(monkeypatch):
    monkeypatch.setattr(ConfigLoader, 'get_data_forward_policy',
                        staticmethod(lambda: DataForwardPolicy(enabled=False)))
    engine = build_hole_engine()
    yield IjpHoleRepository(SimpleNamespace(engine=engine))
    engine.dispose()


def query(**kwargs):
    return IjpQuery(start_time=datetime(2026, 8, 1), end_time=datetime(2026, 9, 10),
                    product_codes=('M626',), **kwargs)


def test_sql_excludes_small_holes_and_deduplicates_all_join_fanout(repository):
    frame = repository.fetch_counts(query(work_order_types=('MP',)))
    assert frame.code_num.sum() == 8
    assert set(frame.glass_id) == {'G1', 'G2'}
    assert frame.groupby('glass_id').in_window.first().to_dict() == {'G1': 1, 'G2': 0}
    assert repository.fetch_counts(query(picis=('LOT1',))).code_num.sum() == 4
    assert repository.fetch_counts(query(lines=('3CEE02',))).empty
    assert repository.fetch_counts(query(work_order_types=('NO',))).empty
    assert repository.fetch_counts(query(picis=("x' OR 1=1 --",))).empty
    assert repository.list_picis(query()) == ('LOT1', 'LOT2')


def test_service_keeps_denominator_and_disabled_selections_never_read_everything(repository):
    service = IjpHoleReportService(repository, ('M626',), today_provider=lambda: date(2026, 9, 10))
    result = service.get_report(query(codes=('C3RA2',)))
    assert result['total'].ratio.tolist() == [0.5]
    assert len(result['day']) == 2
    disabled = query().model_copy(update={'product_codes': ('DISABLED',)})
    assert all(frame.empty for frame in service.get_report(disabled).values())
    assert all(frame.empty for frame in IjpHoleReportService(repository, ()).get_report(query()).values())


def test_time_shift_and_latest_day_cutoff(repository, monkeypatch):
    from src.indicator_domain.infrastructure.ijp_hole import repository as module
    repository._time_policy = DataForwardPolicy(enabled=True, offset_days=4)
    monkeypatch.setattr(module, 'load_report_cutoff_policy', lambda: ReportCutoffPolicy())
    today = date.today()
    source = datetime.combine(today, datetime.min.time()) - timedelta(days=4)
    with repository._engine.begin() as c:
        c.execute(text('UPDATE eda.oled_chamber_hst_t SET cut_start_time=:t'),
                  {'t': source.isoformat(sep=' ')})
        c.execute(text('UPDATE eda.oled_ijp_defect_t SET update_time=:t'),
                  {'t': source.replace(hour=12).isoformat(sep=' ')})
    q = IjpQuery(start_time=datetime.combine(today, datetime.min.time()),
                 end_time=datetime.combine(today, datetime.max.time()), product_codes=('M626',))
    assert set(repository.fetch_counts(q).day) == {today.isoformat()}
    with repository._engine.begin() as c:
        c.execute(text('UPDATE eda.oled_ijp_defect_t SET update_time=:t'),
                  {'t': source.replace(hour=12, microsecond=1).isoformat(sep=' ')})
    assert repository.fetch_counts(q).empty


def test_failures_are_safe(repository):
    repository._engine = None
    with pytest.raises(IjpDataAccessError, match='暂时无法读取'):
        repository.fetch_counts(query())


def test_options_preserve_enabled_order_and_reject_border_codes(repository):
    service = IjpHoleReportService(repository, ('M678', 'M626'),
                                   today_provider=lambda: date(2026, 9, 10))
    assert service.get_filter_options()['product_codes'] == ('M678', 'M626')
    assert service.get_filter_options(('DISABLED',))['picis'] == ()
    with pytest.raises(ValueError, match='孔区 CODE'):
        service.get_report(query(codes=('C3DM1',)))
