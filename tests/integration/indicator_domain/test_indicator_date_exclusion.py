from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import text

from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.qtime.dtos import QTimeQuery
from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.indicator_domain.application.qtime.chamber_cache import cached_chamber_report, clear_chamber_cache
from src.indicator_domain.infrastructure.qtime.repository import QTimeRepository, DETAIL_COLUMNS
from src.indicator_domain.infrastructure.qtime.chamber_repository import ChamberQTimeRepository
from src.indicator_domain.infrastructure.ijp.repository import IjpRepository
from src.indicator_domain.infrastructure.ijp_hole.repository import IjpHoleRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy
from tests.integration.indicator_domain.test_ijp_repository_sql import _build_engine
from tests.integration.indicator_domain.test_chamber_qtime import report_source, write_source
from tests.e2e.fixtures.ijp_hole_data import build_hole_engine


@pytest.fixture(autouse=True)
def policy(monkeypatch):
    state = {'value': ('2026-10-01', '2026-10-06')}
    monkeypatch.setattr(ConfigLoader, 'get_indicator_data_exclusion', lambda: state['value'])
    monkeypatch.setattr(ConfigLoader, 'get_data_forward_policy',
                        lambda: DataForwardPolicy(enabled=True, offset_days=4))
    monkeypatch.setattr(ReportCutoffPolicy, 'boundary',
                        lambda self, now=None: pd.Timestamp('2026-10-13 12:00:00'))
    return state


@pytest.mark.parametrize('mode', ['direct', 'snapshot', 'fallback'])
def test_qtime_projects_filtered_display_facts_and_preserves_source(tmp_path, monkeypatch, policy, mode):
    times = ['20260926235959', '20260927000000', '20261002235959', '20261003000000']
    raw = pd.DataFrame([dict.fromkeys(DETAIL_COLUMNS) for _ in times]).assign(
        step_desc='A->B', lot_id=['BEFORE', 'START', 'END', 'AFTER'],
        timekey=times, shop='OLED', prodcode='M626', q_spec=10, wait_time=20,
    )
    def read_sql(*args, **kwargs):
        return raw.copy()
    monkeypatch.setattr(pd, 'read_sql', read_sql)
    repo = QTimeRepository(SimpleNamespace(engine=object()), snapshot_dir=tmp_path if mode != 'direct' else None)
    query = QTimeQuery(start_time=datetime(2026, 9, 1), end_time=datetime(2026, 10, 14),
                       shop='OLED', step_descriptions=('A->B',))
    result = repo.fetch_details(query)
    assert set(result.lot_id) == {'BEFORE', 'AFTER'}
    if mode != 'direct':
        path = repo._snapshot_store.source_path('OLED')
        before = path.read_bytes()
        if mode == 'fallback':
            from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
            monkeypatch.setattr(repo, '_read_frame', lambda *a, **k: (_ for _ in ()).throw(QTimeDataAccessError('fixture')))
            # Force the fresh-cache path to miss; leave the stale snapshot readable.
            original = repo._read_snapshot
            monkeypatch.setattr(repo, '_read_snapshot', lambda shop, *, fresh_only: None if fresh_only else original(shop, fresh_only=False))
        second = repo.fetch_details(query)
        assert set(second.lot_id) == {'BEFORE', 'AFTER'}
        assert path.read_bytes() == before
        assert set(pd.read_parquet(path).timekey) == set(times)
        if mode == 'fallback':
            assert second.attrs['data_health']['status'] == 'stale'
    first_signature = repo.cache_signature('OLED')
    policy['value'] = None
    assert repo.cache_signature('OLED') != first_signature
    assert set(repo.fetch_details(query).lot_id) == {'BEFORE', 'START', 'END', 'AFTER'}


def test_chamber_excludes_before_product_matching_and_invalidates_cache(report_source, policy):
    engine, paths = report_source
    for path in paths.values():
        write_source(path, [('G1', datetime(2026, 9, 26, 23, 59, 59), 1),
                            ('G1', datetime(2026, 9, 27), 2),
                            ('G1', datetime(2026, 10, 2, 23, 59, 59), 3),
                            ('G1', datetime(2026, 10, 3), 4)])
    before = {line: path.read_bytes() for line, path in paths.items()}
    repo = ChamberQTimeRepository(SimpleNamespace(engine=engine), paths)
    signature = repo.cache_signature()
    first = repo.read(as_of=date(2026, 10, 13))
    assert set(first['measurements']['PT->OC1']) == {1, 4}
    assert set(first['products'].prod_code) == {'M626'}
    clear_chamber_cache()
    service = ChamberQTimeService(repo, enabled_products=('M626',))
    assert set(cached_chamber_report(service, as_of=date(2026, 10, 13))['details'].duration_seconds) == {1, 4}
    policy['value'] = None
    assert repo.cache_signature() != signature
    assert set(repo.read(as_of=date(2026, 10, 13))['measurements']['PT->OC1']) == {1, 2, 3, 4}
    assert set(cached_chamber_report(service, as_of=date(2026, 10, 13))['details'].duration_seconds) == {1, 2, 3, 4}
    clear_chamber_cache()
    assert {line: path.read_bytes() for line, path in paths.items()} == before


def test_border_sql_excludes_before_counts_ratios_limit_and_options(policy):
    engine = _build_engine()
    try:
        with engine.begin() as c:
            c.execute(text('DELETE FROM eda.spot_eda_oled_view_dft_v'))
            c.execute(text('DELETE FROM eda.oled_chamber_hst_t'))
            c.execute(text('DELETE FROM eda.dwd_glass_oled_cycle_v3'))
            for when, code in [('2026-09-26 23:59:59.999999', 'C3DM1'),
                               ('2026-09-27 00:00:00', 'C3DM2'),
                               ('2026-10-02 23:59:59.999999', 'C3DM2'),
                               ('2026-10-03 00:00:00', 'C3DM1')]:
                c.execute(text("INSERT INTO eda.spot_eda_oled_view_dft_v VALUES (:t,'G1','SPEC1',:code,'test.jpg')"), {'t': when, 'code': code})
                c.execute(text("INSERT INTO eda.dwd_glass_oled_cycle_v3 VALUES ('G1',:p,'CYC','M626',:t)"), {'t': when, 'p': when})
            c.execute(text("INSERT INTO eda.oled_chamber_hst_t VALUES ('G1','2026-09-26 23:59:59','3CEE01-IK2-PR1','REQ1')"))
            c.execute(text("INSERT INTO eda.oled_chamber_hst_t VALUES ('G1','2026-10-03 00:00:00','3CEE01-IK2-PR1','REQ1')"))
        repo = IjpRepository(SimpleNamespace(engine=engine))
        query = IjpQuery(start_time=datetime(2026, 9, 30), end_time=datetime(2026, 10, 7, 23, 59, 59, 999999))
        ratios = repo.fetch_glass_ratios(query)
        # Cycle rows multiply source joins, but excluded codes must not enter the denominator.
        assert set(ratios.rs_code) == {'C3DM1'}
        assert ratios.ratio.tolist() == [1.0]
        daily = repo.fetch_daily_counts(query)
        assert set(daily.day) == {'2026-09-30', '2026-10-07'}
        assert set(repo.fetch_details(query).print_time.dt.strftime('%Y-%m-%d')) == {'2026-09-30', '2026-10-07'}
        assert len(repo.list_picis(query.start_time, query.end_time, ('M626',))) == 2
        limited = query.model_copy(update={'start_time': datetime(2026, 10, 1), 'detail_limit': 1})
        assert repo.fetch_details(limited).print_time.dt.strftime('%Y-%m-%d').tolist() == ['2026-10-07']
        signature = repo.cache_signature()
        policy['value'] = None
        assert repo.cache_signature() != signature
        assert 'C3DM2' in set(repo.fetch_glass_ratios(query).rs_code)
    finally:
        engine.dispose()


def test_hole_sql_filters_display_days_and_keeps_denominator(policy):
    engine = build_hole_engine()
    try:
        with engine.begin() as c:
            c.execute(text('DELETE FROM eda.oled_ijp_defect_t'))
            c.execute(text("UPDATE eda.oled_chamber_hst_t SET cut_start_time='2026-09-26 23:59:59'"))
            for when, code in [('2026-09-26 23:59:59.999999', 'C3RA1'),
                               ('2026-09-27 00:00:00', 'C3RA2'),
                               ('2026-10-02 23:59:59.999999', 'C3RA2'),
                               ('2026-10-03 00:00:00', 'C3RA1')]:
                c.execute(text("INSERT INTO eda.oled_ijp_defect_t VALUES ('OLED','21200','G1','xxxA/B/C/D/E/F/G/H/I','ABCDEFGHIJKLMNHT0',:code,:t)"), {'t': when, 'code': code})
        repo = IjpHoleRepository(SimpleNamespace(engine=engine))
        query = IjpQuery(start_time=datetime(2026, 9, 30), end_time=datetime(2026, 10, 7, 23, 59, 59, 999999), product_codes=('M626',))
        frame = repo.fetch_counts(query)
        assert set(frame.day) == {'2026-09-30', '2026-10-07'}
        assert frame.code_num.sum() == 2
        assert set(frame.rs_code) == {'C3RA1'}
        signature = repo.cache_signature()
        policy['value'] = None
        assert repo.cache_signature() != signature
        assert repo.fetch_counts(query).code_num.sum() == 4
    finally:
        engine.dispose()
