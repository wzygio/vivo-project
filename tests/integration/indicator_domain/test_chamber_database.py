from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.infrastructure.qtime.chamber_database import DatabaseChamberQTimeRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy


@pytest.fixture
def database(monkeypatch):
    monkeypatch.setattr(ConfigLoader, 'get_indicator_data_exclusion', lambda: None)
    engine = create_engine('sqlite://')
    with engine.begin() as connection:
        for schema in ('mdw',):
            connection.execute(text(f"ATTACH DATABASE ':memory:' AS {schema}"))
        connection.execute(text('''CREATE TABLE mdw.dwt_inout_sub_unit_gls (
            glass_id TEXT, eqp_id TEXT, unit_id TEXT, sub_unit_id TEXT,
            factory TEXT, prod_type TEXT, inout_type TEXT, date_timekey TEXT,
            event_timekey TEXT, oper_in_time TEXT, oper_out_time TEXT, prod_id TEXT)'''))
        connection.execute(text('CREATE TABLE mdw.dwr_mes_productspec (productspecname TEXT, productcode TEXT)'))
        connection.execute(text("INSERT INTO mdw.dwr_mes_productspec VALUES ('S1','M626'),('S2','DISABLED')"))
    try:
        yield engine
    finally:
        engine.dispose()


def event(engine, glass, chamber, when, kind='IN', eqp='3CEE01'):
    timestamp = pd.Timestamp(when)
    with engine.begin() as connection:
        connection.execute(text('''INSERT INTO mdw.dwt_inout_sub_unit_gls VALUES
            (:glass,:eqp,'UNIT',:chamber,'OLED','P',:kind,:day,:key,:when,:when,:spec)'''),
            {'glass': glass, 'eqp': eqp, 'chamber': eqp + '-' + chamber, 'kind': kind,
             'day': timestamp.strftime('%Y%m%d'), 'key': timestamp.strftime('%Y%m%d%H%M%S%f'),
             'when': timestamp.isoformat(sep=' '), 'spec': glass+'_SPEC'})


def product(engine, glass, spec='S1'):
    with engine.begin() as connection:
        connection.execute(text('INSERT INTO mdw.dwr_mes_productspec VALUES (:name,:code)'),
                           {'name': glass+'_SPEC', 'code': 'M626' if spec == 'S1' else 'DISABLED'})


def service(engine, forward=0):
    source = DatabaseChamberQTimeRepository(
        SimpleNamespace(engine=engine), target_seconds=150,
        data_policy=DataForwardPolicy(enabled=bool(forward), offset_days=forward),
        cutoff_policy=ReportCutoffPolicy(),
    )
    return ChamberQTimeService(source, enabled_products=('M626',))


def passage(engine, glass, start, eqp='3CEE01', seconds=120):
    when = pd.Timestamp(start)
    event(engine, glass, 'BL0-LL1', when, eqp=eqp)
    event(engine, glass, 'BL0-PT1', when + pd.Timedelta(seconds=5), eqp=eqp)
    event(engine, glass, 'BL0-PT1', when + pd.Timedelta(seconds=10), 'OUT', eqp)
    event(engine, glass, 'BL1-C11', when + pd.Timedelta(seconds=10 + seconds), eqp=eqp)


def test_out_to_next_event_seconds_duplicates_repasses_and_line_isolation(database):
    product(database, 'G')
    passage(database, 'G', '2026-09-01 23:59:00', seconds=120)
    event(database, 'G', 'BL0-PT1', '2026-09-01 23:59:10', 'OUT')  # exact duplicate
    passage(database, 'G', '2026-09-02 08:00:00', seconds=160)
    passage(database, 'G', '2026-09-02 08:00:00', eqp='3CEE02', seconds=30)
    report = service(database).load(as_of=date(2026, 9, 3), start_date=date(2026, 9, 1), end_date=date(2026, 9, 2))
    details = report['details']
    assert len(details) == 3
    assert details.duration_seconds.round(3).tolist() == [120, 160, 30]
    assert details.status.tolist() == ['正常', '超限', '正常']
    assert set(details.chamber) == {'PT->OC1'}
    assert set(details.line) == {'3CEE001', '3CEE002'}


def test_disabled_conflicting_unmapped_glasses_and_missing_pairs(database):
    for glass in ('OK', 'OFF', 'CONFLICT', 'UNKNOWN'):
        passage(database, glass, '2026-09-01 08:00:00')
    product(database, 'OK')
    product(database, 'OFF', 'S2')
    product(database, 'CONFLICT')
    product(database, 'CONFLICT', 'S2')
    product(database, 'INCOMPLETE')
    event(database, 'INCOMPLETE', 'BL0-LL1', '2026-09-01 08:00:00')
    event(database, 'INCOMPLETE', 'BL0-PT1', '2026-09-01 08:01:00', 'OUT')
    report = service(database).load(as_of=date(2026, 9, 3), start_date=date(2026, 9, 1), end_date=date(2026, 9, 1))
    assert report['details'].glass_id.tolist() == ['OK']
    assert report['unmatched_glasses'] == 1 and report['ambiguous_glasses'] == 1


def test_source_window_display_exclusion_cutoff_and_cache_identity(database, monkeypatch):
    for number, when in enumerate(('2026-09-26 23:59:00', '2026-09-27 00:00:00',
                                  '2026-09-28 12:00:00', '2026-09-28 12:00:01')):
        glass = f'G{number}'
        product(database, glass)
        passage(database, glass, when)
    svc = service(database, forward=4)
    monkeypatch.setattr(ConfigLoader, 'get_indicator_data_exclusion', lambda: ('2026-10-01', '2026-10-01'))
    report = svc.load(as_of=date(2026, 10, 2), start_date=date(2026, 9, 30), end_date=date(2026, 10, 2))
    assert report['details'].entry_time.tolist() == [pd.Timestamp('2026-09-30 23:59:00'), pd.Timestamp('2026-10-02 12:00:00')]
    signature = svc.cache_signature()
    monkeypatch.setattr(ConfigLoader, 'get_indicator_data_exclusion', lambda: None)
    assert svc.cache_signature() != signature


def test_empty_is_valid_but_database_failure_and_oversized_windows_are_not(database):
    assert service(database).load(as_of=date(2026, 9, 3))['details'].empty
    with pytest.raises(QTimeDataAccessError):
        service(None).load(as_of=date(2026, 9, 3))
    with pytest.raises(ValueError, match='31'):
        service(database).load(as_of=date(2026, 9, 3), start_date=date(2026, 7, 1), end_date=date(2026, 9, 1))


def test_all_eleven_paths_follow_out_to_next_event_and_ignore_unrelated_units(database):
    from src.indicator_domain.core.qtime.chamber import CHAMBERS
    product(database, 'ALL')
    event(database, 'ALL', 'BL0-LL1', '2026-09-01 00:00:00')
    units = ('BL0-PT1','BL1-C11','BL1-C21','BL3-C31','BL3-C41','BL5-C51',
             'BL5-C61','BL7-C71','BL7-C81','BLA-MC1','BLB-CC1','BLD-CD1')
    when = pd.Timestamp('2026-09-01 01:00:00')
    for index, unit in enumerate(units):
        event(database, 'ALL', unit, when)
        event(database, 'ALL', unit, when + pd.Timedelta(seconds=5), 'OUT')
        event(database, 'ALL', 'BL3-PS3', when + pd.Timedelta(seconds=6))
        when += pd.Timedelta(seconds=15 + index)
    report = service(database).load(as_of=date(2026, 9, 3))
    result = report['details'].set_index('chamber').reindex(CHAMBERS)
    assert result.duration_seconds.tolist() == pytest.approx(list(range(10, 21)), abs=0.001)
    assert len(result) == 11


def test_different_date_windows_do_not_reuse_cached_payload(database):
    from src.indicator_domain.application.qtime.chamber_cache import cached_chamber_report, clear_chamber_cache
    clear_chamber_cache()
    try:
        product(database, 'G')
        passage(database, 'G', '2026-09-01 08:00:00')
        passage(database, 'G', '2026-09-02 08:00:00', seconds=40)
        svc = service(database)
        first = cached_chamber_report(svc, as_of=date(2026, 9, 3), start_date=date(2026, 9, 1), end_date=date(2026, 9, 1))
        second = cached_chamber_report(svc, as_of=date(2026, 9, 3), start_date=date(2026, 9, 2), end_date=date(2026, 9, 2))
        assert first['details'].duration_seconds.iloc[0] == pytest.approx(120, abs=0.001)
        assert second['details'].duration_seconds.iloc[0] == pytest.approx(40, abs=0.001)
    finally:
        clear_chamber_cache()


def test_chamber_excludes_before_product_matching_and_invalidates_cache(database, monkeypatch):
    from src.indicator_domain.application.qtime.chamber_cache import cached_chamber_report, clear_chamber_cache

    for number, when in enumerate(('2026-09-26 23:59:59', '2026-09-27 00:00:00',
                                  '2026-10-02 23:59:59', '2026-10-03 00:00:00'), start=1):
        glass = f'G{number}'
        passage(database, glass, when, seconds=number)
        # Excluded passages have no product master: they must disappear before matching.
        if number in (1, 4):
            product(database, glass)
    source_query = text('''
        SELECT glass_id, eqp_id, unit_id, sub_unit_id, factory, prod_type,
               inout_type, date_timekey, event_timekey, oper_in_time, oper_out_time, prod_id
        FROM mdw.dwt_inout_sub_unit_gls
        ORDER BY glass_id, event_timekey, sub_unit_id, inout_type
        LIMIT 17
    ''')
    with database.connect() as connection:
        before = pd.read_sql(source_query, connection)
    assert len(before) == 16
    state = {'value': ('2026-10-01', '2026-10-06')}
    monkeypatch.setattr(ConfigLoader, 'get_indicator_data_exclusion', lambda: state['value'])
    svc = service(database, forward=4)
    signature = svc.cache_signature()
    dates = {'as_of': date(2026, 10, 13), 'start_date': date(2026, 9, 30),
             'end_date': date(2026, 10, 7)}
    clear_chamber_cache()
    try:
        first = cached_chamber_report(svc, **dates)
        assert first['details'].duration_seconds.tolist() == pytest.approx([1, 4], abs=0.001)
        assert first['unmatched_glasses'] == 0
        state['value'] = None
        assert svc.cache_signature() != signature
        second = cached_chamber_report(svc, **dates)
        assert second['unmatched_glasses'] == 2
        assert second['details'].duration_seconds.tolist() == pytest.approx([1, 4], abs=0.001)
        with database.connect() as connection:
            after = pd.read_sql(source_query, connection)
        pd.testing.assert_frame_equal(before, after)
    finally:
        clear_chamber_cache()


def test_result_limit_rejects_partial_results_and_missing_master_is_a_safe_failure(database, monkeypatch):
    from src.indicator_domain.infrastructure.qtime import chamber_database
    product(database, 'G')
    passage(database, 'G', '2026-09-01 08:00:00')
    passage(database, 'G', '2026-09-02 08:00:00')
    monkeypatch.setattr(chamber_database, 'MAX_INTERVALS', 1)
    with pytest.raises(QTimeDataAccessError):
        service(database).load(as_of=date(2026, 9, 3))
    monkeypatch.setattr(chamber_database, 'MAX_INTERVALS', 10)
    with database.begin() as connection:
        connection.execute(text('DROP TABLE mdw.dwr_mes_productspec'))
    with pytest.raises(QTimeDataAccessError, match='数据读取失败'):
        service(database).load(as_of=date(2026, 9, 3))
