"""Database isolation, safe errors, and public cache behavior."""

from contextlib import nullcontext

import pandas as pd
import pytest

from src.iqc_domain.application.lifetime.lifetime import LifetimeSourceUnavailable
from src.iqc_domain.infrastructure.lifetime.lifetime_repository import LifetimeRepository


@pytest.mark.parametrize('product', [None, '', ' \t ', 'DISABLED'])
def test_composed_service_scopes_products_before_identity_validation(monkeypatch, product):
    from src.iqc_domain import composition
    from tests.unit.test_iqc_lifetime import source_frame

    raw = source_frame()
    outside = raw.iloc[[0]].assign(product_group=product, panel_id=None, test_time='bad')
    combined = pd.concat([raw, outside], ignore_index=True)
    monkeypatch.setattr(composition, 'get_m3_engine', lambda: object())
    monkeypatch.setattr(LifetimeRepository, 'read_report', lambda self: combined.copy())
    monkeypatch.setattr(composition.ConfigLoader, 'get_enabled_products',
                        lambda **kwargs: ['M678'])

    report = composition.build_lifetime_service().get_report()

    assert len(report) == len(raw)
    assert report['产品型号'].unique().tolist() == ['M678']
    assert 'private' not in report.to_json()


def test_read_uses_read_only_transaction_and_complete_columns(monkeypatch):
    commands = []

    class Connection:
        def begin(self):
            return nullcontext()

        def execute(self, statement):
            commands.append(str(statement))

    connection = Connection()

    class Engine:
        def connect(self):
            return nullcontext(connection)

    def read_query(query, conn):
        assert conn is connection
        commands.append(str(query))
        return pd.DataFrame({'test_time': ['100']})

    monkeypatch.setattr(pd, 'read_sql_query', read_query)
    assert LifetimeRepository(Engine()).read_report().test_time.tolist() == ['100']
    assert commands[0] == 'SET TRANSACTION READ ONLY'
    assert 'statement_timeout' in commands[1]
    assert 'm3dwd.dwd_panel_eff_dec_enter' in commands[2]
    selected_columns = commands[2].partition('FROM')[0].removeprefix('SELECT').split(',')
    assert 'jncd' in [column.strip() for column in selected_columns]
    assert 'LIMIT' not in commands[2] and 'ext_user' not in commands[2]


def test_database_failure_is_sanitized_and_retried(caplog):
    class Engine:
        def connect(self):
            raise RuntimeError('private-password-and-sql')

    repository = LifetimeRepository(Engine())
    for _ in range(2):
        with pytest.raises(LifetimeSourceUnavailable) as caught:
            repository.read_report()
        assert 'private' not in str(caught.value)
    assert 'private' not in caplog.text
    assert caplog.text.count('LIFETIME_SOURCE_UNAVAILABLE') == 2


def test_m3_engine_only_uses_m3_configuration(monkeypatch):
    from src.iqc_domain.infrastructure.lifetime import m3_database
    m3_database.get_m3_engine.cache_clear()
    monkeypatch.setattr(m3_database, 'load_dotenv', lambda *a, **k: None)
    for key, value in {'USER':'m3-user', 'PASSWORD':'test@:/pass', 'HOST':'m3-host',
                       'PORT':'5432', 'DATABASE':'m3-test'}.items():
        monkeypatch.setenv('M3_DB_' + key, value)
        monkeypatch.setenv('DB_' + key, 'primary-unchanged')
    calls = []

    def create(url, **kwargs):
        calls.append((url, kwargs))
        return object()

    monkeypatch.setattr(m3_database, 'create_engine', create)
    try:
        engine = m3_database.get_m3_engine()
        assert m3_database.get_m3_engine() is engine
        assert len(calls) == 1
        url, options = calls[0]
        assert url.username == 'm3-user' and url.database == 'm3-test'
        assert url.password == 'test@:/pass'
        assert options['hide_parameters'] and options['pool_pre_ping']
    finally:
        m3_database.get_m3_engine.cache_clear()


def test_cache_contains_only_public_frame_and_failure_does_not_poison_retry(monkeypatch):
    from app.sections.iqc_domain.lifetime import lifetime_dashboard as dashboard
    from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
    from tests.unit.test_iqc_lifetime import MemorySource, source_frame
    calls = []

    def build():
        calls.append(1)
        if len(calls) == 1:
            raise LifetimeSourceUnavailable('unavailable')
        return LifetimeReportService(MemorySource(source_frame()))

    monkeypatch.setattr(dashboard, 'build_lifetime_service', build)
    dashboard.fetch_report_payload.clear()
    try:
        with pytest.raises(LifetimeSourceUnavailable):
            dashboard.fetch_report_payload()
        result = dashboard.fetch_report_payload()
        assert 'panel_id' not in result.columns
        result.iloc[0, 0] = 'changed'
        assert dashboard.fetch_report_payload().iloc[0, 0] == 'M678'
        assert len(calls) == 2
        dashboard.fetch_report_payload.clear()
        assert dashboard.fetch_report_payload()['JNCD'].tolist()[:3] == [0.15, 0.75, 1.25]
        assert len(calls) == 3
    finally:
        dashboard.fetch_report_payload.clear()


def test_cache_fill_survives_service_module_reload(monkeypatch):
    import importlib
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    import src.iqc_domain.application.lifetime.lifetime as application
    from app.sections.iqc_domain.lifetime import lifetime_dashboard as dashboard
    from tests.unit.test_iqc_lifetime import MemorySource, source_frame

    started, reloaded = Event(), Event()
    calls = []

    class ReloadingService:
        def get_report(self):
            calls.append(1)
            service = application.LifetimeReportService(MemorySource(source_frame()))
            started.set()
            assert reloaded.wait(10), 'Module reload did not finish'
            return service.get_report()

    monkeypatch.setattr(dashboard, 'build_lifetime_service', ReloadingService)
    dashboard.fetch_report_payload.clear()
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            filling = executor.submit(dashboard.fetch_report_payload)
            try:
                assert started.wait(10), 'Cache fill did not start'
                importlib.reload(application)
            finally:
                reloaded.set()
            report = filling.result(timeout=10)
        pd.testing.assert_frame_equal(report, dashboard.fetch_report_payload())
        assert len(calls) == 1 and not report.attrs
    finally:
        dashboard.fetch_report_payload.clear()


def test_public_cache_changes_with_enabled_product_scope(monkeypatch):
    from app.sections.iqc_domain.lifetime import lifetime_dashboard as dashboard
    from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
    from tests.unit.test_iqc_lifetime import MemorySource, source_frame

    raw = source_frame()
    raw = pd.concat([raw, raw.assign(product_group='M626')], ignore_index=True)
    calls = []

    def build(*, enabled_products):
        calls.append(enabled_products)
        return LifetimeReportService(MemorySource(raw), enabled_products=enabled_products)

    monkeypatch.setattr(dashboard, 'build_lifetime_service', build)
    dashboard.fetch_report_payload.clear()
    try:
        first = dashboard.fetch_report_payload(('M678',))
        pd.testing.assert_frame_equal(first, dashboard.fetch_report_payload(('M678',)))
        assert calls == [('M678',)]
        second = dashboard.fetch_report_payload(('M678', 'M626'))
        assert set(second['产品型号']) == {'M678', 'M626'}
        assert len(calls) == 2
        assert dashboard.fetch_report_payload(()).empty
        assert len(calls) == 3
    finally:
        dashboard.fetch_report_payload.clear()
