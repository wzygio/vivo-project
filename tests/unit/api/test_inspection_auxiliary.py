"""Auxiliary inspection adapters preserve full scopes and publication proof."""
from dataclasses import replace
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from api.inspection import producers_auxiliary as module
from src.equipment_domain.core.parts_alerts import PARTS_PUBLIC_COLUMNS
from src.equipment_domain.infrastructure.parts_alert_workbook import PartsAlertWorkbook
from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS
from src.iqc_domain.infrastructure.eva_materials.alert_workbook import EvaporationAlertWorkbook


PRODUCTS = ('M626', 'M678')


def runtime(calls, failing=None):
    def produce(name, products, reference):
        calls.append(('produce', name, products, reference))
        if name == failing:
            raise RuntimeError('driver text containing private credentials')
        return 0
    def verify(name, products, reference, count, started):
        calls.append(('verify', name, products, reference))
    return module.AuxiliaryRuntime(
        ijp=produce,
        parts=lambda products, reference: produce('parts', products, reference),
        iqc=lambda products, reference: produce('iqc_evaporation', products, reference),
        verify=verify,
    )


def test_each_domain_runs_once_full_scope_and_verifies_even_zero_alerts():
    calls = []
    reference = date.today()
    outcomes = module.produce_auxiliary(PRODUCTS, reference, runtime=runtime(calls))
    assert [r['module'] for r in outcomes] == list(module.MODULES)
    assert all(r['status'] == 'success' and r['alert_count'] == 0 for r in outcomes)
    assert [c[0] for c in calls] == ['produce', 'verify'] * 4
    assert all(c[2] == PRODUCTS and c[3] == reference for c in calls)


@pytest.mark.parametrize('failing', module.MODULES)
def test_source_failure_isolated_and_credentials_never_copied(failing):
    calls = []
    outcomes = module.produce_auxiliary(PRODUCTS, date.today(), runtime=runtime(calls, failing))
    assert sum(r['status'] == 'failed' for r in outcomes) == 1
    assert all(r['status'] == ('failed' if r['module'] == failing else 'success') for r in outcomes)
    assert 'credentials' not in str(outcomes)


def test_own_safe_failure_code_kept_and_unverified_publish_fails():
    def fail(*args):
        raise ValueError('INSPECTION_PUBLICATION_NOT_CURRENT_RUN')
    outcomes = module.produce_auxiliary(PRODUCTS, date.today(),
        runtime=replace(runtime([]), verify=fail))
    assert all(r['status'] == 'failed' and r['code'] == 'INSPECTION_PUBLICATION_NOT_CURRENT_RUN' for r in outcomes)


@pytest.mark.parametrize('products', [(), ('M626', 'M626'), ('',)])
def test_invalid_product_scope_never_runs_sources(products):
    calls = []
    with pytest.raises(ValueError):
        module.produce_auxiliary(products, date.today(), runtime=runtime(calls))
    assert calls == []


def test_header_only_template_is_not_a_completed_publication(tmp_path, monkeypatch):
    path = tmp_path / 'parts.xlsx'
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(columns=PARTS_PUBLIC_COLUMNS).to_excel(writer, sheet_name='超规备件', index=False)
        pd.DataFrame(columns=['as_of_date', 'last_update', 'updated_at', 'alert_count', 'status']).to_excel(
            writer, sheet_name='__meta__', index=False)
    monkeypatch.setattr(module, '_source', lambda name: (path, '超规备件'))
    with pytest.raises(ValueError, match='MISSING_METADATA'):
        module._verify_saved('parts', PRODUCTS, date.today(), 0, datetime.now())


def test_real_empty_iqc_workbook_is_verified_with_correct_products_and_week(tmp_path, monkeypatch):
    path = tmp_path / 'iqc.xlsx'
    started = datetime.now()
    start, end = module._week(date.today())
    EvaporationAlertWorkbook(path).save(pd.DataFrame(columns=REPORT_COLUMNS),
                                       start=start, end=end, products=PRODUCTS)
    monkeypatch.setattr(module, '_source', lambda name: (path, '超规材料'))
    module._verify_saved('iqc_evaporation', PRODUCTS, date.today(), 0, started)
    with pytest.raises(ValueError, match='PRODUCT_SCOPE'):
        module._verify_saved('iqc_evaporation', ('M626',), date.today(), 0, started)
    with pytest.raises(ValueError, match='NOT_CURRENT_RUN'):
        module._verify_saved('iqc_evaporation', PRODUCTS, date.today(), 0, started + timedelta(minutes=1))


def test_parts_workbook_readback_checks_real_record_count(tmp_path, monkeypatch):
    path = tmp_path / 'parts.xlsx'
    started = datetime.now()
    PartsAlertWorkbook(path).save(pd.DataFrame(columns=PARTS_PUBLIC_COLUMNS),
                                  as_of_date=str(date.today()), last_update='')
    monkeypatch.setattr(module, '_source', lambda name: (path, '超规备件'))
    module._verify_saved('parts', PRODUCTS, date.today(), 0, started)
    with pytest.raises(ValueError, match='COUNT_MISMATCH'):
        module._verify_saved('parts', PRODUCTS, date.today(), 1, started)


def test_iqc_service_reads_exact_previous_week_all_enabled_and_publishes(monkeypatch):
    from src.iqc_domain import composition
    from src.iqc_domain.application.eva_materials import alerts
    calls = []
    class Service:
        def get_report(self, start, end, products):
            calls.append((start, end, products))
            return pd.DataFrame(columns=REPORT_COLUMNS)
    monkeypatch.setattr(composition, 'build_evaporation_service', lambda **kwargs: Service())
    monkeypatch.setattr(composition, 'build_evaporation_alert_store', lambda: object())
    monkeypatch.setattr(alerts, 'publish_evaporation_alerts', lambda frame, **kwargs: (
        calls.append(kwargs) or frame))
    assert module._produce_iqc(PRODUCTS, date(2026, 11, 12)) == 0
    assert calls[0] == (date(2026, 11, 2), date(2026, 11, 8), PRODUCTS)
    assert calls[1]['products'] == PRODUCTS


def test_refreshed_parts_port_does_not_read_old_source_snapshot(monkeypatch):
    from src.equipment_domain.infrastructure import data_loader
    from src.shared_kernel.report_cutoff import ReportCutoffPolicy
    empty = pd.DataFrame(columns=['step_id', 'sub_equip_id', 'param_name', 'value', 'glass_start_time'])
    monkeypatch.setattr(data_loader, 'load_fabricated_part_life_snapshot', lambda *a, **k: empty.copy())
    monkeypatch.setattr(data_loader, 'load_report_cutoff_policy', lambda: ReportCutoffPolicy())
    # No delegate load_snapshots exists: using it would expose an old disk generation.
    source, fabricated = module._RefreshedPartsPort(object(), empty).load_snapshots(
        pd.DataFrame(), as_of=pd.Timestamp.now(), max_age_days=3)
    assert source.empty and fabricated.empty


@pytest.mark.parametrize('name', ['ijp_aa', 'ijp_hole'])
def test_ijp_default_adapter_uses_requested_date_all_products_and_original_services(monkeypatch, name):
    from src.indicator_domain import composition
    from src.indicator_domain.application.ijp import service as aa
    from src.indicator_domain.application.ijp_hole import service as hole
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager
    calls = []
    monkeypatch.setattr(DatabaseManager, '__new__', staticmethod(lambda cls: SimpleNamespace(engine=object())))
    monkeypatch.setattr(ConfigLoader, 'get_work_order_types', lambda: ['P'])
    monkeypatch.setattr(ConfigLoader, 'load_domain_config', lambda *args: {})
    monkeypatch.setattr(composition, 'build_ijp_alert_store', lambda: 'store')
    monkeypatch.setattr(composition, 'build_ijp_repository', lambda manager: 'source')
    from src.indicator_domain.infrastructure.ijp_hole import repository
    monkeypatch.setattr(repository, 'IjpHoleRepository', lambda manager: 'source')
    def create(source, **kwargs):
        calls.append((source, kwargs))
        return SimpleNamespace(get_alert_report=lambda: {'alerts': pd.DataFrame()})
    monkeypatch.setattr(aa, 'IjpReportService', create)
    monkeypatch.setattr(hole, 'IjpHoleReportService', create)
    reference = date(2026, 11, 12)
    assert module._produce_ijp(name, PRODUCTS, reference) == 0
    source, kwargs = calls[0]
    assert source == 'source' and kwargs['alert_store'] == 'store'
    assert kwargs['today_provider']() == reference and kwargs['work_order_types'] == ('P',)
    assert kwargs.get('enabled_product_codes', kwargs.get('enabled_products')) == PRODUCTS


@pytest.mark.parametrize('recovered_probe', [False, True])
def test_parts_default_adapter_forces_source_and_does_not_cache_old_report(tmp_path, monkeypatch, recovered_probe):
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
    from src.equipment_domain import composition
    from src.equipment_domain.application import parts_service
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager
    source = pd.DataFrame({'glass_start_time': [pd.Timestamp.now()], 'value': [200]})
    calls = []
    baseline, cvd = tmp_path / 'baseline.csv', tmp_path / 'cvd.xlsx'
    baseline.touch()
    cvd.touch()
    engine = create_engine('sqlite://')
    def refresh(path):
        calls.append('refresh')
        if recovered_probe:
            try:
                with engine.connect() as connection:
                    connection.execute(text('SELECT missing_metadata_column'))
            except SQLAlchemyError:
                pass  # Simulate recovered metadata/pre-ping before successful SELECT.
        frame = source.copy()
        stamp = datetime.now().isoformat()
        frame.attrs['parts_source_read'] = {'status': 'success', 'started_at': stamp, 'completed_at': stamp}
        return frame
    port = SimpleNamespace(load_baseline=lambda path: pd.DataFrame({'spec': [1]}), refresh=refresh)
    monkeypatch.setattr(DatabaseManager, '__new__', staticmethod(lambda cls: SimpleNamespace(engine=engine)))
    monkeypatch.setattr(ConfigLoader, 'get_domain_resource_path', lambda domain, key: (
        baseline if key == 'critical_parts_baseline' else cvd))
    monkeypatch.setattr(composition, 'create_parts_data_port', lambda manager: port)
    monkeypatch.setattr(composition, 'create_parts_alert_store', lambda: PartsAlertWorkbook(tmp_path / 'alerts.xlsx'))
    monkeypatch.setattr(parts_service, 'build_parts_report_cache_context', lambda *args: {'as_of_date': 'old'})
    def report(manager, path, **kwargs):
        calls.append('report')
        assert kwargs['as_of_date'] == str(date.today())
        pd.testing.assert_frame_equal(kwargs['_data_port'].refreshed, source)
        return SimpleNamespace(report_df=pd.DataFrame({'使用进度': [105], '厂别': ['ARRAY']}), last_update='')
    monkeypatch.setattr(parts_service.PartsReportService, 'get_report_data', report)
    try:
        assert module._produce_parts(PRODUCTS, date.today()) == 1
        assert calls == ['refresh', 'report']
    finally:
        engine.dispose()


def test_parts_silently_swallowed_db_error_never_publishes(tmp_path, monkeypatch):
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
    from src.equipment_domain import composition
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager
    baseline, cvd = tmp_path / 'baseline.csv', tmp_path / 'cvd.xlsx'
    baseline.touch()
    cvd.touch()
    engine = create_engine('sqlite://')
    def swallowed(path):
        try:
            with engine.connect() as connection:
                connection.execute(text('SELECT missing_column'))
        except SQLAlchemyError:
            return pd.DataFrame()
    port = SimpleNamespace(load_baseline=lambda path: pd.DataFrame({'spec': [1]}), refresh=swallowed)
    monkeypatch.setattr(DatabaseManager, '__new__', staticmethod(lambda cls: SimpleNamespace(engine=engine)))
    monkeypatch.setattr(ConfigLoader, 'get_domain_resource_path', lambda domain, key: (
        baseline if key == 'critical_parts_baseline' else cvd))
    monkeypatch.setattr(composition, 'create_parts_data_port', lambda manager: port)
    try:
        with pytest.raises(RuntimeError, match='INSPECTION_PARTS_SOURCE_FAILED'):
            module._produce_parts(PRODUCTS, date.today())
    finally:
        engine.dispose()


def test_parts_select_never_uses_pandas_table_existence_probe(monkeypatch):
    from src.equipment_domain.infrastructure import data_loader
    calls = []
    monkeypatch.setattr(data_loader, 'get_equipment_runtime_config', lambda: SimpleNamespace(
        query_lookback_days=90, query_source_table='eda.array_pds_result_t'))
    monkeypatch.setattr(pd, 'read_sql', lambda *a, **k: pytest.fail('ambiguous SQL/table probe used'))
    def read_query(statement, engine, params):
        calls.append((str(statement), params))
        return pd.DataFrame(columns=['step_id', 'sub_equip_id', 'param_name', 'value', 'glass_start_time'])
    monkeypatch.setattr(pd, 'read_sql_query', read_query)
    spec = pd.DataFrame({'站点': ['00100'], '机台号-腔室': ['TOOL'], '参数名称': ['LIFE%']})
    assert data_loader._query_part_life_from_db(SimpleNamespace(engine=object()), spec).empty
    assert len(calls) == 1 and 'glass_start_time >=' in calls[0][0]
    assert calls[0][1]['pat0'] == 'LIFE%'


def test_parts_native_empty_success_has_fresh_completion_proof(monkeypatch):
    from src.equipment_domain.infrastructure import data_loader
    monkeypatch.setattr(data_loader, 'get_equipment_runtime_config', lambda: SimpleNamespace(
        query_lookback_days=90, query_source_table='eda.array_pds_result_t'))
    monkeypatch.setattr(pd, 'read_sql_query', lambda *a, **k: pd.DataFrame(
        columns=['step_id', 'sub_equip_id', 'param_name', 'value', 'glass_start_time']))
    spec = pd.DataFrame({'站点': ['00100'], '机台号-腔室': ['TOOL'], '参数名称': ['LIFE%']})
    started = datetime.now()
    result = data_loader.latest_part_measurements(data_loader._query_part_life_from_db(
        SimpleNamespace(engine=object()), spec))
    assert result.empty and result.attrs['parts_source_read']['status'] == 'success'
    assert pd.Timestamp(result.attrs['parts_source_read']['started_at']) >= pd.Timestamp(started)


def test_parts_failed_source_never_has_success_proof(monkeypatch):
    from src.equipment_domain.infrastructure import data_loader
    monkeypatch.setattr(data_loader, 'get_equipment_runtime_config', lambda: SimpleNamespace(
        query_lookback_days=90, query_source_table='eda.array_pds_result_t'))
    def fail(*args, **kwargs):
        raise OSError('private source failure')
    monkeypatch.setattr(pd, 'read_sql_query', fail)
    spec = pd.DataFrame({'站点': ['00100'], '机台号-腔室': ['TOOL'], '参数名称': ['LIFE%']})
    result = data_loader._query_part_life_from_db(SimpleNamespace(engine=object()), spec)
    assert result.empty and 'parts_source_read' not in result.attrs
