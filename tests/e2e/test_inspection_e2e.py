"""Fixture acquisition → full inspection receipt → real HTTP → real SMTP.

All domain result files are temporary. External manufacturing acquisition is
replaced with synthetic facts; domain publication/readers and socket protocols
remain real. This never queries the production database or starts Office.
"""
from dataclasses import replace
from datetime import datetime
import csv
from io import StringIO
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from api.inspection.application import InspectionJob, MODULES, required_keys, run_inspection
from api.inspection.scheduling import GateError, wait_for_inspection
from api.notification.application import run_notification
from api.notification.infrastructure import HttpAlertSource, SmtpDelivery
from api.notification.settings import MailCredentials, load_settings
from tests.e2e.test_anomaly_api_sheet_membership_e2e import _live_server
from tests.e2e.test_notification_e2e import smtp_server
from tests.integration.test_anomaly_api_capability import capability_files
from tests.integration.test_anomaly_api_files import FIXTURE_KEY, NOW, _release_yield, published_files
from tests.unit.api.test_notification import ROOT, TEST_INBOX


EVIDENCE = ROOT / 'output/test-results/automatic-inspection'


@pytest.fixture
def acquired_publications(capability_files, tmp_path, monkeypatch):
    """Keep acquired facts outside publication paths until their jobs execute."""
    files = capability_files
    from api.inspection import application as inspection_application
    monkeypatch.setattr(inspection_application, '_now', lambda: NOW.isoformat())
    _release_yield(files)
    stage = {}
    for key, path in files.paths.items():
        if key[0] != 'shared_kernel' and path.exists():
            assert path.resolve().is_relative_to(tmp_path.resolve())
            stage[key] = path.read_bytes()
            path.unlink()
    for path in files.membership_path.glob('*.json'):
        assert path.resolve().is_relative_to(tmp_path.resolve())
        path.unlink()
    for key in [('inline_domain', 'aoi_tt_trend_alerts'), ('inline_domain', 'aoi_rs_trend_alerts'),
                ('indicator_domain', 'ijp_alerts'), ('equipment_domain', 'parts_alerts'),
                ('iqc_domain', 'evaporation_alerts')]:
        files.paths[key] = tmp_path / (key[1] + '.xlsx')

    # Fail on any DB or Office fallback, even when a library catches its error.
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager
    import win32com.client
    attempts = []
    def forbidden(*args, **kwargs):
        attempts.append('external-source')
        raise RuntimeError('Fixture E2E forbids external acquisition')
    monkeypatch.setattr(DatabaseManager, '__new__', staticmethod(forbidden))
    monkeypatch.setattr(win32com.client, 'Dispatch', forbidden)
    monkeypatch.setattr(win32com.client, 'DispatchEx', forbidden)

    def restore(key):
        path = files.paths[key]
        assert path.resolve().is_relative_to(tmp_path.resolve())
        path.write_bytes(stage[key])

    def inline(scope):
        from src.inline_domain.composition import build_sheet_alert_query, build_capability_alert_query, build_trend_alert_results
        for event in ('oos', 'ooc'):
            restore(('inline_domain', f'{scope}_sheet_{event}_decoration'))
            result = build_sheet_alert_query(scope, event, strict_empty=True).read(scope, 'P1', NOW.date())
            assert result['state'] in ('alert', 'ok')
        if scope == 'spc':
            restore(('inline_domain', 'spc_cpk_cpm_decoration'))
            files.publish_membership(files.member_points)
            for metric in ('cpk', 'cpm'):
                assert build_capability_alert_query(metric).read('P1', NOW.date())['state'] == 'alert'
        else:
            if scope in ('aoi_tt', 'aoi_rs'):
                publisher = build_trend_alert_results(scope)
                publisher.publish('P1', NOW.date(), pd.DataFrame())
                assert publisher.read('P1', NOW.date())['state'] == 'no_data'
        return [{'module': scope, 'product': 'P1', 'status': 'success'}]

    def yield_results():
        restore(('yield_domain', 'lot_oos_results'))
        files.yield_service().publish('P1', 'trend', [], reference_date=NOW.date(),
            coverage_start='2026-10-01', coverage_end=str(NOW.date()), has_data=True,
            health={'status': 'fresh', 'refreshed_at': NOW.isoformat()}, calculation_signature='fixture-trend')
        assert all(files.yield_service().read('P1', kind, NOW.date()).status == 'ready' for kind in ('lot', 'trend'))
        return [{'module': 'yield', 'product': 'P1', 'status': 'success'}]

    def qtime():
        restore(('indicator_domain', 'qtime_alerts'))
        for shop in ('OLED', 'TP'):
            files.qtime_service().publish(shop, pd.DataFrame(), pd.DataFrame(),
                {'status': 'fresh', 'refreshed_at': NOW.isoformat()}, NOW.date())
        assert files.qtime_service().read('P1', NOW.date())['state'] == 'alert'
        return [{'module': 'qtime', 'status': 'success'}]

    def auxiliary(module):
        from src.indicator_domain.infrastructure.ijp import alert_workbook as ijp
        from src.equipment_domain.infrastructure import parts_alert_workbook as parts
        from src.iqc_domain.infrastructure.eva_materials import alert_workbook as iqc
        from app.sections.shared import published_alert_snapshots as snapshots

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return NOW.astimezone(tz) if tz is not None else NOW.replace(tzinfo=None)
        class Stamp:
            def __new__(cls, *args, **kwargs): return pd.Timestamp(*args, **kwargs)
            @staticmethod
            def now(): return pd.Timestamp(NOW.replace(tzinfo=None))
        def fixture_workbook(path):
            assert Path(path).resolve().is_relative_to(tmp_path.resolve())
            return pd.read_excel(path, sheet_name=None)
        for adapter in (ijp, parts, iqc):
            monkeypatch.setattr(adapter, 'datetime', Clock)
            monkeypatch.setattr(adapter, 'read_workbook_sheets', fixture_workbook)
        monkeypatch.setattr(snapshots, 'pd', SimpleNamespace(Timestamp=Stamp, DataFrame=pd.DataFrame, to_numeric=pd.to_numeric))
        monkeypatch.setattr(snapshots, 'read_snapshot_workbook', lambda path, *args: fixture_workbook(path))
        if module in ('ijp_aa', 'ijp_hole'):
            from src.indicator_domain.core.ijp.alerts import ALERT_COLUMNS
            region = 'AA区' if module == 'ijp_aa' else '孔区'
            ijp.IjpAlertWorkbook(files.paths[('indicator_domain', 'ijp_alerts')]).save(
                region, pd.DataFrame(columns=ALERT_COLUMNS), NOW.date())
            row_key = module + '_oos'
        elif module == 'parts':
            from src.equipment_domain.core.parts_alerts import PARTS_PUBLIC_COLUMNS
            parts.PartsAlertWorkbook(files.paths[('equipment_domain', 'parts_alerts')]).save(
                pd.DataFrame(columns=PARTS_PUBLIC_COLUMNS), as_of_date=str(NOW.date()), last_update=NOW.isoformat())
            row_key = 'parts_oos'
        else:
            from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS
            from datetime import timedelta
            monday = NOW.date() - timedelta(days=NOW.weekday())
            iqc.EvaporationAlertWorkbook(files.paths[('iqc_domain', 'evaporation_alerts')]).save(
                pd.DataFrame(columns=REPORT_COLUMNS), start=monday-timedelta(days=7),
                end=monday-timedelta(days=1), products=('P1',))
            row_key = 'iqc_evaporation_ng'
        assert snapshots.read_published_alerts(row_key, 'P1', NOW.date())['state'] == 'ok'
        return [{'module': module, 'status': 'success'}]

    jobs = [InspectionJob(scope, ((scope, 'P1'),), lambda scope=scope: inline(scope))
            for scope in ('spc', 'ctq', 'aoi_tt', 'aoi_rs')]
    jobs += [InspectionJob('yield', (('yield', 'P1'),), yield_results),
             InspectionJob('qtime', (('qtime', ''),), qtime)]
    jobs += [InspectionJob(scope, ((scope, ''),), lambda scope=scope: auxiliary(scope))
             for scope in ('ijp_aa', 'ijp_hole', 'parts', 'iqc_evaporation')]
    yield files, jobs
    assert attempts == []


def test_all_domain_publications_receipt_http_and_mail(acquired_publications, tmp_path, monkeypatch):
    files, jobs = acquired_publications
    report = run_inspection(('P1',), NOW.date(), jobs, tmp_path / 'inspection')
    assert report['status'] == 'success'
    assert set(report['modules']) == set(MODULES)
    assert {(row['module'], row.get('product', '')) for row in report['outcomes']} == required_keys(('P1',))
    receipt = tmp_path / 'inspection/latest.json'
    assert wait_for_inspection(receipt, ('P1',), NOW.date(), now=lambda: NOW)['status'] == 'success'

    evidence = EVIDENCE / 'fixture-flow'
    with smtp_server(evidence / 'smtp-listener.json') as (port, messages):
        with _live_server(files.app(), evidence / 'http-listener.json') as base:
            settings = replace(load_settings(ROOT / 'config/domain/api_domain.yaml'), api_base_url=base,
                               smtp_host='127.0.0.1', smtp_port=port, smtp_security='plain')
            result = run_notification(HttpAlertSource(settings, FIXTURE_KEY),
                SmtpDelivery(settings, MailCredentials('fixture', 'sender@example.com', ('fixture',))),
                settings, test_mode=True)
    assert result.complete and result.sent_count == 1 and result.record_count > 2
    (recipients, message), = messages
    assert recipients == [TEST_INBOX] and message['To'] == TEST_INBOX
    assert message['Cc'] is None and message['Bcc'] is None
    attachment, = message.iter_attachments()
    records = list(csv.DictReader(StringIO(attachment.get_payload(decode=True).decode('utf-8-sig'))))
    assert {row['分类'] for row in records} == {'SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time'}
    capability = [row for row in records if row['对象类型'] == 'capability']
    assert {row['异常事件'] for row in capability} == {'CPK', 'CPM'}
    assert all(json.loads(row['对象编号']) == ['00001', '00002'] for row in capability)


def test_missing_required_module_rejected_before_any_production(acquired_publications, tmp_path):
    _, jobs = acquired_publications
    incomplete = [job for job in jobs if job.name != 'parts']
    with pytest.raises(ValueError, match='every enabled product and module'):
        run_inspection(('P1',), NOW.date(), incomplete, tmp_path / 'inspection')
    assert not (tmp_path / 'inspection/latest.json').exists()


def test_partial_domain_failure_blocks_email_completion_gate(acquired_publications, tmp_path):
    _, jobs = acquired_publications
    replacement = InspectionJob('parts', (('parts', ''),), lambda: [])
    jobs = [replacement if job.name == 'parts' else job for job in jobs]
    report = run_inspection(('P1',), NOW.date(), jobs, tmp_path / 'inspection')
    assert report['status'] == 'partial'
    with pytest.raises(GateError):
        wait_for_inspection(tmp_path / 'inspection/latest.json', ('P1',), NOW.date(), now=lambda: NOW)
