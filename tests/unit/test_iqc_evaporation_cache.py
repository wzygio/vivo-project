from datetime import date
import importlib

import pandas as pd
import pytest

from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
from tests.unit.test_iqc_evaporation import MemorySource, source_frame


def test_cache_native_payload_survives_module_reload_and_separates_policy(monkeypatch):
    from app.sections.iqc_domain.eva_materials import evaporation_dashboard as dashboard
    import src.iqc_domain.application.eva_materials.evaporation as application

    calls = []

    class ReloadingService:
        def __init__(self, **kwargs):
            pass

        def get_report(self, start, end, products):
            calls.append((start, end, products))
            service = EvaporationReportService(MemorySource(source_frame()), enabled_products=('M678', 'M626'))
            importlib.reload(application)
            return service.get_report(start, end, products)

    monkeypatch.setattr(dashboard, 'build_evaporation_service', ReloadingService)
    dashboard.fetch_report_payload.clear()
    args = (date(2026, 8, 1), date(2026, 8, 31), 'forward-4', 'cutoff-day1', ('M678',), ('M678', 'M626'), ('P',))
    try:
        report = dashboard.fetch_report_payload(*args)
        pd.testing.assert_frame_equal(report, dashboard.fetch_report_payload(*args))
        assert len(calls) == 1
        report.loc[0, 'IQC结果'] = 'changed'
        assert dashboard.fetch_report_payload(*args).loc[0, 'IQC结果'] == 'OK'
        dashboard.fetch_report_payload(*args[:2], 'forward-0', 'cutoff-day1', *args[4:])
        dashboard.fetch_report_payload(*args[:3], 'cutoff-day2', *args[4:])
        assert len(calls) == 3
        dashboard.fetch_report_payload(*args, result_rule_version='next-rule')
        assert len(calls) == 4
        dashboard.fetch_report_payload(*args[:4], ('M626',), *args[5:])
        dashboard.fetch_report_payload(*args[:5], ('M678',), args[6])
        dashboard.fetch_report_payload(*args[:6], ('ESLC',))
        assert len(calls) == 7
        dashboard.fetch_report_payload(*args, product_scope_rule_version='next-product-scope')
        assert len(calls) == 8
    finally:
        dashboard.fetch_report_payload.clear()


def test_source_failure_is_not_cached(monkeypatch):
    from app.sections.iqc_domain.eva_materials import evaporation_dashboard as dashboard

    calls = []

    class RecoveringService:
        def __init__(self, **kwargs):
            pass

        def get_report(self, start, end, products):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError('unavailable')
            return EvaporationReportService(MemorySource(source_frame()), enabled_products=('M678',)).get_report(start, end, products)

    monkeypatch.setattr(dashboard, 'build_evaporation_service', RecoveringService)
    dashboard.fetch_report_payload.clear()
    args = (date(2026, 8, 1), date(2026, 8, 31), 'forward', 'cutoff', ('M678',), ('M678',), ('P',))
    try:
        with pytest.raises(RuntimeError, match='unavailable'):
            dashboard.fetch_report_payload(*args)
        assert len(dashboard.fetch_report_payload(*args)) == 1
        assert len(calls) == 2
    finally:
        dashboard.fetch_report_payload.clear()
