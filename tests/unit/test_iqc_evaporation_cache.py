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
        def get_report(self, start, end, product_code=None):
            calls.append((start, end))
            service = EvaporationReportService(MemorySource(source_frame()))
            importlib.reload(application)
            return service.get_report(start, end, product_code=product_code)

    monkeypatch.setattr(dashboard, 'build_evaporation_service', ReloadingService)
    dashboard.fetch_report_payload.clear()
    args = (date(2026, 8, 1), date(2026, 8, 31), 'forward-4', 'cutoff-day1')
    try:
        report = dashboard.fetch_report_payload(*args)
        pd.testing.assert_frame_equal(report, dashboard.fetch_report_payload(*args))
        assert len(calls) == 1
        report.loc[0, 'IQC结果'] = 'changed'
        assert dashboard.fetch_report_payload(*args).loc[0, 'IQC结果'] == 'OK'
        dashboard.fetch_report_payload(*args[:2], 'forward-0', 'cutoff-day1')
        dashboard.fetch_report_payload(*args[:3], 'cutoff-day2')
        assert len(calls) == 3
        assert dashboard.fetch_report_payload(*args, 'M626').empty
        assert len(calls) == 4
        dashboard.fetch_report_payload(*args, result_rule_version='next-rule')
        assert len(calls) == 5
    finally:
        dashboard.fetch_report_payload.clear()


def test_source_failure_is_not_cached(monkeypatch):
    from app.sections.iqc_domain.eva_materials import evaporation_dashboard as dashboard

    calls = []

    class RecoveringService:
        def get_report(self, start, end, product_code=None):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError('unavailable')
            return EvaporationReportService(MemorySource(source_frame())).get_report(start, end, product_code=product_code)

    monkeypatch.setattr(dashboard, 'build_evaporation_service', RecoveringService)
    dashboard.fetch_report_payload.clear()
    args = (date(2026, 8, 1), date(2026, 8, 31), 'forward', 'cutoff')
    try:
        with pytest.raises(RuntimeError, match='unavailable'):
            dashboard.fetch_report_payload(*args)
        assert len(dashboard.fetch_report_payload(*args)) == 1
        assert len(calls) == 2
    finally:
        dashboard.fetch_report_payload.clear()
