import pandas as pd

from src.inline_domain.application.shared.decorated_features import fetch_decorated_features
from src.inline_domain.application.spc.spc_service import SpcReportService
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.monitor.live_source import LiveMonitorSource
from src.shared_kernel.config import ConfigLoader
from tests.unit.inline_domain.application.resource_fixtures import write_inline_resource_config


def test_point_fraction_and_window_change_both_report_caches(tmp_path, monkeypatch):
    write_inline_resource_config(tmp_path)
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    current = [(.5, "2026-10-05", "2026-10-11")]
    monkeypatch.setattr(ConfigLoader, "get_spc_point_decoration_policy", lambda: current[0])
    calls = []

    class Source:
        def get_spc_measurements(self, _query):
            calls.append(1)
            return pd.DataFrame([
                dict(prod_code="P", factory="ARRAY", step_id="1", param_name="CD", sheet_id="S",
                     site_name=site, param_value=value, sheet_start_time=pd.Timestamp("2026-10-05"), data_type="SPC")
                for site, value in [("P1", 3.), ("P2", 11.)]
            ])

        def get_spc_spec_limits(self, _product):
            return pd.DataFrame([dict(prod_code="P", step_id="1", param_name="CD", lsl=2., usl=10.)])

    SpcReportService.fetch_spc_report_payload.clear()
    fetch_decorated_features.clear()
    kwargs = dict(_data_port=Source(), query_config_json=SpcQueryConfig(
        prod_code="P", start_date="2026-10-01", end_date="2026-10-12",
    ).model_dump_json(), snapshot_signature="same-source")
    first = SpcReportService.get_spc_report_data(**kwargs)
    assert first.raw_measurements_df.param_value.between(4., 8.).all()
    current[0] = (1., "2026-10-05", "2026-10-11")
    second = SpcReportService.get_spc_report_data(**kwargs)
    assert second.raw_measurements_df.param_value.max() > 8.
    current[0] = (.5, "2026-10-06", "2026-10-11")
    third = SpcReportService.get_spc_report_data(**kwargs)
    assert third.raw_measurements_df.param_value.max() > 8.
    current[0] = (.5, "2026-10-01", "2026-10-04")
    SpcReportService.get_spc_report_data(**kwargs)
    assert len(calls) == 4


def test_monitor_signature_includes_point_policy(monkeypatch):
    current = [(.5, "2026-10-05", "2026-10-10")]
    monkeypatch.setattr(ConfigLoader, "get_spc_point_decoration_policy", lambda: current[0])
    source = LiveMonitorSource(port_factory=lambda *_a: None, signature_provider=lambda *_a: "same")
    first = source.source_signature(["P"], iter(["spc"]))
    current[0] = (.4, "2026-10-05", "2026-10-10")
    assert source.source_signature(["P"], ["spc"]) != first
