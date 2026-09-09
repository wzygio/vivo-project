from types import SimpleNamespace

import pandas as pd
import pytest

from src.shared_kernel.data_health import attach_data_health, make_data_health
from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig
from src.yield_domain.application.yield_service import YieldAnalysisService, YieldDataLoadError
from src.equipment_domain.application.parts_service import PartsReportService


def test_yield_accepts_native_empty_result_from_in_memory_port():
    query = YieldQueryConfig(start_date="2026-01-01", end_date="2026-01-02", product_code="FAKE")
    health = make_data_health("fresh", source_start=query.start_date, source_end=query.end_date)
    port = SimpleNamespace(read_panel=lambda *args, **kwargs: attach_data_health(pd.DataFrame(), health))
    YieldAnalysisService.get_raw_panel_details.clear()
    result = YieldAnalysisService.get_raw_panel_details(query.model_dump_json(), YieldDataPolicy().model_dump_json(), snapshot_signature="fake-port", _data_port=port)
    assert result.empty
    assert result.attrs["data_health"] == health


def test_equipment_refresh_uses_only_in_memory_port():
    calls = []
    port = SimpleNamespace(refresh=lambda path: calls.append(path) or pd.DataFrame({"value": [1]}))
    assert PartsReportService.safe_refresh_snapshots(None, "nonexistent.csv", _data_port=port)
    assert calls == ["nonexistent.csv"]


def test_equipment_payload_reads_fake_port_without_files_or_database():
    spec = pd.DataFrame([{"厂别": "Array", "备件类型": "Target", "设备类型": "PVD", "膜层": "MO", "制程": "Mo DEPO", "寿命规格": 41000.0, "站点": "1K200", "机台号-腔室": "3AFS01-SPU-PM5", "参数名称": "%TRGTLIFE%_G_MAX"}])
    facts = pd.DataFrame({"step_id": ["1K200"], "sub_equip_id": ["3AFS01-SPU-PM5"], "param_name": ["CH_A_TRGTLIFE_X_G_MAX"], "value": [32000.0], "glass_start_time": pd.to_datetime(["2026-05-12 08:00:00"])})
    port = SimpleNamespace(load_baseline=lambda path: spec, load_snapshots=lambda *args, **kwargs: (facts, pd.DataFrame()))
    PartsReportService.fetch_report_payload.clear()
    result = PartsReportService.fetch_report_payload(None, "nonexistent.csv", "fake-port", _data_port=port)
    assert result["total_count"] == 1
    assert result["report_df"]["测量值"].iloc[0] == 32000.0
    replacement = SimpleNamespace(load_baseline=lambda path: spec, load_snapshots=lambda *args, **kwargs: (facts.assign(value=1000.0), pd.DataFrame()))
    # Same business key, a different explicit store: do not reuse the first store.
    other = PartsReportService.fetch_report_payload(None, "nonexistent.csv", "fake-port", _data_port=replacement)
    assert other["report_df"]["测量值"].iloc[0] == 1000.0


def test_yield_changed_port_does_not_hide_unavailable_source():
    query = YieldQueryConfig(start_date="2026-01-01", end_date="2026-01-02", product_code="FAKE")
    def port(status):
        return SimpleNamespace(read_panel=lambda *args, **kwargs: attach_data_health(pd.DataFrame(), make_data_health(status)))
    YieldAnalysisService.get_raw_panel_details.clear()
    args = (query.model_dump_json(), YieldDataPolicy().model_dump_json())
    YieldAnalysisService.get_raw_panel_details(*args, snapshot_signature="same-key", _data_port=port("fresh"))
    with pytest.raises(YieldDataLoadError):
        YieldAnalysisService.get_raw_panel_details(*args, snapshot_signature="same-key", _data_port=port("unavailable"))


def test_default_yield_port_keeps_cache_and_page_refresh_discovery(monkeypatch):
    from inspect import unwrap
    from app.components.page_header import extract_cached_funcs
    from src.shared_kernel.config import ConfigLoader
    from src.yield_domain.application import yield_service

    calls = []
    def read(*args, **kwargs):
        calls.append(True)
        return attach_data_health(pd.DataFrame(), make_data_health("fresh"))
    monkeypatch.setattr(yield_service, "_resolve_data_port", lambda *args: SimpleNamespace(read_panel=read))
    query = YieldQueryConfig(start_date="2026-01-01", end_date="2026-01-02", product_code="DEFAULT")
    cached = YieldAnalysisService.get_raw_panel_details
    assert cached._info.ttl == ConfigLoader.get_cache_ttl_seconds()
    assert unwrap(cached).__name__ == "get_raw_panel_details"
    args = (query.model_dump_json(), YieldDataPolicy().model_dump_json())
    cached.clear()
    cached(*args, snapshot_signature="default-production-path")
    cached(*args, snapshot_signature="default-production-path")
    assert len(calls) == 1
    assert cached in extract_cached_funcs(YieldAnalysisService)
    cached.clear()
    cached(*args, snapshot_signature="default-production-path")
    assert len(calls) == 2
