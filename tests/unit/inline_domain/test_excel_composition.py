from src.inline_domain import composition
from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService
from src.inline_domain.infrastructure.monitor.capability_summary_store import CapabilitySummaryStore
from src.inline_domain.infrastructure.monitor.capability_snapshot_source import SnapshotCapabilitySource
from tests.unit.inline_domain.application.resource_fixtures import write_inline_resource_config


def test_warning_builders_never_construct_live_source(monkeypatch, tmp_path):
    def forbidden(*args, **kwargs):
        raise AssertionError("Warning board cannot construct a raw source")

    monkeypatch.setattr(composition, "build_live_monitor_source", forbidden)
    monkeypatch.setattr(composition, "DatabaseManager", forbidden)
    monkeypatch.setattr(composition.ConfigLoader, "get_project_root", lambda: tmp_path)
    oos = composition.build_oos_history_service(tmp_path)
    ooc = composition.build_ooc_history_service(tmp_path)
    assert isinstance(oos, ExcelAlarmReader)
    assert isinstance(ooc, ExcelAlarmReader)
    assert oos.read_product("spc", "M626").source == "missing"
    assert ooc.read_product("spc", "M626").source == "missing"
    cpk = composition.build_cpk_monitor_service(tmp_path)
    assert isinstance(cpk, CpkWorkbookMonitorService)
    cpm = composition.build_cpm_monitor_service(tmp_path)
    assert isinstance(cpm, CpkWorkbookMonitorService)
    assert isinstance(cpk._store, CapabilitySummaryStore)
    assert isinstance(cpm._store, CapabilitySummaryStore)
    assert cpk._store.metric == "cpk" and cpm._store.metric == "cpm"
    assert cpk._store.workbook_path != cpm._store.workbook_path
    assert isinstance(cpk._weekly_source, SnapshotCapabilitySource)
    assert isinstance(cpm._weekly_source, SnapshotCapabilitySource)
    assert cpk._weekly_source.snapshot_dir == tmp_path / "data/inline_domain/shared"
    assert cpm._latest_reader.sheet_suffix == "_cpm"


def test_raw_snapshot_builders_share_domain_module_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(composition.ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(composition, "InlineMeasurementSnapshotRepository", lambda **kwargs: kwargs)
    monkeypatch.setattr(composition, "AoiRsSnapshotRepository", lambda **kwargs: kwargs)
    assert composition.build_raw_measurement_repository(None, "M626")["snapshot_dir"] == tmp_path / "data/inline_domain/shared"
    assert composition.build_raw_measurement_repository(None, "M678")["snapshot_dir"] == tmp_path / "data/inline_domain/shared"
    assert composition.build_aoi_rs_repository(None, "M626")["snapshot_dir"] == tmp_path / "data/inline_domain/aoi_rs"


def test_warning_builders_use_independent_configured_summary_workbooks(monkeypatch, tmp_path):
    write_inline_resource_config(tmp_path)
    configured = tmp_path / "configured" / "alarm-summary.xlsx"
    capability_paths = {metric: tmp_path / "configured" / f"{metric}-summary.xlsx"
                        for metric in ("cpk", "cpm")}
    monkeypatch.setattr(
        composition, "monitor_summary_workbook_path", lambda resource_dir=None: configured
    )
    monkeypatch.setattr(composition, "capability_summary_workbook_path",
                        lambda metric, resource_dir=None: capability_paths[metric])
    monkeypatch.setattr(composition.ConfigLoader, "get_project_root", lambda: tmp_path)
    composition._build_monitor_summary_workbook_service.cache_clear()

    alarm = composition.build_monitor_summary_workbook_service()
    cpk = composition.build_cpk_monitor_service()
    cpm = composition.build_cpm_monitor_service()

    assert alarm._store.workbook_path == configured
    assert cpk._store.workbook_path == capability_paths["cpk"]
    assert cpm._store.workbook_path == capability_paths["cpm"]
    assert len({alarm._store.workbook_path, cpk._store.workbook_path, cpm._store.workbook_path}) == 3


def test_spc_and_monitor_read_the_same_configured_capability_file(monkeypatch, tmp_path):
    path = tmp_path / "external" / "custom.xlsx"
    monkeypatch.setattr(composition.ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(composition.ConfigLoader, "get_domain_resource_path", lambda *args: path)
    for metric in ["cpk", "cpm"]:
        reader = composition.build_capability_latest_reader(metric)
        monitor = (composition.build_cpk_monitor_service if metric == "cpk"
                   else composition.build_cpm_monitor_service)()
        assert reader.path == monitor._latest_reader.path == path
        assert reader.metric == monitor._latest_reader.metric == metric
