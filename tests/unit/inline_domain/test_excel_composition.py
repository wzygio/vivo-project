from src.inline_domain import composition
from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService


def test_warning_builders_never_construct_live_source(monkeypatch, tmp_path):
    def forbidden(*args, **kwargs):
        raise AssertionError("Warning board cannot construct a raw source")

    monkeypatch.setattr(composition, "build_live_monitor_source", forbidden)
    monkeypatch.setattr(composition, "DatabaseManager", forbidden)
    oos = composition.build_oos_history_service(tmp_path)
    ooc = composition.build_ooc_history_service(tmp_path)
    assert isinstance(oos, ExcelAlarmReader)
    assert isinstance(ooc, ExcelAlarmReader)
    assert oos.read_product("spc", "M626").source == "missing"
    assert ooc.read_product("spc", "M626").source == "missing"
    assert isinstance(composition.build_cpk_monitor_service(tmp_path), CpkWorkbookMonitorService)


def test_raw_snapshot_builders_share_domain_module_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(composition.ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(composition, "InlineMeasurementSnapshotRepository", lambda **kwargs: kwargs)
    monkeypatch.setattr(composition, "AoiRsSnapshotRepository", lambda **kwargs: kwargs)
    assert composition.build_raw_measurement_repository(None, "M626")["snapshot_dir"] == tmp_path / "data/inline_domain/shared"
    assert composition.build_raw_measurement_repository(None, "M678")["snapshot_dir"] == tmp_path / "data/inline_domain/shared"
    assert composition.build_aoi_rs_repository(None, "M626")["snapshot_dir"] == tmp_path / "data/inline_domain/aoi_rs"
