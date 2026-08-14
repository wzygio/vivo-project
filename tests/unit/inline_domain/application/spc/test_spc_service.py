import importlib
import sys
import threading
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.spc import spc_service
from src.inline_domain.application.spc import spc_data_decoration
from src.inline_domain.application.shared.decorated_features import fetch_decorated_features
from src.inline_domain.application.spc.spc_service import SpcReportService, resolve_period_capability_end_date
from src.inline_domain.application.spc.spc_service import assign_indicator_chart_type
from src.inline_domain.core.spc.spc_sheet_oos_decoration import (
    SheetOosDecorationReadError,
)
from src.inline_domain.application.spc.dtos import SpcQueryConfig


class FakeSpcRepository:
    seen_data_type_filters: list[str] = []

    def __init__(self, snapshot_dir: Path, use_snapshot: bool, db_manager: object) -> None:
        self.snapshot_dir = snapshot_dir
        self.use_snapshot = use_snapshot
        self.db_manager = db_manager

    def get_spc_measurements(self, config: SpcQueryConfig, force_refresh: bool = False) -> pd.DataFrame:
        self.seen_data_type_filters.append(config.data_type_filter or "")
        return pd.DataFrame(
            [
                {
                    "factory": "ARRAY",
                    "prod_code": config.prod_code,
                    "sheet_start_time": "2026-06-01",
                    "sheet_id": "LOT00000101",
                    "step_id": "S1",
                    "param_name": "THK",
                    "site_name": "P1",
                    "param_value": 49.0,
                    "data_type": "SPC",
                },
                {
                    "factory": "ARRAY",
                    "prod_code": config.prod_code,
                    "sheet_start_time": "2026-06-01",
                    "sheet_id": "LOT00000101",
                    "step_id": "S1",
                    "param_name": "THK",
                    "site_name": "P2",
                    "param_value": 51.0,
                    "data_type": "SPC",
                },
                {
                    "factory": "ARRAY",
                    "prod_code": config.prod_code,
                    "sheet_start_time": "2026-06-02",
                    "sheet_id": "LOT00000102",
                    "step_id": "S1",
                    "param_name": "THK",
                    "site_name": "P1",
                    "param_value": 50.0,
                    "data_type": "SPC",
                },
                {
                    "factory": "ARRAY",
                    "prod_code": config.prod_code,
                    "sheet_start_time": "2026-06-02",
                    "sheet_id": "LOT00000102",
                    "step_id": "S1",
                    "param_name": "THK",
                    "site_name": "P2",
                    "param_value": 52.0,
                    "data_type": "SPC",
                },
            ]
        ).assign(
            unit_id="MEASURE-EQP",
            main_step_id="M1",
            main_eqp_type="EQP",
            main_process_unit_id="MAIN-EQP-01",
            main_process_event_time=pd.Timestamp("2026-05-31 12:00:00"),
            main_process_trace_source="array_sht",
        )

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "prod_code": prod_code,
                    "step_id": "S1",
                    "param_name": "THK",
                    "usl": 55.0,
                    "lsl": 45.0,
                    "ucl": 54.0,
                    "lcl": 46.0,
                    "target": 50.0,
                }
            ]
        )


def test_spc_service_surfaces_decoration_read_failure_without_caching_it(
    monkeypatch,
) -> None:
    SpcReportService.fetch_spc_report_payload.clear()
    fetch_decorated_features.clear()
    calls = 0

    def fail_decoration(**_kwargs):
        nonlocal calls
        calls += 1
        raise SheetOosDecorationReadError("unreadable decoration file")

    # 段2 改造后服务走共享缓存管线；在共享函数注入点模拟修饰工作簿读取失败。
    monkeypatch.setattr(spc_service, "fetch_decorated_features", fail_decoration)
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-07",
        data_type_filter="SPC",
    )

    for _ in range(2):
        with pytest.raises(spc_service.SpcDecorationFileError):
            SpcReportService.get_spc_report_data(
                _data_port=FakeSpcRepository(Path("data"), True, object()),
                query_config_json=query.model_dump_json(),
                snapshot_signature="unreadable-decoration",
            )

    assert calls == 2


def test_assign_indicator_chart_type_marks_uni_parameters_for_line_charts() -> None:
    source_df = pd.DataFrame(
        [
            {"param_name": "SE_L1T_UNI"},
            {"param_name": "cd_uni"},
            {"param_name": "THK"},
            {"param_name": None},
        ]
    )

    result = assign_indicator_chart_type(source_df)

    assert result["chart_type"].tolist() == ["line", "line", "box", "box"]


def test_spc_service_requests_spc_only_and_returns_distribution_report(monkeypatch, tmp_path: Path) -> None:
    SpcReportService.fetch_spc_report_payload.clear()
    FakeSpcRepository.seen_data_type_filters = []
    monkeypatch.setattr(
        spc_service.ConfigLoader,
        "get_spc_period_sigma_source",
        staticmethod(lambda: "sheet_mean"),
    )
    monkeypatch.setattr(
        spc_data_decoration.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )

    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-07",
        data_type_filter="CTQ",
    )

    report = SpcReportService.get_spc_report_data(
        _data_port=FakeSpcRepository(Path("data"), True, object()),
        query_config_json=query.model_dump_json(),
        snapshot_signature="unit-test",
    )

    assert FakeSpcRepository.seen_data_type_filters == ["SPC"]
    assert not report.sheet_features_df.empty
    assert not report.period_capability_df.empty
    assert len(report.raw_measurements_df) == 4
    assert {"cpm", "cpk"}.issubset(report.period_capability_df.columns)
    assert set(report.period_capability_df["chart_type"]) == {"box"}
    assert set(report.sheet_features_df["chart_type"]) == {"box"}
    assert set(report.raw_measurements_df["chart_type"]) == {"box"}
    assert set(report.indicators_df["chart_type"]) == {"box"}
    assert set(report.period_capability_df["sigma_source"]) == {"sheet_mean"}
    assert set(report.period_capability_df["cpk_decorated"]) == {False}
    assert set(report.raw_measurements_df["data_type"]) == {"SPC"}
    assert set(report.raw_measurements_df["main_process_unit_id"]) == {"MAIN-EQP-01"}
    assert set(report.raw_measurements_df["main_process_trace_source"]) == {"array_sht"}
    assert report.sheet_oos_decoration_result is not None
    assert report.cpk_decoration_result is not None


def test_spc_service_can_switch_period_sigma_source_from_global_config(monkeypatch, tmp_path: Path) -> None:
    SpcReportService.fetch_spc_report_payload.clear()
    FakeSpcRepository.seen_data_type_filters = []
    monkeypatch.setattr(
        spc_service.ConfigLoader,
        "get_spc_period_sigma_source",
        staticmethod(lambda: "point_value"),
    )
    monkeypatch.setattr(
        spc_data_decoration.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )

    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-07",
        data_type_filter="SPC",
    )

    report = SpcReportService.get_spc_report_data(
        _data_port=FakeSpcRepository(Path("data"), True, object()),
        query_config_json=query.model_dump_json(),
        snapshot_signature="unit-test-point-sigma",
    )

    assert not report.period_capability_df.empty
    assert set(report.period_capability_df["sigma_source"]) == {"point_value"}
    assert report.period_capability_df["point_count"].dropna().max() >= 2


def test_spc_service_excludes_ppa_parameters_from_cpm_and_cpk_calculation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class PpaOnlySpcRepository(FakeSpcRepository):
        def get_spc_measurements(
            self,
            config: SpcQueryConfig,
            force_refresh: bool = False,
        ) -> pd.DataFrame:
            measurements_df = super().get_spc_measurements(config, force_refresh)
            return measurements_df.assign(param_name="PPA_THK")

        def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
            spec_df = super().get_spc_spec_limits(prod_code)
            return spec_df.assign(param_name="PPA_THK")

    SpcReportService.fetch_spc_report_payload.clear()
    monkeypatch.setattr(
        spc_service.ConfigLoader,
        "get_spc_period_sigma_source",
        staticmethod(lambda: "sheet_mean"),
    )
    monkeypatch.setattr(
        spc_data_decoration.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-07",
        data_type_filter="SPC",
    )

    report = SpcReportService.get_spc_report_data(
        _data_port=PpaOnlySpcRepository(Path("data"), True, object()),
        query_config_json=query.model_dump_json(),
        snapshot_signature="ppa-excluded-from-capability",
    )

    assert not report.raw_measurements_df.empty
    assert not report.sheet_features_df.empty
    assert set(report.indicators_df["param_name"]) == {"PPA_THK"}
    assert report.period_capability_df.empty
    assert report.cpk_decoration_result is not None


def test_period_capability_end_date_follows_latest_available_sheet_date() -> None:
    sheet_features = pd.DataFrame(
        [
            {"sheet_start_time": "2026-05-13", "sheet_id": "S1"},
            {"sheet_start_time": "2026-05-14", "sheet_id": "S2"},
        ]
    )

    assert resolve_period_capability_end_date(sheet_features, "2026-06-30") == date(2026, 5, 14)


def test_cpm_report_remains_available_when_service_module_reloads_during_cache_fill(
    monkeypatch,
    tmp_path: Path,
) -> None:
    entered_repository = threading.Event()
    continue_repository = threading.Event()

    class BlockingSpcRepository(FakeSpcRepository):
        def get_spc_measurements(
            self,
            config: SpcQueryConfig,
            force_refresh: bool = False,
        ) -> pd.DataFrame:
            entered_repository.set()
            assert continue_repository.wait(timeout=10)
            return super().get_spc_measurements(config, force_refresh)

    original_module = spc_service
    original_service = original_module.SpcReportService
    original_service.fetch_spc_report_payload.clear()
    monkeypatch.setattr(
        original_module.ConfigLoader,
        "get_spc_period_sigma_source",
        staticmethod(lambda: "sheet_mean"),
    )
    monkeypatch.setattr(
        spc_data_decoration.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-07",
        data_type_filter="SPC",
    )
    outcome: dict[str, object] = {}

    def load_report() -> None:
        try:
            outcome["report"] = original_service.get_spc_report_data(
                _data_port=BlockingSpcRepository(Path("data"), True, object()),
                query_config_json=query.model_dump_json(),
                snapshot_signature="reload-during-cache-fill",
            )
        except BaseException as exc:
            outcome["error"] = exc

    worker = threading.Thread(target=load_report)
    worker.start()
    assert entered_repository.wait(timeout=10)

    module_name = original_module.__name__
    try:
        del sys.modules[module_name]
        importlib.import_module(module_name)
        continue_repository.set()
        worker.join(timeout=15)
    finally:
        continue_repository.set()
        sys.modules[module_name] = original_module

    assert not worker.is_alive()
    assert "error" not in outcome, repr(outcome.get("error"))
    report = outcome["report"]
    assert isinstance(report, original_module.SpcReportViewModel)
    assert not report.raw_measurements_df.empty
