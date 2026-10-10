import importlib
import sys
import threading
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.spc import spc_service
from src.inline_domain.application.shared import decorated_data
from src.inline_domain.application.shared.decorated_features import fetch_decorated_features
from src.inline_domain.application.spc.spc_service import (
    SpcReportService,
    exclude_cpm_cpk_parameters,
    resolve_period_capability_end_date,
)
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    SheetOosDecorationReadError,
)
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from tests.unit.inline_domain.application.resource_fixtures import write_inline_resource_config


@pytest.fixture(autouse=True)
def isolate_workbook_paths(monkeypatch, tmp_path):
    """Report-service fixtures must never read or write maintained workbooks."""
    write_inline_resource_config(tmp_path)
    original = spc_service.ConfigLoader.get_domain_resource_path
    monkeypatch.setattr(
        spc_service.ConfigLoader, "get_domain_resource_path",
        classmethod(lambda cls, domain, key, default_name=None: (
            tmp_path / original(domain, key, default_name).name
            if domain == "inline_domain" else original(domain, key, default_name)
        )),
    )


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


def test_capability_parameter_exclusions_use_only_configured_tokens() -> None:
    dataframe = pd.DataFrame({"param_name": ["PPA_THK", "PROFILE_THK", "CD"]})

    result = exclude_cpm_cpk_parameters(dataframe, ["PROFILE"])

    assert result["param_name"].tolist() == ["PPA_THK", "CD"]


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
        end_date="2026-06-08",
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


def test_spc_service_requests_spc_only_and_returns_distribution_report(monkeypatch, tmp_path: Path) -> None:
    SpcReportService.fetch_spc_report_payload.clear()
    FakeSpcRepository.seen_data_type_filters = []
    monkeypatch.setattr(
        spc_service.ConfigLoader,
        "get_spc_period_sigma_source",
        staticmethod(lambda: "sheet_mean"),
    )
    monkeypatch.setattr(
        decorated_data.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-08",
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
    assert "chart_type" not in report.period_capability_df.columns
    assert "chart_type" not in report.sheet_features_df.columns
    assert "chart_type" not in report.raw_measurements_df.columns
    assert "chart_type" not in report.indicators_df.columns
    assert set(report.period_capability_df["sigma_source"]) == {"sheet_mean"}
    assert set(report.period_capability_df["cpk_decorated"]) == {False}
    assert set(report.period_capability_df["cpm_decorated"]) == {False}
    assert set(report.raw_measurements_df["data_type"]) == {"SPC"}
    assert set(report.raw_measurements_df["main_process_unit_id"]) == {"MAIN-EQP-01"}
    assert set(report.raw_measurements_df["main_process_trace_source"]) == {"array_sht"}
    assert report.sheet_oos_decoration_result is not None
    assert report.cpk_decoration_result is not None
    assert report.cpm_decoration_result is not None


def test_spc_service_can_switch_period_sigma_source_from_global_config(monkeypatch, tmp_path: Path) -> None:
    SpcReportService.fetch_spc_report_payload.clear()
    FakeSpcRepository.seen_data_type_filters = []
    monkeypatch.setattr(
        spc_service.ConfigLoader,
        "get_spc_period_sigma_source",
        staticmethod(lambda: "point_value"),
    )
    monkeypatch.setattr(
        decorated_data.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )

    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-08",
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


def test_spc_service_excludes_configured_parameters_from_cpm_and_cpk_calculation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class ExemptOnlySpcRepository(FakeSpcRepository):
        def get_spc_measurements(
            self,
            config: SpcQueryConfig,
            force_refresh: bool = False,
        ) -> pd.DataFrame:
            measurements_df = super().get_spc_measurements(config, force_refresh)
            return measurements_df.assign(param_name="PROFILE_THK")

        def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
            spec_df = super().get_spc_spec_limits(prod_code)
            return spec_df.assign(param_name="PROFILE_THK")

    SpcReportService.fetch_spc_report_payload.clear()
    monkeypatch.setattr(
        spc_service.ConfigLoader,
        "get_spc_period_sigma_source",
        staticmethod(lambda: "sheet_mean"),
    )
    monkeypatch.setattr(
        spc_service.ConfigLoader,
        "get_spc_capability_param_exemptions",
        staticmethod(lambda: ["PROFILE"]),
    )
    monkeypatch.setattr(
        decorated_data.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-08",
        data_type_filter="SPC",
    )

    report = SpcReportService.get_spc_report_data(
        _data_port=ExemptOnlySpcRepository(Path("data"), True, object()),
        query_config_json=query.model_dump_json(),
        snapshot_signature="configured-exclusion-from-capability",
    )

    assert not report.raw_measurements_df.empty
    assert not report.sheet_features_df.empty
    assert set(report.indicators_df["param_name"]) == {"PROFILE_THK"}
    assert report.period_capability_df.empty
    assert report.cpk_decoration_result is not None
    assert report.cpm_decoration_result is not None


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
        "get_cache_ttl_seconds",
        staticmethod(lambda: 12 * 60 * 60),
    )
    monkeypatch.setattr(
        original_module.ConfigLoader,
        "get_spc_period_sigma_source",
        staticmethod(lambda: "sheet_mean"),
    )
    monkeypatch.setattr(
        decorated_data.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-08",
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


def test_spc_service_threads_gate_params_to_shared_pipeline(monkeypatch) -> None:
    """get_spc_report_data 把 product_revision/decision_signature 穿到共享管线。"""
    SpcReportService.fetch_spc_report_payload.clear()
    recorded: dict[str, object] = {}

    def spy_fetch(**kwargs):
        recorded.update(kwargs)
        return {
            "sheet_features_df": pd.DataFrame(),
            "raw_measurements_df": pd.DataFrame(),
            "spec_empty": True,
            "sheet_oos_decoration": None,
        }

    monkeypatch.setattr(spc_service, "fetch_decorated_features", spy_fetch)
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-08",
        data_type_filter="SPC",
    )

    SpcReportService.get_spc_report_data(
        _data_port=FakeSpcRepository(Path("data"), True, object()),
        query_config_json=query.model_dump_json(),
        snapshot_signature="gate-thread-spc",
        product_revision="rev-spc",
        decision_signature="sig-spc",
    )

    assert recorded["scope"] == "spc"
    assert recorded["prod_code"] == "M626"
    assert recorded["product_revision"] == "rev-spc"
    assert recorded["decision_signature"] == "sig-spc"


def test_excel_flag_save_invalidates_capability_cache(monkeypatch, tmp_path: Path) -> None:
    import os
    from src.inline_domain.core.spc.cpk_decoration import (
        build_capability_detail, merge_capability_detail_with_decoration_flags,
    )

    SpcReportService.fetch_spc_report_payload.clear()
    monkeypatch.setattr(spc_service, "resolve_product_resource_dir", lambda _product: tmp_path)
    monkeypatch.setattr(decorated_data.ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))
    original_resource_path = spc_service.ConfigLoader.get_domain_resource_path
    monkeypatch.setattr(
        spc_service.ConfigLoader, "get_domain_resource_path",
        classmethod(lambda cls, domain, key, default_name=None: (
            tmp_path / "spc_cpk_cpm_decoration.xlsx" if key == "spc_cpk_cpm_decoration"
            else original_resource_path(domain, key, default_name)
        )),
    )
    capability = pd.DataFrame([{
        "prod_code": "M626", "factory": "ARRAY", "step_id": "10140", "param_name": "SE_L1T",
        "period_type": "week", "period_label": "2026-W23", "cpk": 1.084, "cpm": 1.133,
    }])
    monkeypatch.setattr(spc_service, "build_period_capability_report", lambda **kwargs: capability.copy())
    ledger_path = tmp_path / "spc_cpk_cpm_decoration.xlsx"
    ledgers = {
        metric: merge_capability_detail_with_decoration_flags(
            build_capability_detail(capability, metric), pd.DataFrame(), metric,
        ) for metric in ("cpk", "cpm")
    }

    def save(enabled: bool) -> None:
        with pd.ExcelWriter(ledger_path) as writer:
            for metric, frame in ledgers.items():
                frame.assign(flag=enabled).to_excel(
                    writer, sheet_name="M626" if metric == "cpk" else "M626_cpm", index=False,
                )

    save(False)
    kwargs = dict(
        _data_port=FakeSpcRepository(Path("data"), True, object()),
        query_config_json=SpcQueryConfig(prod_code="M626", start_date="2026-06-01", end_date="2026-06-08").model_dump_json(),
        snapshot_signature="excel-save-cache-test",
    )
    first = SpcReportService.get_spc_report_data(**kwargs)
    assert first.period_capability_df["cpk"].tolist() == [1.084]
    old_mtime = ledger_path.stat().st_mtime_ns
    save(True)
    os.utime(ledger_path, ns=(old_mtime + 1_000_000_000, old_mtime + 1_000_000_000))
    second = SpcReportService.get_spc_report_data(**kwargs)
    assert second.period_capability_df["cpk"].tolist() == ledgers["cpk"]["cpk_replacement"].tolist()
    assert 1.33 < second.period_capability_df["cpk"].iloc[0] < 1.4
    assert second.period_capability_df["cpm"].tolist() == ledgers["cpm"]["cpm_replacement"].tolist()
    assert 1.33 < second.period_capability_df["cpm"].iloc[0] < 1.4


def test_same_week_snapshot_change_refreshes_saved_values_through_both_caches(monkeypatch, tmp_path):
    from src.inline_domain.infrastructure.spc.spc_repository import spc_input_signature
    from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore
    from src.inline_domain.core.monitor.cpk_latest import latest_capability_alerts

    SpcReportService.fetch_spc_report_payload.clear()
    fetch_decorated_features.clear()
    monkeypatch.setattr(spc_service.ConfigLoader, "get_project_root", lambda: tmp_path)

    class ChangingSource(FakeSpcRepository):
        values = [41., 59., 41., 59.]
        calls = 0

        def get_spc_measurements(self, config, force_refresh=False):
            self.calls += 1
            frame = super().get_spc_measurements(config, force_refresh)
            frame["param_value"] = self.values
            frame["sheet_start_time"] = frame["sheet_start_time"].replace({"2026-06-02": "2026-06-03"})
            return frame

    source = ChangingSource(tmp_path, True, None)
    snapshot = tmp_path / "data/inline_domain/shared/inline_measurements_M626.parquet"
    snapshot.parent.mkdir(parents=True)
    snapshot.write_bytes(b"snapshot-v1")
    query = SpcQueryConfig(prod_code="M626", start_date="2026-06-01", end_date="2026-06-08")
    kwargs = dict(_data_port=source, query_config_json=query.model_dump_json(), period_sigma_source="point_value")
    first = SpcReportService.get_spc_report_data(
        **kwargs, snapshot_signature=spc_input_signature(tmp_path, "M626"),
    )
    assert first.period_capability_df["cpk"].iloc[0] < 1.33
    path = first.cpk_decoration_result.decoration_path
    reader = CpkLatestExcelStore(path)
    assert len(latest_capability_alerts(reader.read_product("M626"), date(2026, 6, 8))) == 1
    source.values = [49., 51., 49., 51.]
    snapshot.write_bytes(b"snapshot-v2-more-data")
    second = SpcReportService.get_spc_report_data(
        **kwargs, snapshot_signature=spc_input_signature(tmp_path, "M626"),
    )
    assert source.calls == 2
    assert second.period_capability_df["cpk"].iloc[0] >= 1.33
    assert latest_capability_alerts(reader.read_product("M626"), date(2026, 6, 8)).empty
    saved = pd.read_excel(path, sheet_name="M626")
    assert len(saved) == 1 and saved["cpk_corrected"].iloc[0] >= 1.33


def _decoration_payload_with_decisions() -> dict:
    return {
        "decoration_df": pd.DataFrame(),
        "decoration_path": "resources/spc_sheet_oos_decoration.xlsx",
        "decoration_sheet": "M626",
        "decision_sheet": "M626__flags",
        "decision_df": pd.DataFrame(
            [
                {
                    "prod_code": "M626",
                    "step_id": "S1",
                    "param_name": "THK",
                    "sheet_id": "LOT00000101",
                    "flag": False,
                }
            ]
        ),
        "refresh_reason": "decision_changed",
    }


def test_view_model_from_payload_preserves_decision_ledger() -> None:
    """缓存 payload 往返后 decision_df/decision_sheet/refresh_reason 不得丢失。"""
    payload = {"sheet_oos_decoration": _decoration_payload_with_decisions()}

    report = SpcReportService._view_model_from_payload(payload)

    result = report.sheet_oos_decoration_result
    assert result is not None
    assert result.decision_sheet == "M626__flags"
    assert result.decision_df is not None
    assert result.decision_df["flag"].tolist() == [False]
    assert result.refresh_reason == "decision_changed"


def test_view_model_from_payload_tolerates_legacy_decoration_payload() -> None:
    """旧缓存条目（无决策字段）重建时退化为默认空值，不抛错。"""
    payload = {
        "sheet_oos_decoration": {
            "decoration_df": pd.DataFrame(),
            "decoration_path": "resources/spc_sheet_oos_decoration.xlsx",
            "decoration_sheet": "M626",
        },
    }

    report = SpcReportService._view_model_from_payload(payload)

    result = report.sheet_oos_decoration_result
    assert result is not None
    assert result.decision_df is None
    assert result.decision_sheet == ""
    assert result.refresh_reason == ""
