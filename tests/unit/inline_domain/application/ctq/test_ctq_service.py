import importlib
import sys
import threading
from pathlib import Path

import pandas as pd

from src.inline_domain.application.ctq import ctq_service
from src.inline_domain.application.shared import decorated_data
from src.inline_domain.application.ctq.ctq_service import CtqReportService
from src.inline_domain.application.spc.dtos import SpcQueryConfig


class FakeCtqRepository:
    seen_data_type_filters: list[str] = []

    def __init__(self, snapshot_dir: Path, use_snapshot: bool, db_manager: object) -> None:
        self.snapshot_dir = snapshot_dir
        self.use_snapshot = use_snapshot
        self.db_manager = db_manager

    def get_spc_measurements(
        self,
        config: SpcQueryConfig,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        self.seen_data_type_filters.append(config.data_type_filter or "")
        rows: list[dict[str, object]] = []
        for param_name, values in {
            "SE_L1T_UNI": (0.95, 1.05),
            "THK": (49.0, 51.0),
        }.items():
            for site_name, value in zip(("P1", "P2"), values, strict=True):
                rows.append(
                    {
                        "factory": "ARRAY",
                        "prod_code": config.prod_code,
                        "sheet_start_time": "2026-07-01 08:00:00",
                        "sheet_id": "CTQ00000101",
                        "step_id": "12140",
                        "param_name": param_name,
                        "site_name": site_name,
                        "param_value": value,
                        "data_type": "CTQ",
                    }
                )
        return pd.DataFrame(rows)

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "prod_code": prod_code,
                    "step_id": "12140",
                    "param_name": "SE_L1T_UNI",
                    "usl": 1.2,
                    "lsl": 0.8,
                    "ucl": 1.15,
                    "lcl": 0.85,
                    "target": 1.0,
                },
                {
                    "prod_code": prod_code,
                    "step_id": "12140",
                    "param_name": "THK",
                    "usl": 55.0,
                    "lsl": 45.0,
                    "ucl": 54.0,
                    "lcl": 46.0,
                    "target": 50.0,
                },
            ]
        )


def test_ctq_service_loads_ctq_distributions_without_capability_fields(
    monkeypatch,
    tmp_path: Path,
) -> None:
    CtqReportService.fetch_ctq_report_payload.clear()
    FakeCtqRepository.seen_data_type_filters = []
    monkeypatch.setattr(
        decorated_data.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )
    query = SpcQueryConfig(
        prod_code="M678",
        start_date="2026-07-01",
        end_date="2026-07-07",
        data_type_filter="SPC",
    )

    report = CtqReportService.get_ctq_report_data(
        _data_port=FakeCtqRepository(Path("data"), True, object()),
        query_config_json=query.model_dump_json(),
        snapshot_signature="ctq-tracer",
    )

    assert FakeCtqRepository.seen_data_type_filters == ["CTQ"]
    assert not report.sheet_features_df.empty
    assert len(report.raw_measurements_df) == 4
    assert set(report.raw_measurements_df["data_type"]) == {"CTQ"}
    assert dict(zip(report.indicators_df["param_name"], report.indicators_df["chart_type"])) == {
        "SE_L1T_UNI": "line",
        "THK": "box",
    }
    assert report.sheet_oos_decoration_result is not None
    assert report.sheet_oos_decoration_result.decoration_path.parent == tmp_path / "resources"
    assert report.sheet_oos_decoration_result.decoration_sheet == "M678"
    assert not (tmp_path / "resources" / "ctq_sheet_oos_detail.xlsx").exists()
    assert [
        path.name for path in (tmp_path / "resources").glob("*.xlsx")
    ] == ["ctq_sheet_oos_decoration.xlsx"]
    assert not hasattr(report, "period_capability_df")
    assert not hasattr(report, "cpk_decoration_result")


def test_ctq_report_remains_available_when_service_module_reloads_during_cache_fill(
    monkeypatch,
    tmp_path: Path,
) -> None:
    entered_repository = threading.Event()
    continue_repository = threading.Event()

    class BlockingCtqRepository(FakeCtqRepository):
        def get_spc_measurements(
            self,
            config: SpcQueryConfig,
            force_refresh: bool = False,
        ) -> pd.DataFrame:
            entered_repository.set()
            assert continue_repository.wait(timeout=10)
            return super().get_spc_measurements(config, force_refresh)

    original_module = ctq_service
    original_service = original_module.CtqReportService
    original_service.fetch_ctq_report_payload.clear()
    monkeypatch.setattr(
        decorated_data.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )
    query = SpcQueryConfig(
        prod_code="M678",
        start_date="2026-07-01",
        end_date="2026-07-07",
        data_type_filter="CTQ",
    )
    outcome: dict[str, object] = {}

    def load_report() -> None:
        try:
            outcome["report"] = original_service.get_ctq_report_data(
                _data_port=BlockingCtqRepository(Path("data"), True, object()),
                query_config_json=query.model_dump_json(),
                snapshot_signature="ctq-reload-during-cache-fill",
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
    assert isinstance(report, original_module.CtqReportViewModel)
    assert not report.raw_measurements_df.empty


def test_ctq_service_returns_an_empty_view_model_when_physical_data_is_unavailable(
    tmp_path: Path,
) -> None:
    class EmptyCtqRepository(FakeCtqRepository):
        def get_spc_measurements(
            self,
            config: SpcQueryConfig,
            force_refresh: bool = False,
        ) -> pd.DataFrame:
            return pd.DataFrame()

    CtqReportService.fetch_ctq_report_payload.clear()
    query = SpcQueryConfig(
        prod_code="NO_CTQ_DATA",
        start_date="2026-07-01",
        end_date="2026-07-07",
        data_type_filter="SPC",
    )
    snapshot_dir = tmp_path / "data" / query.prod_code

    report = CtqReportService.get_ctq_report_data(
        _data_port=EmptyCtqRepository(snapshot_dir, True, object()),
        query_config_json=query.model_dump_json(),
        snapshot_signature="ctq-empty-data",
    )

    assert report.sheet_features_df.empty
    assert report.raw_measurements_df.empty
    assert report.indicators_df.empty
    assert report.sheet_oos_decoration_result is None


def test_ctq_service_threads_gate_params_to_shared_pipeline(monkeypatch) -> None:
    """get_ctq_report_data 把 product_revision/decision_signature 穿到共享管线。"""
    CtqReportService.fetch_ctq_report_payload.clear()
    recorded: dict[str, object] = {}

    def spy_fetch(**kwargs):
        recorded.update(kwargs)
        return {
            "sheet_features_df": pd.DataFrame(),
            "raw_measurements_df": pd.DataFrame(),
            "spec_empty": True,
            "sheet_oos_decoration": None,
        }

    monkeypatch.setattr(ctq_service, "fetch_decorated_features", spy_fetch)
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-07",
        data_type_filter="CTQ",
    )

    CtqReportService.get_ctq_report_data(
        _data_port=FakeCtqRepository(Path("data"), True, object()),
        query_config_json=query.model_dump_json(),
        snapshot_signature="gate-thread-ctq",
        product_revision="rev-ctq",
        decision_signature="sig-ctq",
    )

    assert recorded["scope"] == "ctq"
    assert recorded["prod_code"] == "M626"
    assert recorded["product_revision"] == "rev-ctq"
    assert recorded["decision_signature"] == "sig-ctq"
