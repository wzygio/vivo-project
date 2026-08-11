from datetime import datetime
from pathlib import Path

import pandas as pd

from src.inline_domain.application.spc.dtos import SpcQueryConfig
import src.inline_domain.infrastructure.spc.repositories.spc_repository as spc_repository
from src.inline_domain.infrastructure.spc.repositories.spc_repository import SpcRepository


class DummyDbManager:
    engine = object()


def _snapshot_rows() -> pd.DataFrame:
    rows = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "sheet_start_time": datetime(2026, 6, 30),
                "sheet_id": "S1",
                "step_id": "10140",
                "param_name": "PPA_B_X",
                "site_name": "P1",
                "unit_id": "3CEE01-PPA",
                "param_value": 1.0,
            },
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "sheet_start_time": datetime(2026, 6, 30),
                "sheet_id": "S2",
                "step_id": "10140",
                "param_name": "TOTAL_LOSS_RATE",
                "site_name": "P1",
                "unit_id": "3CEE01-PPA",
                "param_value": 2.0,
            },
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "sheet_start_time": datetime(2026, 6, 30),
                "sheet_id": "S3",
                "step_id": "10140",
                "param_name": "MT_CH_PRESS_A",
                "site_name": "P1",
                "unit_id": "3CEE01-PPA",
                "param_value": 3.0,
            },
        ]
    )
    return rows.assign(
        main_step_id="10000",
        main_eqp_type="EQP",
        main_process_unit_id="MAIN-EQP-01",
        main_process_event_time=pd.Timestamp("2026-06-29 12:00:00"),
        main_process_trace_source="array_sht",
    )


def test_repository_keeps_whitelisted_mt_ch_spc_params_from_fresh_snapshot(
    monkeypatch,
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "spc_snapshot_M626.parquet"
    _snapshot_rows().to_parquet(snapshot_path, index=False)
    snapshot_path.with_suffix(".policy").write_text(
        "spc-main-process-trace-v2",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        spc_repository,
        "load_param_whitelist",
        lambda db, prod: pd.DataFrame(
            [
                {"ref_param_name": "PPA_B_X", "data_type": "SPC"},
                {"ref_param_name": "TOTAL_LOSS_RATE", "data_type": "SPC"},
                {"ref_param_name": "MT_CH_PRESS_A", "data_type": "SPC"},
            ]
        ),
    )
    monkeypatch.setattr(SpcRepository, "_apply_outlier_filters", lambda self, df, prod: df)

    repo = SpcRepository(snapshot_dir=tmp_path, use_snapshot=True, db_manager=DummyDbManager())
    result = repo.get_spc_measurements(
        SpcQueryConfig(
            prod_code="M626",
            start_date="2026-06-01",
            end_date="2026-06-30",
        )
    )

    assert result["param_name"].tolist() == ["PPA_B_X", "MT_CH_PRESS_A"]


def test_repository_refreshes_snapshot_created_with_old_param_filter_policy(
    monkeypatch,
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "spc_snapshot_M626.parquet"
    _snapshot_rows().iloc[:1].to_parquet(snapshot_path, index=False)
    refresh_calls: list[str] = []

    def fake_load_spc_measurements(db, start_date, end_date, prod_code) -> pd.DataFrame:
        refresh_calls.append(prod_code)
        return _snapshot_rows()

    monkeypatch.setattr(
        spc_repository,
        "load_spc_measurements",
        fake_load_spc_measurements,
    )
    monkeypatch.setattr(
        spc_repository,
        "load_param_whitelist",
        lambda db, prod: pd.DataFrame(
            [
                {"ref_param_name": "PPA_B_X", "data_type": "SPC"},
                {"ref_param_name": "TOTAL_LOSS_RATE", "data_type": "SPC"},
                {"ref_param_name": "MT_CH_PRESS_A", "data_type": "SPC"},
            ]
        ),
    )
    monkeypatch.setattr(SpcRepository, "_apply_outlier_filters", lambda self, df, prod: df)

    repo = SpcRepository(snapshot_dir=tmp_path, use_snapshot=True, db_manager=DummyDbManager())
    result = repo.get_spc_measurements(
        SpcQueryConfig(
            prod_code="M626",
            start_date="2026-06-01",
            end_date="2026-06-30",
        )
    )

    assert refresh_calls == ["M626"]
    assert result["param_name"].tolist() == ["PPA_B_X", "MT_CH_PRESS_A"]
    assert snapshot_path.with_suffix(".policy").read_text(encoding="utf-8") == (
        "spc-main-process-trace-v2"
    )


def test_repository_falls_back_to_legacy_snapshot_when_policy_refresh_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "spc_snapshot_M626.parquet"
    _snapshot_rows().iloc[:1].to_parquet(snapshot_path, index=False)

    monkeypatch.setattr(
        spc_repository,
        "load_spc_measurements",
        lambda db, start_date, end_date, prod_code: pd.DataFrame(),
    )
    monkeypatch.setattr(
        spc_repository,
        "load_param_whitelist",
        lambda db, prod: pd.DataFrame(
            [{"ref_param_name": "PPA_B_X", "data_type": "SPC"}]
        ),
    )
    monkeypatch.setattr(SpcRepository, "_apply_outlier_filters", lambda self, df, prod: df)

    repo = SpcRepository(snapshot_dir=tmp_path, use_snapshot=True, db_manager=DummyDbManager())
    result = repo.get_spc_measurements(
        SpcQueryConfig(
            prod_code="M626",
            start_date="2026-06-01",
            end_date="2026-06-30",
        )
    )

    assert result["param_name"].tolist() == ["PPA_B_X"]
    assert not snapshot_path.with_suffix(".policy").exists()


def test_repository_enriches_refreshed_measurements_with_main_process_trace(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        spc_repository,
        "load_spc_measurements",
        lambda db, start_date, end_date, prod_code: _snapshot_rows()
        .iloc[:1]
        .drop(columns=SpcRepository.TRACE_SNAPSHOT_COLUMNS)
        .copy(),
    )
    monkeypatch.setattr(
        spc_repository,
        "load_spc_spec_limits",
        lambda db, prod: pd.DataFrame(
            [
                {
                    "prod_code": "M626",
                    "step_id": "10140",
                    "param_name": "PPA_B_X",
                    "main_step_id": "10000",
                    "main_eqp_type": "EQP",
                }
            ]
        ),
    )

    def fake_enrich(db, measurements, specifications, history_start, history_end):
        captured.update(
            {
                "rows": len(measurements),
                "history_start": history_start,
                "history_end": history_end,
            }
        )
        return measurements.assign(
            main_step_id="10000",
            main_eqp_type="EQP",
            main_process_unit_id="MAIN-EQP-01",
            main_process_event_time=pd.Timestamp("2026-06-29 12:00:00"),
            main_process_trace_source="array_sht",
        )

    monkeypatch.setattr(
        spc_repository,
        "enrich_measurements_with_main_process_trace",
        fake_enrich,
        raising=False,
    )
    monkeypatch.setattr(
        spc_repository,
        "load_param_whitelist",
        lambda db, prod: pd.DataFrame(
            [{"ref_param_name": "PPA_B_X", "data_type": "SPC"}]
        ),
    )
    monkeypatch.setattr(SpcRepository, "_apply_outlier_filters", lambda self, df, prod: df)

    repo = SpcRepository(snapshot_dir=tmp_path, use_snapshot=True, db_manager=DummyDbManager())
    result = repo.get_spc_measurements(
        SpcQueryConfig(
            prod_code="M626",
            start_date="2026-06-01",
            end_date="2026-06-30",
        )
    )

    assert captured == {
        "rows": 1,
        "history_start": datetime(2026, 2, 28),
        "history_end": datetime(2026, 6, 30),
    }
    assert result.loc[0, "main_process_unit_id"] == "MAIN-EQP-01"
    assert (tmp_path / "spc_snapshot_M626.policy").read_text(encoding="utf-8") == (
        "spc-main-process-trace-v2"
    )
