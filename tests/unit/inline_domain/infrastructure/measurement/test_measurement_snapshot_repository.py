from __future__ import annotations

from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import json

import pandas as pd
import pytest

from src.shared_kernel.data_forward import DataForwardPolicy
from src.inline_domain.infrastructure.shared.snapshot_window import IncompleteMonitorSnapshotError

from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import (
    InlineMeasurementSnapshotRepository,
    MeasurementRefreshResult,
)


@pytest.fixture(autouse=True)
def fixed_display_policy(monkeypatch):
    monkeypatch.setattr(
        "src.shared_kernel.config.ConfigLoader.get_data_forward_policy",
        lambda: DataForwardPolicy(enabled=True, offset_days=4),
    )


def _write_coverage(snapshot_path):
    snapshot_path.with_suffix(".policy").write_text(
        InlineMeasurementSnapshotRepository.SNAPSHOT_POLICY_VERSION, encoding="utf-8",
    )
    snapshot_path.with_suffix(".snapshot.json").write_text(
        json.dumps({"covered_from": "2026-01-01", "covered_through": "2026-08-13"}),
        encoding="utf-8",
    )


def _raw_measurements() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-08-13 08:00:00"),
                "sheet_id": "SHEET-1",
                "lot_id": "LOT-1",
                "step_id": "11620",
                "param_name": "TDSUM",
                "site_name": "S1",
                "unit_id": "EQ-1",
                "param_value": 3.0,
            }
        ]
    )


def test_repository_reuses_one_product_snapshot_for_repeated_reads(tmp_path) -> None:
    calls: list[tuple[str, str, str]] = []

    def loader(_db, start_date: str, end_date: str, prod_code: str) -> pd.DataFrame:
        calls.append((start_date, end_date, prod_code))
        return _raw_measurements()

    repository = InlineMeasurementSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        measurement_loader=loader,
    )

    first = repository.get_measurements(prod_code="M678", end_date="2026-08-13")
    second = repository.get_measurements(prod_code="M678", end_date="2026-08-13")

    assert calls == [("2026-01-01", "2026-08-13", "M678")]
    assert first.equals(second)
    assert first.loc[0, "lot_id"] == "LOT-1"
    assert first.loc[0, "start_time"] == pd.Timestamp("2026-08-17 08:00:00")
    snapshot_path = tmp_path / "inline_measurements_M678.parquet"
    assert pd.read_parquet(snapshot_path).loc[0, "start_time"] == pd.Timestamp(
        "2026-08-13 08:00:00"
    )


def test_repository_refreshes_snapshot_without_current_raw_policy(tmp_path) -> None:
    snapshot_path = tmp_path / "inline_measurements_M678.parquet"
    _raw_measurements().assign(lot_id="STALE").to_parquet(snapshot_path, index=False)
    calls = 0

    def loader(_db, _start_date: str, _end_date: str, _prod_code: str) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _raw_measurements()

    repository = InlineMeasurementSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        measurement_loader=loader,
    )

    result = repository.get_measurements(prod_code="M678", end_date="2026-08-13")

    assert calls == 1
    assert result.loc[0, "lot_id"] == "LOT-1"
    assert (tmp_path / "inline_measurements_M678.policy").read_text(encoding="utf-8") == (
        InlineMeasurementSnapshotRepository.SNAPSHOT_POLICY_VERSION
    )


def test_repository_falls_back_to_existing_snapshot_when_refresh_fails(tmp_path) -> None:
    snapshot_path = tmp_path / "inline_measurements_M678.parquet"
    stale = _raw_measurements().assign(lot_id="FALLBACK")
    stale.to_parquet(snapshot_path, index=False)
    _write_coverage(snapshot_path)
    snapshot_path.with_suffix(".policy").write_text(
        InlineMeasurementSnapshotRepository.SNAPSHOT_POLICY_VERSION,
        encoding="utf-8",
    )

    def failing_loader(*_args) -> pd.DataFrame:
        raise RuntimeError("database unavailable")

    repository = InlineMeasurementSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        measurement_loader=failing_loader,
    )

    result = repository.get_measurements(
        prod_code="M678",
        end_date="2026-08-13",
        force_refresh=True,
    )

    assert result.loc[0, "lot_id"] == "FALLBACK"
    assert repository.last_refresh_from_db is False


def test_refresh_measurements_reports_fallback_as_not_refreshed(tmp_path) -> None:
    snapshot_path = tmp_path / "inline_measurements_M678.parquet"
    stale = _raw_measurements().assign(lot_id="FALLBACK")
    stale.to_parquet(snapshot_path, index=False)
    _write_coverage(snapshot_path)

    def failing_loader(*_args) -> pd.DataFrame:
        raise RuntimeError("database unavailable")

    repository = InlineMeasurementSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        measurement_loader=failing_loader,
    )

    result = repository.refresh_measurements(prod_code="M678", end_date="2026-08-13")

    assert isinstance(result, MeasurementRefreshResult)
    assert result.refreshed_from_db is False
    assert repository.last_refresh_from_db is False
    # 数据内容仍是降级后的旧快照，前端读取不受影响
    assert result.measurements.loc[0, "lot_id"] == "FALLBACK"


def test_refresh_measurements_treats_empty_window_as_success(tmp_path) -> None:
    def empty_loader(*_args) -> pd.DataFrame:
        return pd.DataFrame(columns=list(_raw_measurements().columns))

    repository = InlineMeasurementSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        measurement_loader=empty_loader,
    )

    result = repository.refresh_measurements(prod_code="M678", end_date="2026-08-13")

    assert result.refreshed_from_db is True
    assert result.measurements.empty


def test_refresh_measurements_reports_real_refresh_as_success(tmp_path) -> None:
    def loader(*_args) -> pd.DataFrame:
        return _raw_measurements()

    repository = InlineMeasurementSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        measurement_loader=loader,
    )

    result = repository.refresh_measurements(prod_code="M678", end_date="2026-08-13")

    assert result.refreshed_from_db is True
    assert result.measurements.loc[0, "lot_id"] == "LOT-1"


def test_repository_coalesces_concurrent_first_reads(tmp_path) -> None:
    calls = 0
    calls_lock = threading.Lock()

    def slow_loader(*_args) -> pd.DataFrame:
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.1)
        return _raw_measurements()

    repository = InlineMeasurementSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        measurement_loader=slow_loader,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda _: repository.get_measurements(
                    prod_code="M678",
                    end_date="2026-08-13",
                ),
                range(2),
            )
        )

    assert calls == 1
    assert all(result.loc[0, "lot_id"] == "LOT-1" for result in results)
    assert not list(tmp_path.glob("*.tmp"))


def test_old_short_snapshot_cannot_be_fallback_for_year_summary(tmp_path):
    snapshot_path = tmp_path / "inline_measurements_M678.parquet"
    _raw_measurements().to_parquet(snapshot_path, index=False)
    snapshot_path.with_suffix(".policy").write_text("inline-measurement-raw-v2", encoding="utf-8")

    def fail(*args):
        raise RuntimeError("database unavailable")

    repository = InlineMeasurementSnapshotRepository(tmp_path, object(), measurement_loader=fail)
    with pytest.raises(IncompleteMonitorSnapshotError, match="未覆盖"):
        repository.get_measurements("M678", "2026-09-07")
    assert snapshot_path.exists()


def test_missing_snapshot_and_database_failure_is_not_zero_year_data(tmp_path):
    def fail(*args):
        raise RuntimeError("database unavailable")

    repository = InlineMeasurementSnapshotRepository(tmp_path, object(), measurement_loader=fail)
    with pytest.raises(IncompleteMonitorSnapshotError):
        repository.get_measurements("M678", "2026-09-07")


def test_refresh_metadata_records_query_coverage_not_first_fact_time(tmp_path):
    repository = InlineMeasurementSnapshotRepository(
        tmp_path, object(), measurement_loader=lambda *args: _raw_measurements(),
    )
    repository.get_measurements("M678", "2026-08-13")
    metadata = json.loads((tmp_path / "inline_measurements_M678.snapshot.json").read_text())
    assert metadata["covered_from"] == "2026-01-01"
    assert metadata["covered_through"] == "2026-08-13"
