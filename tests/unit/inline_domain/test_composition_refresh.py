from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from src.inline_domain import composition
from src.inline_domain.infrastructure.measurement.measurement_snapshot_repository import (
    InlineMeasurementSnapshotRepository,
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


def _repository(tmp_path, loader) -> InlineMeasurementSnapshotRepository:
    return InlineMeasurementSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        measurement_loader=loader,
    )


def _use_repository(monkeypatch, repository: InlineMeasurementSnapshotRepository) -> None:
    monkeypatch.setattr(
        composition,
        "build_raw_measurement_repository",
        lambda *_args, **_kwargs: repository,
    )


def test_refresh_raw_measurements_fails_when_db_refresh_falls_back(
    monkeypatch,
    tmp_path,
) -> None:
    """DB 失败 + 降级返回旧快照：handler 必须返回 False（PRD 11.1）。"""
    stale = _raw_measurements().assign(lot_id="FALLBACK")
    stale.to_parquet(tmp_path / "inline_measurements_M678.parquet", index=False)

    def failing_loader(*_args) -> pd.DataFrame:
        raise RuntimeError("database unavailable")

    _use_repository(monkeypatch, _repository(tmp_path, failing_loader))

    assert (
        composition.refresh_raw_measurements(object(), "M678", "2026-08-13") is False
    )


def test_refresh_raw_measurements_treats_empty_window_as_success(
    monkeypatch,
    tmp_path,
) -> None:
    """数据库正常返回空窗口是合法结果，不得误报失败。"""
    def empty_loader(*_args) -> pd.DataFrame:
        return pd.DataFrame(columns=list(_raw_measurements().columns))

    _use_repository(monkeypatch, _repository(tmp_path, empty_loader))

    assert (
        composition.refresh_raw_measurements(object(), "M678", "2026-08-13") is True
    )


def test_refresh_raw_measurements_succeeds_on_real_refresh(
    monkeypatch,
    tmp_path,
) -> None:
    def loader(*_args) -> pd.DataFrame:
        return _raw_measurements()

    _use_repository(monkeypatch, _repository(tmp_path, loader))

    assert (
        composition.refresh_raw_measurements(object(), "M678", "2026-08-13") is True
    )
