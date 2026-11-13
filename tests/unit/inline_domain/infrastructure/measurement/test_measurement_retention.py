"""Product retention survives cache reuse, month changes, full refresh and fallback."""
import json

import pandas as pd
import pytest

from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import (
    InlineMeasurementSnapshotRepository,
)
from src.inline_domain.infrastructure.shared.snapshot_window import IncompleteMonitorSnapshotError
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy


@pytest.fixture
def retention(monkeypatch):
    config = {"measurement_snapshots": {"retain_from": {"Z553": "2026-05-01"}}}
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _domain: config)
    monkeypatch.setattr(ConfigLoader, "get_data_forward_policy", lambda: DataForwardPolicy())
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    monkeypatch.setattr(ConfigLoader, "get_incremental_refresh_days", lambda: 7)
    return config


def facts():
    return pd.DataFrame({
        "start_time": pd.to_datetime(["2026-05-06", "2026-09-12", "2026-11-12"]),
        "param_value": [10.0, 20.0, 30.0],
    })


def test_retained_history_is_reused_then_survives_incremental_month_transition(tmp_path, retention):
    calls = []
    batches = [facts(), facts().iloc[:0]]

    def loader(_db, start, end, product):
        calls.append((start, end, product))
        return batches.pop(0)

    repo = InlineMeasurementSnapshotRepository(tmp_path, object(), loader)
    repo.get_measurements("Z553", "2026-11-13")
    repo.get_measurements("Z553", "2026-11-13")
    assert calls == [("2026-05-01", "2026-11-13", "Z553")]
    repo.get_measurements("Z553", "2026-12-01")
    assert calls[-1] == ("2026-11-06", "2026-12-01", "Z553")
    path = tmp_path / "inline_measurements_Z553.parquet"
    pd.testing.assert_frame_equal(pd.read_parquet(path), facts().iloc[:2])
    metadata = json.loads(path.with_suffix(".snapshot.json").read_text())
    assert metadata["covered_from"] == "2026-05-01"
    assert metadata["covered_through"] == "2026-12-01"


@pytest.mark.parametrize("entrypoint", ["get_measurements", "refresh_measurements"])
@pytest.mark.parametrize("product,expected_start", [("Z553", "2026-05-01"), ("M678", "2026-08-01")])
def test_full_refresh_uses_product_window(tmp_path, retention, entrypoint, product, expected_start):
    calls = []

    def loader(_db, start, end, product):
        calls.append(start)
        return facts().loc[facts().start_time.ge(start)].reset_index(drop=True)

    repo = InlineMeasurementSnapshotRepository(tmp_path, object(), loader)
    repo.get_measurements(product, "2026-11-13")
    if entrypoint == "get_measurements":
        repo.get_measurements(product, "2026-11-13", force_refresh=True)
    else:
        assert repo.refresh_measurements(product, "2026-11-13").refreshed_from_db
    assert calls == [expected_start, expected_start]
    actual = pd.read_parquet(tmp_path / f"inline_measurements_{product}.parquet")
    assert actual.start_time.min() == (pd.Timestamp("2026-05-06") if product == "Z553" else pd.Timestamp("2026-09-12"))


@pytest.mark.parametrize("has_history", [True, False])
def test_failure_fallback_requires_retained_coverage(tmp_path, retention, has_history):
    fail = False
    if not has_history:
        retention.clear()

    def loader(*args):
        if fail:
            raise OSError("source unavailable")
        return facts()

    repo = InlineMeasurementSnapshotRepository(tmp_path, object(), loader)
    repo.get_measurements("Z553", "2026-11-13")
    originals = {p: p.read_bytes() for p in tmp_path.iterdir()}
    retention.update({"measurement_snapshots": {"retain_from": {"Z553": "2026-05-01"}}})
    fail = True
    if has_history:
        result = repo.refresh_measurements("Z553", "2026-11-14")
        assert result.refreshed_from_db is False
        assert result.measurements.start_time.min() == pd.Timestamp("2026-05-06")
    else:
        with pytest.raises(IncompleteMonitorSnapshotError):
            repo.refresh_measurements("Z553", "2026-11-14")
    assert {p: p.read_bytes() for p in originals} == originals


@pytest.mark.parametrize("value", [None, True, 123, [], {}, "2026-02-30", "20260501", "2026-05-01T00:00:00"])
def test_invalid_retention_date_fails_before_query(monkeypatch, value):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _domain: {
        "measurement_snapshots": {"retain_from": {"Z553": value}},
    })
    with pytest.raises(ValueError, match="measurement_snapshots.retain_from"):
        ConfigLoader.get_inline_measurement_retention_start("Z553")


@pytest.mark.parametrize("end,expected", [("2027-11-13", "2026-05-01"), ("2026-06-01", "2026-03-01")])
def test_retention_extends_default_window_and_does_not_move_with_years(retention, end, expected):
    from src.inline_domain.infrastructure.shared.snapshot_window import inline_measurement_snapshot_window_start

    assert inline_measurement_snapshot_window_start("Z553", end) == pd.Timestamp(expected)


@pytest.mark.parametrize("config", [{}, {"measurement_snapshots": {}}, {"measurement_snapshots": {"retain_from": {"OTHER": "2026-01-01"}}}])
def test_absent_product_retention_keeps_default(monkeypatch, config):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _domain: config)
    assert ConfigLoader.get_inline_measurement_retention_start("Z553") is None


@pytest.mark.parametrize("config", [
    {"measurement_snapshots": None},
    {"measurement_snapshots": []},
    {"measurement_snapshots": {"retain_from": None}},
    {"measurement_snapshots": {"retain_from": []}},
])
def test_invalid_retention_mapping_is_rejected(monkeypatch, config):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _domain: config)
    with pytest.raises(ValueError, match="measurement_snapshots.retain_from"):
        ConfigLoader.get_inline_measurement_retention_start("Z553")
