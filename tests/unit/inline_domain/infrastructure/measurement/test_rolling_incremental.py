"""Raw snapshots replace a covered tail; empty queries also establish coverage."""
import json

import pandas as pd
import pytest

from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import InlineMeasurementSnapshotRepository
from src.shared_kernel.data_forward import DataForwardPolicy


def test_incremental_replaces_deleted_tail_and_never_corrects_old_rows_twice(tmp_path, monkeypatch):
    monkeypatch.setattr("src.shared_kernel.config.ConfigLoader.get_data_forward_policy", lambda: DataForwardPolicy(enabled=False))
    calls = []
    batches = [pd.DataFrame({"start_time": pd.to_datetime(["2026-06-01", "2026-08-10", "2026-08-13"]), "param_value": [1, 2, 3]}), pd.DataFrame({"start_time": pd.to_datetime(["2026-08-14", "2026-08-14"]), "param_value": [4, 4]})]
    def loader(_db, start, end, product):
        calls.append((start, end))
        return batches.pop(0)
    repo = InlineMeasurementSnapshotRepository(tmp_path, object(), loader, lambda df: df.assign(param_value=df.param_value + 10))
    repo.get_measurements("M1", "2026-08-13")
    result = repo.get_measurements("M1", "2026-08-14", force_refresh=True)
    assert calls == [("2026-05-01", "2026-08-13"), ("2026-08-11", "2026-08-14")]
    assert result.param_value.tolist() == [11, 12, 14]


def test_empty_query_is_persisted_and_reused_without_latest_fact(tmp_path):
    calls = []
    def loader(*args):
        calls.append(args)
        return pd.DataFrame(columns=["start_time", "param_value"])
    repo = InlineMeasurementSnapshotRepository(tmp_path, object(), loader)
    assert repo.get_measurements("M1", "2026-09-08").empty
    assert repo.get_measurements("M1", "2026-09-08").empty
    assert len(calls) == 1
    meta = json.loads((tmp_path / "inline_measurements_M1.snapshot.json").read_text())
    assert meta["covered_from"] == "2026-06-01"
    assert meta["covered_through"] == "2026-09-08"
    assert meta["refreshed_at"]


def test_month_transition_prunes_old_month_and_replaces_empty_tail(tmp_path, monkeypatch):
    monkeypatch.setattr("src.shared_kernel.config.ConfigLoader.get_data_forward_policy", lambda: DataForwardPolicy(enabled=False))
    batches = [pd.DataFrame({"start_time": pd.to_datetime(["2026-05-20", "2026-06-20", "2026-08-31"])}), pd.DataFrame(columns=["start_time"])]
    repo = InlineMeasurementSnapshotRepository(tmp_path, object(), lambda *args: batches.pop(0))
    repo.get_measurements("M1", "2026-08-31")
    result = repo.get_measurements("M1", "2026-09-01")
    assert result.start_time.tolist() == [pd.Timestamp("2026-06-20")]
    meta = json.loads((tmp_path / "inline_measurements_M1.snapshot.json").read_text())
    assert meta["covered_from"] == "2026-06-01"


def test_measurement_ttl_is_coverage_based_and_expiry_loads_tail(tmp_path):
    calls = []
    def loader(_db, start, end, product):
        calls.append(start)
        return pd.DataFrame({"start_time": pd.to_datetime(["2026-09-01"]), "param_value": [1]})
    repo = InlineMeasurementSnapshotRepository(tmp_path, object(), loader)
    repo.get_measurements("M1", "2026-09-08")
    repo.get_measurements("M1", "2026-09-08")
    assert calls == ["2026-06-01"]
    meta_path = tmp_path / "inline_measurements_M1.snapshot.json"
    meta = json.loads(meta_path.read_text())
    meta["refreshed_at"] = "2020-01-01T00:00:00+00:00"
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    repo.get_measurements("M1", "2026-09-08")
    assert calls == ["2026-06-01", "2026-09-06"]


@pytest.mark.parametrize("damage", ["policy", "metadata", "parquet"])
def test_incomplete_generation_rebuilds_full_window(tmp_path, damage):
    calls = []
    def loader(_db, start, end, product):
        calls.append(start)
        return pd.DataFrame({"start_time": pd.to_datetime(["2026-09-01"])})
    repo = InlineMeasurementSnapshotRepository(tmp_path, object(), loader)
    repo.get_measurements("M1", "2026-09-08")
    path = tmp_path / "inline_measurements_M1.parquet"
    if damage == "policy":
        path.with_suffix(".policy").write_text("old", encoding="utf-8")
    elif damage == "metadata":
        path.with_suffix(".snapshot.json").write_text("{}", encoding="utf-8")
    else:
        path.write_bytes(b"broken parquet")
    repo.get_measurements("M1", "2026-09-09")
    assert calls == ["2026-06-01", "2026-06-01"]


def test_failed_publication_restores_entire_prior_generation(tmp_path, monkeypatch):
    from src.inline_domain.infrastructure.shared import rolling_snapshot
    repo = InlineMeasurementSnapshotRepository(tmp_path, object(), lambda *args: pd.DataFrame({"start_time": pd.to_datetime(["2026-09-01"])}))
    repo.get_measurements("M1", "2026-09-08")
    originals = {p: p.read_bytes() for p in tmp_path.iterdir()}
    real_replace = rolling_snapshot.os.replace
    def fail_metadata(source, target):
        if str(target).endswith(".snapshot.json"):
            raise OSError("simulated metadata publication failure")
        return real_replace(source, target)
    monkeypatch.setattr(rolling_snapshot.os, "replace", fail_metadata)
    with pytest.raises(OSError, match="publication"):
        repo.get_measurements("M1", "2026-09-09", force_refresh=True)
    assert {p: p.read_bytes() for p in tmp_path.iterdir()} == originals
