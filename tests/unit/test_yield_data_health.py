from types import SimpleNamespace

import pandas as pd
import pytest

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig
from src.yield_domain.infrastructure import data_loader
from src.yield_domain.infrastructure.repositories import yield_repository as module


@pytest.fixture
def repository(tmp_path, monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_data_forward_policy", lambda: DataForwardPolicy(enabled=False))
    return module.PanelRepository(tmp_path / "snapshot.parquet", YieldDataPolicy(work_order_types=("P",), target_defect_groups=()), db_manager=SimpleNamespace(engine=object()))


def query():
    return YieldQueryConfig(start_date="2026-06-01", end_date="2026-08-01", product_code="TEST")


def row():
    return pd.DataFrame({"panel_id": ["OLD"], "warehousing_time": [pd.Timestamp("2026-06-05")]})


def test_loader_failure_is_safe_and_not_empty(monkeypatch, caplog):
    def fail(*args, **kwargs):
        raise RuntimeError("SECRET SQL password=test")
    monkeypatch.setattr(data_loader.pd, "read_sql", fail)
    from src.yield_domain.application.errors import YieldSourceReadError
    with pytest.raises(YieldSourceReadError, match="YIELD_SOURCE_UNAVAILABLE") as error:
        data_loader.load_panel_details(SimpleNamespace(engine=object()), "2026-01-01", "2026-01-02", "TEST", ("P",))
    assert "SECRET" not in str(error.value)
    assert "SECRET" not in caplog.text


def test_second_chunk_failure_preserves_snapshot_on_force_refresh(repository, monkeypatch):
    row().to_parquet(repository.snapshot_path)
    original = repository.snapshot_path.read_bytes()
    calls = []
    def fetch(*args):
        calls.append(args)
        if len(calls) == 2:
            raise RuntimeError("private SQL")
        return row().assign(panel_id="NEW")
    monkeypatch.setattr(module, "load_panel_details", fetch)
    result = repository.get_panel_details(query(), force_refresh=True)
    assert result.panel_id.tolist() == ["OLD"]
    assert result.attrs["data_health"]["status"] == "stale"
    assert repository.snapshot_path.read_bytes() == original


def test_no_snapshot_failure_is_unavailable(repository, monkeypatch):
    monkeypatch.setattr(module, "load_panel_details", lambda *args: (_ for _ in ()).throw(RuntimeError("private")))
    result = repository.get_panel_details(query())
    assert result.empty
    assert result.attrs["data_health"]["status"] == "unavailable"
    assert not repository.snapshot_path.exists()


def test_empty_success_is_fresh_and_reusable(repository, monkeypatch):
    calls = []
    monkeypatch.setattr(module, "load_panel_details", lambda *args: calls.append(args) or pd.DataFrame())
    result = repository.get_panel_details(query())
    assert result.empty
    assert result.attrs["data_health"]["status"] == "fresh"
    assert result.attrs["data_health"]["source_start"] == "2026-06-01"
    calls.clear()
    repeated = repository.get_panel_details(query())
    assert repeated.empty
    assert repeated.attrs["data_health"] == result.attrs["data_health"]
    assert calls == []


def test_incremental_failure_keeps_original_health_and_bytes(repository, monkeypatch):
    monkeypatch.setattr(module, "load_panel_details", lambda *args: row())
    fresh = repository.get_panel_details(query())
    before = repository.snapshot_path.read_bytes()
    repository.SNAPSHOT_TTL_HOURS = -1
    monkeypatch.setattr(module, "load_panel_details", lambda *args: (_ for _ in ()).throw(RuntimeError("private")))
    stale = repository.get_panel_details(query())
    assert stale.attrs["data_health"]["status"] == "stale"
    assert stale.attrs["data_health"]["refreshed_at"] == fresh.attrs["data_health"]["refreshed_at"]
    assert repository.snapshot_path.read_bytes() == before


def test_source_health_survives_display_shift(repository, monkeypatch):
    repository.data_forward_policy = DataForwardPolicy(enabled=True, offset_days=4)
    windows = []
    def fetch(db, start, end, *args):
        windows.append((start, end))
        return row()
    monkeypatch.setattr(module, "load_panel_details", fetch)
    result = repository.get_panel_details(query())
    assert windows[0][0] == "2026-05-28"
    assert windows[-1][1] == "2026-07-28"
    assert result.warehousing_time.iloc[0] == pd.Timestamp("2026-06-09")
    assert result.attrs["data_health"]["source_start"] == "2026-05-28"
    assert pd.read_parquet(repository.snapshot_path).warehousing_time.iloc[0] == pd.Timestamp("2026-06-05")
