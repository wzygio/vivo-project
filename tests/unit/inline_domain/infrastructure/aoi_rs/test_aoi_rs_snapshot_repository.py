"""AOI_RS 产品级本地快照仓储行为测试。"""

from pathlib import Path
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
import json
import os
import threading

import pandas as pd
import pytest

from src.shared_kernel.data_forward import DataForwardPolicy
from src.inline_domain.infrastructure.shared.snapshot_window import IncompleteMonitorSnapshotError

from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.infrastructure.aoi_rs.snapshot_repository import AoiRsSnapshotRepository


@pytest.fixture(autouse=True)
def fixed_display_policy(monkeypatch):
    monkeypatch.setattr(
        "src.shared_kernel.config.ConfigLoader.get_data_forward_policy",
        lambda: DataForwardPolicy(enabled=True, offset_days=4),
    )


def _query() -> AoiRsQueryConfig:
    return AoiRsQueryConfig(
        prod_code="M678",
        start_date="2026-07-01",
        end_date="2026-08-10",
    )


def _details() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-08-06 08:00:00"),
                "sheet_id": "SHT-A01",
                "lot_id": "LOT-A1",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "code_qty": 3,
            }
        ]
    )


def _pass_through() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-08-06 07:00:00"),
                "sheet_id": "SHT-A01",
                "lot_id": "LOT-A1",
                "step_id": "11629",
            }
        ]
    )


def test_rs_details_reuse_fresh_product_snapshot_without_reloading_database(
    tmp_path: Path,
) -> None:
    loader_calls: list[AoiRsQueryConfig] = []

    def load_details(_db_manager: object, query: AoiRsQueryConfig) -> pd.DataFrame:
        loader_calls.append(query)
        return _details()

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=load_details,
    )

    first = repository.get_rs_details(_query())
    second = repository.get_rs_details(_query())

    assert loader_calls == [loader_calls[0]]
    assert loader_calls[0].start_date == "2026-05-01"
    assert loader_calls[0].end_date == "2026-08-10"
    pd.testing.assert_frame_equal(first, second)
    assert first.loc[0, "start_time"] == pd.Timestamp("2026-08-10 08:00:00")
    snapshot_path = tmp_path / "aoi_rs_details_M678.parquet"
    assert pd.read_parquet(snapshot_path).loc[0, "start_time"] == pd.Timestamp(
        "2026-08-06 08:00:00"
    )


def test_pass_through_reuses_its_own_fresh_product_snapshot(
    tmp_path: Path,
) -> None:
    loader_calls: list[AoiRsQueryConfig] = []

    def load_pass_through(_db_manager: object, query: AoiRsQueryConfig) -> pd.DataFrame:
        loader_calls.append(query)
        return _pass_through()

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        pass_through_loader=load_pass_through,
    )

    first = repository.get_pass_through(_query())
    second = repository.get_pass_through(_query())

    assert len(loader_calls) == 1
    pd.testing.assert_frame_equal(first, second)
    assert (tmp_path / "aoi_rs_pass_through_M678.parquet").exists()


def test_corrupt_fresh_snapshot_is_reloaded_from_database(tmp_path: Path) -> None:
    loader_calls = 0

    def load_details(_db_manager: object, _query: AoiRsQueryConfig) -> pd.DataFrame:
        nonlocal loader_calls
        loader_calls += 1
        return _details()

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=load_details,
    )
    repository.get_rs_details(_query())
    (tmp_path / "aoi_rs_details_M678.parquet").write_bytes(b"not parquet")

    recovered = repository.get_rs_details(_query())

    assert loader_calls == 2
    assert len(recovered) == 1
    assert pd.read_parquet(tmp_path / "aoi_rs_details_M678.parquet").shape[0] == 1


def test_explicit_refresh_failure_preserves_both_existing_snapshots(tmp_path: Path) -> None:
    should_fail = False

    def load_details(_db_manager: object, _query: AoiRsQueryConfig) -> pd.DataFrame:
        if should_fail:
            raise RuntimeError("database unavailable")
        return _details()

    def load_pass_through(_db_manager: object, _query: AoiRsQueryConfig) -> pd.DataFrame:
        return pd.DataFrame() if should_fail else _pass_through()

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=load_details,
        pass_through_loader=load_pass_through,
    )
    expected_details = repository.get_rs_details(_query())
    expected_pass_through = repository.get_pass_through(_query())
    should_fail = True

    assert repository.refresh(_query()) is False
    pd.testing.assert_frame_equal(repository.get_rs_details(_query()), expected_details)
    pd.testing.assert_frame_equal(
        repository.get_pass_through(_query()), expected_pass_through
    )


def test_snapshot_without_coverage_metadata_is_reloaded(tmp_path: Path) -> None:
    loader_calls = 0

    def load_details(_db_manager: object, _query: AoiRsQueryConfig) -> pd.DataFrame:
        nonlocal loader_calls
        loader_calls += 1
        return _details()

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=load_details,
    )
    repository.get_rs_details(_query())
    metadata_path = tmp_path / "aoi_rs_details_M678.snapshot.json"
    metadata_path.write_text(
        json.dumps({"policy_version": repository.SNAPSHOT_POLICY_VERSION}),
        encoding="utf-8",
    )

    repository.get_rs_details(_query())

    assert loader_calls == 2


def test_expired_policy_or_insufficient_coverage_reloads_snapshot(tmp_path: Path) -> None:
    loader_calls = 0

    def load_details(_db_manager: object, _query: AoiRsQueryConfig) -> pd.DataFrame:
        nonlocal loader_calls
        loader_calls += 1
        return _details()

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=load_details,
    )
    repository.get_rs_details(_query())
    metadata_path = tmp_path / "aoi_rs_details_M678.snapshot.json"
    metadata_path.write_text(
        json.dumps(
            {
                "policy_version": "old-policy",
                "covered_through": "2026-08-10",
            }
        ),
        encoding="utf-8",
    )
    repository.get_rs_details(_query())

    metadata_path.write_text(
        json.dumps(
            {
                "policy_version": repository.SNAPSHOT_POLICY_VERSION,
                "covered_through": "2026-08-09",
            }
        ),
        encoding="utf-8",
    )
    repository.get_rs_details(_query())

    assert loader_calls == 3


def test_snapshot_older_than_ttl_is_reloaded(tmp_path: Path) -> None:
    loader_calls = 0

    def load_details(_db_manager: object, _query: AoiRsQueryConfig) -> pd.DataFrame:
        nonlocal loader_calls
        loader_calls += 1
        return _details()

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=load_details,
    )
    repository.get_rs_details(_query())
    metadata_path = tmp_path / "aoi_rs_details_M678.snapshot.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["refreshed_at"] = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=repository.SNAPSHOT_TTL_HOURS + 1)).isoformat()
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    repository.get_rs_details(_query())

    assert loader_calls == 2


def test_database_failure_falls_back_to_existing_snapshot(tmp_path: Path) -> None:
    should_fail = False

    def load_details(_db_manager: object, _query: AoiRsQueryConfig) -> pd.DataFrame:
        if should_fail:
            raise RuntimeError("database unavailable")
        return _details()

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=load_details,
    )
    expected = repository.get_rs_details(_query())
    metadata_path = tmp_path / "aoi_rs_details_M678.snapshot.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["refreshed_at"] = "2020-01-01T00:00:00+00:00"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    should_fail = True

    actual = repository.get_rs_details(_query())

    pd.testing.assert_frame_equal(actual, expected)


def test_database_failure_without_snapshot_reports_unavailable(tmp_path: Path) -> None:
    def fail(*_args: object) -> pd.DataFrame:
        raise RuntimeError("database unavailable")

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=fail,
        pass_through_loader=fail,
    )

    for method in (repository.get_rs_details, repository.get_pass_through):
        with pytest.raises(IncompleteMonitorSnapshotError, match="原始数据不可用"):
            method(_query())
    assert not list(tmp_path.glob("*.parquet"))


def test_snapshot_filters_rolling_data_to_requested_page_window(tmp_path: Path) -> None:
    rolling = pd.concat(
        [
            _details().assign(start_time=pd.Timestamp("2026-06-01 08:00:00")),
            _details(),
        ],
        ignore_index=True,
    )
    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=lambda *_args: rolling,
    )

    result = repository.get_rs_details(_query())

    assert result["start_time"].tolist() == [pd.Timestamp("2026-08-10 08:00:00")]


def test_concurrent_cold_reads_share_one_database_load(tmp_path: Path) -> None:
    loader_calls = 0
    calls_lock = threading.Lock()

    def load_details(_db_manager: object, _query: AoiRsQueryConfig) -> pd.DataFrame:
        nonlocal loader_calls
        with calls_lock:
            loader_calls += 1
        return _details()

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=load_details,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: repository.get_rs_details(_query()), range(2)))

    assert loader_calls == 1
    pd.testing.assert_frame_equal(results[0], results[1])
    assert list(tmp_path.glob("*.tmp")) == []


def test_snapshot_missing_contract_columns_is_reloaded(tmp_path: Path) -> None:
    loader_calls = 0

    def load_details(_db_manager: object, _query: AoiRsQueryConfig) -> pd.DataFrame:
        nonlocal loader_calls
        loader_calls += 1
        return _details()

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=load_details,
    )
    repository.get_rs_details(_query())
    pd.DataFrame({"start_time": [pd.Timestamp("2026-08-10")] }).to_parquet(
        tmp_path / "aoi_rs_details_M678.parquet",
        index=False,
    )

    recovered = repository.get_rs_details(_query())

    assert loader_calls == 2
    assert list(recovered.columns) == list(_details().columns)


@pytest.mark.parametrize("method,prefix,frame", [
    ("get_rs_details", "aoi_rs_details", _details),
    ("get_pass_through", "aoi_rs_pass_through", _pass_through),
])
def test_annual_read_rejects_old_short_fallback(tmp_path, method, prefix, frame):
    snapshot = tmp_path / f"{prefix}_M678.parquet"
    frame().to_parquet(snapshot, index=False)
    snapshot.with_suffix(".snapshot.json").write_text(
        json.dumps({"policy_version": "old-policy", "covered_through": "2026-08-10"}),
        encoding="utf-8",
    )

    def fail(*args):
        raise RuntimeError("database unavailable")

    repository = AoiRsSnapshotRepository(
        tmp_path, object(), details_loader=fail, pass_through_loader=fail,
    )
    query = _query().model_copy(update={"start_date": "2026-01-01"})
    with pytest.raises(IncompleteMonitorSnapshotError, match="未覆盖"):
        getattr(repository, method)(query)
    assert snapshot.exists()


def test_annual_snapshot_metadata_covers_no_fact_months(tmp_path):
    repository = AoiRsSnapshotRepository(
        tmp_path, object(), details_loader=lambda *args: _details(),
    )
    repository.get_rs_details(_query())
    metadata = json.loads((tmp_path / "aoi_rs_details_M678.snapshot.json").read_text())
    assert metadata["covered_from"] == "2026-05-01"


@pytest.mark.parametrize("method,prefix", [
    ("get_rs_details", "aoi_rs_details"),
    ("get_pass_through", "aoi_rs_pass_through"),
])
def test_covered_but_corrupt_annual_snapshot_cannot_be_zero_fallback(tmp_path, method, prefix):
    snapshot = tmp_path / f"{prefix}_M678.parquet"
    snapshot.write_bytes(b"not parquet")
    snapshot.with_suffix(".snapshot.json").write_text(json.dumps({
        "policy_version": AoiRsSnapshotRepository.SNAPSHOT_POLICY_VERSION,
        "covered_from": "2026-01-01", "covered_through": "2026-08-10",
    }), encoding="utf-8")

    def fail(*args):
        raise RuntimeError("database unavailable")

    repository = AoiRsSnapshotRepository(
        tmp_path, object(), details_loader=fail, pass_through_loader=fail,
    )
    query = _query().model_copy(update={"start_date": "2026-01-01"})
    with pytest.raises(IncompleteMonitorSnapshotError, match="损坏"):
        getattr(repository, method)(query)


@pytest.mark.parametrize("end_date", ["2027-01-07", "2027-02-07"])
@pytest.mark.parametrize("corrupt", [False, True])
@pytest.mark.parametrize("method,prefix", [
    ("get_rs_details", "aoi_rs_details"),
    ("get_pass_through", "aoi_rs_pass_through"),
])
def test_explicit_monitor_coverage_is_required_in_early_year(
    tmp_path, end_date, corrupt, method, prefix,
):
    if corrupt:
        snapshot = tmp_path / f"{prefix}_M678.parquet"
        snapshot.write_bytes(b"corrupt parquet")
        snapshot.with_suffix(".snapshot.json").write_text(json.dumps({
            "policy_version": AoiRsSnapshotRepository.SNAPSHOT_POLICY_VERSION,
            "covered_from": "2026-10-01", "covered_through": end_date,
        }), encoding="utf-8")

    def fail(*args):
        raise RuntimeError("database unavailable")

    repository = AoiRsSnapshotRepository(
        tmp_path, object(), details_loader=fail, pass_through_loader=fail,
        require_complete_coverage=True,
    )
    query = AoiRsQueryConfig(prod_code="M678", start_date="2027-01-01", end_date=end_date)
    with pytest.raises(IncompleteMonitorSnapshotError):
        getattr(repository, method)(query)


def test_ordinary_report_does_not_turn_failure_into_empty_result(tmp_path):
    def fail(*args):
        raise RuntimeError("database unavailable")

    repository = AoiRsSnapshotRepository(
        tmp_path, object(), details_loader=fail, pass_through_loader=fail,
        require_complete_coverage=False,
    )
    query = AoiRsQueryConfig(prod_code="M678", start_date="2027-01-01", end_date="2027-02-07")
    for method in (repository.get_rs_details, repository.get_pass_through):
        with pytest.raises(IncompleteMonitorSnapshotError, match="原始数据不可用"):
            method(query)
