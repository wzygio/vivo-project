"""AOI_RS 产品级本地快照仓储行为测试。"""

from pathlib import Path
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
import json
import os
import threading

import pandas as pd

from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.infrastructure.aoi_rs.snapshot_repository import AoiRsSnapshotRepository


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
    snapshot_path = tmp_path / "aoi_rs_details_M678.parquet"
    expired = snapshot_path.stat().st_mtime - (repository.SNAPSHOT_TTL_HOURS + 1) * 3600
    os.utime(snapshot_path, (expired, expired))

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
    metadata_path.write_text("{}", encoding="utf-8")
    should_fail = True

    actual = repository.get_rs_details(_query())

    pd.testing.assert_frame_equal(actual, expected)


def test_database_failure_without_snapshot_returns_contract_empty_frames(tmp_path: Path) -> None:
    def fail(*_args: object) -> pd.DataFrame:
        raise RuntimeError("database unavailable")

    repository = AoiRsSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        details_loader=fail,
        pass_through_loader=fail,
    )

    details = repository.get_rs_details(_query())
    pass_through = repository.get_pass_through(_query())

    assert list(details.columns) == [
        "factory", "prod_code", "start_time", "sheet_id", "lot_id", "step_id",
        "rs_code", "code_qty",
    ]
    assert list(pass_through.columns) == [
        "factory", "prod_code", "start_time", "sheet_id", "lot_id", "step_id",
    ]
    assert details.empty and pass_through.empty


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
