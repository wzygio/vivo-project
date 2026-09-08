from dataclasses import replace
import hashlib
from pathlib import Path

import pandas as pd
import pytest

from src.shared_kernel.snapshot_paths import (
    aoi_rs_snapshot_directory,
    inline_measurement_directory,
    snapshot_component,
)
from src.inline_domain.infrastructure.shared.throughput_history_store import ThroughputHistoryStore
from tools.migrate_snapshot_layout import apply_manifest, build_manifest


def _create(root: Path, relative: str, content: bytes = b"snapshot") -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_domain_layout_helpers(tmp_path):
    assert inline_measurement_directory(tmp_path) == tmp_path / "inline_domain/shared"
    assert aoi_rs_snapshot_directory(tmp_path) == tmp_path / "inline_domain/aoi_rs"
    assert ThroughputHistoryStore(tmp_path / "inline_domain").snapshot_path("SPC", "M626") == tmp_path / "inline_domain/spc/throughput_M626.parquet"


@pytest.mark.parametrize("value", ["../M626", "M626/x", "M626\\x", "C:", "", ".."])
def test_reject_unsafe_product(value):
    with pytest.raises(ValueError):
        snapshot_component(value)


def test_preview_and_apply_preserve_bytes_unknown_files_and_qtime(tmp_path):
    mappings = {
        "M626/inline_measurements_M626.parquet": "inline_domain/shared/inline_measurements_M626.parquet",
        "M626/inline_measurements_M626.policy": "inline_domain/shared/inline_measurements_M626.policy",
        "M626/inline_measurements_M626.snapshot.json": "inline_domain/shared/inline_measurements_M626.snapshot.json",
        "M626/aoi_rs_details_M626.parquet": "inline_domain/aoi_rs/aoi_rs_details_M626.parquet",
        "M626/aoi_rs_pass_through_M626.snapshot.json": "inline_domain/aoi_rs/aoi_rs_pass_through_M626.snapshot.json",
        "M626/yield_snapshot_M626_abc123.parquet": "yield_domain/yield/yield_snapshot_M626_abc123.parquet",
        "equipment/part_life_snapshot_abc.parquet": "equipment_domain/parts/part_life_snapshot_abc.parquet",
    }
    for source in mappings:
        _create(tmp_path, source)
    unknown = ["custom folder/user.txt", "M626/notes.txt", "indicator_domain/qtime/qtime.parquet"]
    for relative in unknown:
        _create(tmp_path, relative, b"keep")
    moves, preserved = build_manifest(tmp_path)
    assert {m.source: m.destination for m in moves} == mappings
    assert set(preserved) == set(unknown)
    assert all((tmp_path / source).exists() for source in mappings)
    assert build_manifest(tmp_path) == (moves, preserved)
    apply_manifest(tmp_path, moves)
    for source, destination in mappings.items():
        assert not (tmp_path / source).exists()
        assert (tmp_path / destination).read_bytes() == b"snapshot"
    assert all((tmp_path / relative).read_bytes() == b"keep" for relative in unknown)
    assert build_manifest(tmp_path)[0] == []


def test_collision_rejected_before_any_move(tmp_path):
    _create(tmp_path, "M626/inline_measurements_M626.parquet")
    _create(tmp_path, "M678/inline_measurements_M678.parquet")
    moves, _ = build_manifest(tmp_path)
    _create(tmp_path, moves[-1].destination, b"existing")
    with pytest.raises(FileExistsError):
        apply_manifest(tmp_path, moves)
    assert all((tmp_path / m.source).exists() for m in moves)
    assert (tmp_path / moves[-1].destination).read_bytes() == b"existing"


def test_modified_source_and_escape_rejected(tmp_path):
    source = _create(tmp_path, "M626/inline_measurements_M626.parquet")
    moves, _ = build_manifest(tmp_path)
    with pytest.raises(ValueError, match="Unsafe"):
        apply_manifest(tmp_path, [replace(moves[0], destination="../outside.parquet")])
    source.write_bytes(b"changed snapshot")
    with pytest.raises(ValueError, match="changed"):
        apply_manifest(tmp_path, moves)
    assert source.exists()


def test_hashed_throughput_identity_migration(tmp_path):
    digest = hashlib.sha256(b"M626").hexdigest()[:16]
    path = tmp_path / f"inline_domain/throughput_history/spc__{digest}.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame({"scope": ["spc"], "prod_code": ["M626"]}).to_parquet(path)
    moves, _ = build_manifest(tmp_path)
    assert moves[0].destination == "inline_domain/spc/throughput_M626.parquet"
    apply_manifest(tmp_path, moves)
    assert pd.read_parquet(tmp_path / moves[0].destination).iloc[0]["prod_code"] == "M626"


def test_mismatched_throughput_identity_rejected(tmp_path):
    path = tmp_path / "inline_domain/throughput_history/spc__0000000000000000.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame({"scope": ["spc"], "prod_code": ["M626"]}).to_parquet(path)
    with pytest.raises(ValueError, match="identity mismatch"):
        build_manifest(tmp_path)


def test_nested_and_hashed_throughput_collision_preserves_both(tmp_path):
    digest = hashlib.sha256(b"M626").hexdigest()[:16]
    old = tmp_path / f"inline_domain/throughput_history/spc__{digest}.parquet"
    old.parent.mkdir(parents=True)
    pd.DataFrame({"scope": ["spc"], "prod_code": ["M626"]}).to_parquet(old)
    nested = _create(tmp_path, "inline_domain/throughput_history/spc/throughput_M626.parquet", b"new")
    moves, _ = build_manifest(tmp_path, preserve_conflicts=True)
    apply_manifest(tmp_path, moves)
    assert (tmp_path / "inline_domain/spc/throughput_M626.parquet").read_bytes() == b"new"
    assert len(list((tmp_path / "inline_domain/spc").glob("legacy_*.parquet"))) == 1
    assert not old.exists() and not nested.exists()


def test_empty_throughput_uses_known_product_hash_and_preserves_unknown(tmp_path):
    (tmp_path / "M626").mkdir()
    legacy = tmp_path / "inline_domain/throughput_history"
    legacy.mkdir(parents=True)
    digest = hashlib.sha256(b"M626").hexdigest()[:16]
    for token in [digest, "0000000000000000"]:
        pd.DataFrame(columns=["scope", "prod_code"]).to_parquet(legacy / f"spc__{token}.parquet")
    moves, preserved = build_manifest(tmp_path)
    assert len(moves) == 1
    assert moves[0].destination == "inline_domain/spc/throughput_M626.parquet"
    assert preserved == ["inline_domain/throughput_history/spc__0000000000000000.parquet"]


def test_explicit_conflict_preservation_keeps_both_versions(tmp_path):
    source = "equipment/part_life_snapshot_abc.parquet"
    target = "equipment_domain/parts/part_life_snapshot_abc.parquet"
    _create(tmp_path, source, b"old snapshot")
    _create(tmp_path, target, b"current snapshot")
    moves, _ = build_manifest(tmp_path, preserve_conflicts=True)
    digest = hashlib.sha256(b"old snapshot").hexdigest()[:12]
    assert moves[0].destination == f"equipment_domain/parts/legacy_part_life_snapshot_abc_{digest}.parquet"
    apply_manifest(tmp_path, moves)
    assert (tmp_path / target).read_bytes() == b"current snapshot"
    assert (tmp_path / moves[0].destination).read_bytes() == b"old snapshot"
    assert not (tmp_path / source).exists()
    assert len(list((tmp_path / "equipment_domain/parts").glob("part_life_*.parquet"))) == 1


def test_conflict_preserves_metadata_family_and_refuses_existing_legacy(tmp_path):
    for suffix in [".parquet", ".policy", ".snapshot.json"]:
        _create(tmp_path, f"M626/inline_measurements_M626{suffix}", b"old")
    _create(tmp_path, "inline_domain/shared/inline_measurements_M626.parquet", b"new")
    moves, _ = build_manifest(tmp_path, preserve_conflicts=True)
    digest = hashlib.sha256(b"old").hexdigest()[:12]
    assert all(f"legacy_inline_measurements_M626_{digest}" in m.destination for m in moves)
    _create(tmp_path, moves[-1].destination, b"old")
    with pytest.raises(FileExistsError):
        apply_manifest(tmp_path, moves)
    assert all((tmp_path / m.source).exists() for m in moves)
