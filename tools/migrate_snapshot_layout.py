"""Preview known snapshot moves; use --apply only after inspecting the manifest.

No database access, snapshot regeneration, or unknown-file deletion is performed.
Run while application/refresh writers are stopped. Paths in JSON are data-relative.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import re
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.shared_kernel.snapshot_paths import snapshot_component


@dataclass(frozen=True)
class SnapshotMove:
    source: str
    destination: str
    size_bytes: int
    mtime_ns: int


def _throughput_destination(path: Path, root: Path) -> str | None:
    """Read identity columns only; keep ambiguous/empty legacy files untouched."""
    import pandas as pd

    match = re.fullmatch(r"(spc|ctq|aoi_tt|aoi_rs)__([0-9a-f]{16})\.parquet", path.name)
    if not match:
        return None
    identity = pd.read_parquet(path, columns=["scope", "prod_code"]).drop_duplicates()
    if identity.empty:
        candidates = [
            entry.name for entry in root.iterdir()
            if entry.is_dir()
            and hashlib.sha256(entry.name.encode("utf-8")).hexdigest()[:16] == match[2]
        ]
        if len(candidates) != 1:
            return None
        product = snapshot_component(candidates[0])
        return f"inline_domain/{match[1]}/throughput_{product}.parquet"
    if len(identity) != 1:
        return None
    scope, product = str(identity.iloc[0]["scope"]), str(identity.iloc[0]["prod_code"])
    snapshot_component(product)
    digest = hashlib.sha256(product.encode("utf-8")).hexdigest()[:16]
    if scope != match[1] or digest != match[2]:
        raise ValueError(f"Legacy throughput identity mismatch: {path.name}")
    return f"inline_domain/{scope}/throughput_{product}.parquet"


def _destination(path: Path, root: Path) -> str | None:
    relative = path.relative_to(root)
    parts = relative.parts
    if len(parts) == 2:
        product, filename = parts
        if product == "equipment" and re.fullmatch(r"part_life_[A-Za-z0-9_]+\.parquet", filename):
            return f"equipment_domain/parts/{filename}"
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", product):
            return None
        if filename in {
            f"inline_measurements_{product}.{suffix}"
            for suffix in ("parquet", "policy", "snapshot.json")
        }:
            return f"inline_domain/shared/{filename}"
        if filename in {
            f"aoi_rs_{kind}_{product}.{suffix}"
            for kind in ("details", "pass_through")
            for suffix in ("parquet", "snapshot.json")
        }:
            return f"inline_domain/aoi_rs/{filename}"
        if re.fullmatch(rf"yield_snapshot_{re.escape(product)}(?:_[A-Za-z0-9-]+)?\.parquet", filename):
            return f"yield_domain/yield/{filename}"
    if parts[:2] == ("inline_domain", "throughput_history") and len(parts) == 3:
        return _throughput_destination(path, root)
    return None


def _snapshot_family(relative: str) -> tuple[str, str]:
    for suffix in (".snapshot.json", ".parquet", ".policy"):
        if relative.endswith(suffix):
            return relative[:-len(suffix)], suffix
    raise ValueError(f"Unknown snapshot suffix: {relative}")


def _preserve_conflicting_families(root: Path, moves: list[SnapshotMove]) -> list[SnapshotMove]:
    conflicts = {
        _snapshot_family(move.source)[0]
        for move in moves if (root / move.destination).exists()
    }
    replacements: dict[str, str] = {}
    for family in sorted(conflicts):
        primary = root / f"{family}.parquet"
        if not primary.is_file():
            primary = root / next(m.source for m in moves if _snapshot_family(m.source)[0] == family)
        with primary.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()[:12]
        replacements[family] = digest
    result = []
    for move in moves:
        family, suffix = _snapshot_family(move.source)
        if family in replacements:
            stem = Path(family).name
            destination = Path(move.destination).with_name(f"legacy_{stem}_{replacements[family]}{suffix}")
            move = replace(move, destination=destination.as_posix())
        result.append(move)
    return result


def build_manifest(data_root: Path, *, preserve_conflicts: bool = False) -> tuple[list[SnapshotMove], list[str]]:
    root = data_root.resolve(strict=True)
    moves: list[SnapshotMove] = []
    preserved: list[str] = []
    for path in sorted(root.rglob("*"), key=lambda p: p.as_posix()):
        if not path.is_file():
            continue
        # Never follow an alias outside the approved data tree.
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError(f"Snapshot path is an unsafe alias: {path.relative_to(root)}")
        destination = _destination(path, root)
        if destination is None:
            preserved.append(path.relative_to(root).as_posix())
            continue
        stat = path.stat()
        moves.append(SnapshotMove(path.relative_to(root).as_posix(), destination, stat.st_size, stat.st_mtime_ns))
    if preserve_conflicts:
        moves = _preserve_conflicting_families(root, moves)
    return moves, preserved


def _contained_path(root: Path, relative: str) -> Path:
    raw = Path(relative)
    path = root / raw
    if raw.is_absolute() or ".." in raw.parts or path.is_symlink():
        raise ValueError(f"Unsafe migration path: {relative}")
    if path.resolve() == root or not path.resolve().is_relative_to(root):
        raise ValueError(f"Migration path escapes data directory: {relative}")
    return path


def validate_manifest(data_root: Path, moves: list[SnapshotMove]) -> None:
    root = data_root.resolve(strict=True)
    sources: set[str] = set()
    destinations: set[str] = set()
    for move in moves:
        source = _contained_path(root, move.source)
        destination = _contained_path(root, move.destination)
        source_key, destination_key = str(source).casefold(), str(destination).casefold()
        if source_key in sources or destination_key in destinations:
            raise ValueError("Duplicate source or destination in migration plan")
        sources.add(source_key)
        destinations.add(destination_key)
        if not source.is_file():
            raise FileNotFoundError(source)
        if destination.exists():
            if destination.name.startswith("legacy_") and destination.is_file():
                with source.open("rb") as old, destination.open("rb") as saved:
                    identical = hashlib.file_digest(old, "sha256").digest() == hashlib.file_digest(saved, "sha256").digest()
                state = "identical" if identical else "different"
                raise FileExistsError(f"Legacy target already exists ({state} bytes); source retained: {move.destination}")
            raise FileExistsError(f"Refusing to overwrite snapshot: {move.destination}")
        stat = source.stat()
        if (stat.st_size, stat.st_mtime_ns) != (move.size_bytes, move.mtime_ns):
            raise ValueError(f"Snapshot changed since plan creation: {move.source}")
    if sources.intersection(destinations):
        raise ValueError("Migration cannot overwrite another planned source")


def apply_manifest(data_root: Path, moves: list[SnapshotMove]) -> None:
    """Preflight the entire plan before any move, never overwrite a destination."""
    root = data_root.resolve(strict=True)
    validate_manifest(root, moves)
    for move in moves:
        validate_manifest(root, [move])
        source = _contained_path(root, move.source)
        destination = _contained_path(root, move.destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            os.rename(source, destination)  # Windows refuses an existing destination.
        else:
            os.link(source, destination)  # Exclusive on POSIX; same filesystem.
            source.unlink()
    # Intentionally keep empty directories: no deletion is needed for correctness.


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=PROJECT_ROOT / "data")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--preserve-conflicts", action="store_true", help="Keep active targets; move entire old snapshot families under legacy_<stem>_<sha12> names. Existing legacy destinations still cause refusal.")
    args = parser.parse_args()
    moves, preserved = build_manifest(args.data_root, preserve_conflicts=args.preserve_conflicts)
    validation_error = None
    try:
        validate_manifest(args.data_root, moves)
    except (ValueError, OSError) as exc:
        validation_error = str(exc)
    print(json.dumps({"mode": "apply" if args.apply else "dry-run", "moves": [asdict(m) for m in moves], "preserved": preserved, "validation_error": validation_error}, ensure_ascii=False, indent=2))
    if validation_error:
        return 2
    if args.apply:
        apply_manifest(args.data_root, moves)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
