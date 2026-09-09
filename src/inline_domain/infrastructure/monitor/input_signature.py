"""Cheap dependency probes for configuration-driven Inline computations."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from pathlib import Path


def monitor_input_signature(
    project_root: Path,
    resource_root: Path,
    products: Iterable[str],
    *,
    excluded_paths: Iterable[Path] = (),
) -> str:
    """Track source snapshots and editable inputs, excluding generated summaries."""
    excluded = {
        Path(path).resolve()
        for path in excluded_paths
    }
    paths: set[Path] = set()
    for root in (resource_root, project_root / "config"):
        if root.exists():
            paths.update(
                path for path in root.rglob("*")
                if path.is_file()
                and path.suffix.lower() in {".xlsx", ".yaml", ".yml", ".csv", ".json"}
                and not path.name.startswith(("~$", "."))
                and path.resolve() not in excluded
            )
    for product in sorted(set(products)):
        root = project_root / "data" / product
        if root.exists():
            paths.update(root.glob("inline_measurements*"))
            paths.update(root.glob("aoi_rs_*"))
    parts = []
    for path in sorted(paths):
        stat = path.stat()
        parts.append(f"{path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
