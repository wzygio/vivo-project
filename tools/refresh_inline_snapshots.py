"""Rebuild approved Inline raw snapshots after all application writers are stopped.

Default is a read-only plan. --apply --writers-stopped deletes only recognized
raw snapshot files and rebuilds through repository ports. No Excel is modified.
Run migrate_snapshot_layout.py before this command.
"""
from __future__ import annotations

import argparse
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import logging
from pathlib import Path
import re
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def deletion_plan(data_root: Path, products: list[str]) -> list[Path]:
    from src.shared_kernel.snapshot_paths import snapshot_component

    root = data_root.resolve(strict=True)
    inline = root / "inline_domain"
    if inline.is_symlink() or not inline.resolve().is_relative_to(root):
        raise ValueError("Unsafe Inline directory")
    targets = []
    for product in products:
        snapshot_component(product)
        names = {
            "shared": [f"inline_measurements_{product}.{suffix}" for suffix in ("parquet", "policy", "snapshot.json")],
            "aoi_rs": [f"aoi_rs_{kind}_{product}.{suffix}" for kind in ("details", "pass_through") for suffix in ("parquet", "snapshot.json")],
        }
        for module, filenames in names.items():
            module_dir = inline / module
            if module_dir.is_dir():
                stems = {name.removesuffix(".snapshot.json").removesuffix(".parquet").removesuffix(".policy") for name in filenames}
                legacy_pattern = re.compile(r"legacy_(?:" + "|".join(re.escape(stem) for stem in stems) + r")_[0-9a-f]{12}\.(?:parquet|policy|snapshot\.json)")
                filenames += [path.name for path in module_dir.iterdir() if legacy_pattern.fullmatch(path.name)]
            for filename in filenames:
                path = inline / module / filename
                if path.parent.is_symlink() or path.is_symlink() or not path.resolve().is_relative_to(inline.resolve()):
                    raise ValueError("Unsafe Inline snapshot path")
                if path.is_file():
                    targets.append(path)
    return sorted(set(targets))


def rebuild(products: list[str], end_date: str) -> list[dict]:
    # DB exceptions can contain SQL/credentials. Keep library diagnostics out of
    # console/files; return only per-product, per-module success booleans.
    results = []
    prior_disable = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            from src.inline_domain.composition import refresh_aoi_rs_snapshots, refresh_raw_measurements
            from src.shared_kernel.infrastructure.db_handler import DatabaseManager

            try:
                db = DatabaseManager()
            except Exception:
                return [{"product": product, "module": module, "success": False, "reason": "database_initialization_failed"}
                        for product in products for module in ("shared", "aoi_rs")]
            for product in products:
                for module, refresh in (("shared", refresh_raw_measurements), ("aoi_rs", refresh_aoi_rs_snapshots)):
                    try:
                        success = bool(refresh(db, product, end_date))
                    except Exception:
                        success = False
                    results.append({"product": product, "module": module, "success": success})
    finally:
        logging.disable(prior_disable)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--products", nargs="+", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--writers-stopped", action="store_true")
    args = parser.parse_args()
    from datetime import date
    date.fromisoformat(args.end_date)
    root = PROJECT_ROOT / "data"
    products = list(dict.fromkeys(args.products))
    targets = deletion_plan(root, products)
    print(json.dumps({"mode": "apply" if args.apply else "dry-run", "delete": [str(p.relative_to(root)) for p in targets], "products": products, "end_date": args.end_date}, ensure_ascii=False), flush=True)
    if not args.apply:
        return 0
    if not args.writers_stopped:
        parser.error("--apply requires --writers-stopped; stop application/refresh writers first")
    # Reject any remaining legacy raw source files: migration must finish first.
    for directory in root.iterdir():
        if directory.is_dir() and re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", directory.name):
            if any(directory.glob("inline_measurements_*.parquet")) or any(directory.glob("aoi_rs_*.parquet")):
                parser.error("Legacy raw snapshots remain; run layout migration first")
    for path in targets:
        path.unlink()
    results = rebuild(products, args.end_date)
    print(json.dumps({"results": results}, ensure_ascii=False), flush=True)
    return 0 if results and all(row["success"] for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
