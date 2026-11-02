"""Explicitly initialize API metadata or synchronize an allowlisted station set."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
import logging
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
for import_path in (ROOT, ROOT / "src"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))


def main(
    argv: Sequence[str] | None = None, *, engine: Any = None, metadata_path: Path | None = None,
) -> int:
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure.api_alert_metadata import MAX_QUERY_ROWS, initialize, sync_metadata

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--initialize", action="store_true", help="Create header-only metadata; existing files are preserved. No DB required.")
    parser.add_argument("--step", action="append", default=[], help="Exact station code; repeat up to 1000 times. No unbounded dictionary query.")
    parser.add_argument("--timeout-ms", type=int, default=10000, help="Per-query transaction-local timeout, 1..60000 milliseconds.")
    parser.add_argument("--max-rows", type=int, default=MAX_QUERY_ROWS, help="Source row cap, 1..10000; truncation is an error.")
    args = parser.parse_args(argv)
    if not args.initialize and not args.step:
        parser.error("Use --initialize or at least one --step.")
    if len(args.step) > 1000 or any(not step.strip() or len(step.strip()) > 128 for step in args.step):
        parser.error("Provide at most 1000 nonempty station codes, each at most 128 characters.")
    if not 1 <= args.timeout_ms <= 60000 or not 1 <= args.max_rows <= MAX_QUERY_ROWS:
        parser.error("Query timeout or row limit is outside the supported range.")
    try:
        path = metadata_path if metadata_path is not None else ConfigLoader.get_domain_resource_path("shared_kernel", "api_alert_metadata")
        created = initialize(path) if args.initialize else False
        count = 0
        if args.step:
            Path(path).stat()
            if engine is None:
                from src.shared_kernel.infrastructure.db_handler import DatabaseManager

                engine = DatabaseManager().engine
            if engine is None:
                raise OSError("STATION_DICTIONARY_DATABASE_UNAVAILABLE")
            count = sync_metadata(path, args.step, engine, max_rows=args.max_rows, timeout_ms=args.timeout_ms)
        print(json.dumps({"status": "success", "created": created, "stations_updated": count}))
        return 0
    except (OSError, ValueError, KeyError) as exc:
        logging.getLogger(__name__).error("API_METADATA_MAINTENANCE_FAILED exception_type=%s", type(exc).__name__)
        print(json.dumps({"status": "error", "code": "API_METADATA_MAINTENANCE_FAILED"}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
