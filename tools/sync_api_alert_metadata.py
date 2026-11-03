"""Initialize the complete station dictionary or refresh selected station codes."""

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
    from src.shared_kernel.infrastructure.api_alert_metadata import (
        MAX_QUERY_ROWS, initialize, initialize_from_database, inspect_station_dictionary, sync_metadata,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--initialize", action="store_true", help="Fetch every station and derived layer after EXPLAIN scale checks; create/update the local dictionary atomically. Requires DB.")
    mode.add_argument("--template-only", action="store_true", help="Offline create/validate a header-only template; does not populate stations or access DB.")
    mode.add_argument("--step", action="append", default=[], help="Refresh exact station codes in an existing dictionary; repeat up to 1000 times.")
    parser.add_argument("--timeout-ms", type=int, default=10000, help="Per-query transaction-local timeout, 1..60000 milliseconds.")
    parser.add_argument("--max-rows", type=int, default=MAX_QUERY_ROWS, help="Source row cap, 1..10000; truncation is an error.")
    args = parser.parse_args(argv)
    if len(args.step) > 1000 or any(not step.strip() or len(step.strip()) > 128 for step in args.step):
        parser.error("Provide at most 1000 nonempty station codes, each at most 128 characters.")
    if not 1 <= args.timeout_ms <= 60000 or not 1 <= args.max_rows <= MAX_QUERY_ROWS:
        parser.error("Query timeout or row limit is outside the supported range.")
    try:
        path = metadata_path if metadata_path is not None else ConfigLoader.get_domain_resource_path("shared_kernel", "api_alert_metadata")
        created, count = False, 0
        if args.step:
            Path(path).stat()
        if args.initialize or args.step:
            if engine is None:
                from src.shared_kernel.infrastructure.db_handler import DatabaseManager

                engine = DatabaseManager().engine
            if engine is None:
                raise OSError("STATION_DICTIONARY_DATABASE_UNAVAILABLE")
        if args.initialize:
            created, count = initialize_from_database(path, engine, max_rows=args.max_rows, timeout_ms=args.timeout_ms)
        elif args.template_only:
            created = initialize(path)
        else:
            count = sync_metadata(path, args.step, engine, max_rows=args.max_rows, timeout_ms=args.timeout_ms)
        payload: dict[str, Any] = {"status": "success", "created": created, "stations_updated": count}
        if args.initialize or args.template_only:
            summary = inspect_station_dictionary(path)
            total = summary.station_count
            message = '站点字典已新建并通过校验。' if created else '站点字典已存在并通过校验，原文件已保留。'
            if args.initialize:
                message = '站点字典已完成全量初始化／更新，已保存站点、描述和层别，其它工作表及历史站点已保留。'
            elif total == 0:
                message += '当前仅有空模板；请使用 --initialize 从数据库初始化全部站点及层别。'
            if summary.stations_without_layer:
                message += '部分源描述无法提取层别，已保留原始记录并列出待维护站点。'
            payload.update(initialized=True, stations_total=total,
                           dictionary_state='populated' if total else 'empty',
                           layers_complete=not summary.stations_without_layer,
                           stations_without_layer=list(summary.stations_without_layer), message=message)
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    except (OSError, ValueError, KeyError) as exc:
        logging.getLogger(__name__).error("API_METADATA_MAINTENANCE_FAILED exception_type=%s", type(exc).__name__)
        print(json.dumps({"status": "error", "code": "API_METADATA_MAINTENANCE_FAILED"}))
        return 2


if __name__ == "__main__":
    # Keep Chinese messages valid JSON through Windows/uv redirected pipes.
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
