"""Refresh the shared rolling Q-Time snapshots for all shops."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from dataclasses import asdict
from datetime import date
import json
import logging
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
for import_path in (REPO_ROOT, REPO_ROOT / "src"):
    path_text = str(import_path)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)

from src.indicator_domain.application.qtime.service import QTimeReportService
from src.indicator_domain.composition import build_qtime_service
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)


def _build_service() -> QTimeReportService:
    return build_qtime_service(DatabaseManager())


def main(
    argv: Sequence[str] | None = None,
    *,
    service_factory: Callable[[], QTimeReportService] | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        description="Incrementally refresh all shared Q-Time shop snapshots.",
    )
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        help="Report date in YYYY-MM-DD format; defaults to today.",
    )
    parser.add_argument("--full", action="store_true", help="Reload the full rolling window, including historical specification changes.")
    args = parser.parse_args(argv)

    try:
        service = (service_factory or _build_service)()
        results = service.refresh_snapshots(
            as_of=args.as_of, **({"full_refresh": True} if args.full else {}),
        )
    except Exception:
        logger.exception("Q-Time snapshot refresh failed")
        print(
            json.dumps(
                {
                    "status": "error",
                    "message": "Q-Time snapshot refresh failed; inspect the log.",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2

    is_fresh = all(result.refreshed_from_database for result in results)
    payload = {
        "status": "success" if is_fresh else "stale-fallback",
        "as_of": (args.as_of or date.today()).isoformat(),
        "snapshots": [asdict(result) for result in results],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if is_fresh else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())

