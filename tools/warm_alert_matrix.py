"""Compute today's full alert matrix and publish a cross-process status snapshot."""
from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
import sys
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[1]
for import_path in (REPO_ROOT, REPO_ROOT / "src"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from app.sections.inline_domain.monitor.alert_matrix_snapshot import DailyMatrixSnapshot

def main(
    argv: Sequence[str] | None = None,
    *,
    loader: Callable | None = None,
    store: DailyMatrixSnapshot | None = None,
    log_dir: Path | None = None,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Validate paths only; do not query data.")
    args = parser.parse_args(argv)
    target = store or DailyMatrixSnapshot()
    directory = log_dir or REPO_ROOT / "output/logs/alert_matrix_warmup"
    directory.mkdir(parents=True, exist_ok=True)
    target.path.parent.mkdir(parents=True, exist_ok=True)
    if args.check:
        return 0
    started = datetime.now()
    log_path = directory / f"{started:%Y%m%d-%H%M%S}-{uuid4().hex[:8]}.json"
    status = {"started_at": started.isoformat(), "status": "running"}
    log_path.write_text(json.dumps(status), encoding="utf-8")
    try:
        if loader is None:
            from app.sections.inline_domain.monitor.alert_matrix_cache import get_cached_alert_matrix
            loader = get_cached_alert_matrix
        payload = loader(reference_date=date.today(), reuse_snapshot=False)
        if not payload.get("cells"):
            raise ValueError("Empty alert matrix")
        target.write(payload)
        counts = dict(Counter(cell["state"] for cell in payload["cells"].values()))
        failures = [f"{row}|{product}" for (row, product), cell in payload["cells"].items()
                    if cell["state"] == "error"]
        status.update(status="partial" if failures else "success", counts=counts,
                      failed_cells=failures, snapshot=str(target.path))
        result = 1 if failures else 0
    except Exception as exc:
        # Do not persist driver messages, SQL or credentials in scheduler output.
        status.update(status="failed", error_type=type(exc).__name__)
        result = 2
    status["finished_at"] = datetime.now().isoformat()
    log_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    if sys.stdout is not None:
        print(json.dumps(status, ensure_ascii=False))
    return result


if __name__ == "__main__":
    os.chdir(REPO_ROOT)
    # Scheduled execution writes only the structured, non-sensitive run report.
    logging.basicConfig(handlers=[logging.NullHandler()], force=True)
    raise SystemExit(main())
