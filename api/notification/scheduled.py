"""Noninteractive mail job, gated by complete current-day inspection evidence."""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
import json
import logging
import os
from pathlib import Path
import sys
from typing import Callable
from uuid import uuid4

from api.inspection.application import atomic_report
from api.inspection.scheduling import GateError, ROOT, ZONE, wait_for_inspection
from src.shared_kernel.infrastructure.file_lock import publication_lock


def enabled_products() -> tuple[str, ...]:
    from src.shared_kernel.config import ConfigLoader
    return tuple(ConfigLoader.get_enabled_products(strict=True))


def _write_summary(report: dict) -> None:
    if sys.stdout is not None:
        print(json.dumps(report, ensure_ascii=False))


def main(argv: list[str] | None = None, *, sender: Callable[[list[str]], int] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, default=ROOT / 'output/logs/alert_inspection/latest.json')
    parser.add_argument('--log-dir', type=Path, default=ROOT / 'output/logs/alert_notification')
    parser.add_argument('--env-file', type=Path, default=ROOT / '.env')
    parser.add_argument('--mail-config', type=Path)
    parser.add_argument('--wait-seconds', type=float, default=7200)
    parser.add_argument('--test-mode', action='store_true', help='Manual verification only: override every recipient; never present in deployed task definitions')
    parser.add_argument('--check', action='store_true', help='Check job scope only; no business reads, writes or sends')
    args = parser.parse_args(argv)
    started = datetime.now(ZONE)
    if args.check:
        _write_summary({'status': 'checked', 'module': 'api.notification.scheduled',
                        'products': enabled_products(), 'requires_inspection': True})
        return 0
    report = {'run_id': uuid4().hex, 'status': 'running', 'started_at': started.isoformat(),
              'runner_pid': os.getpid(), 'sent_count': 0}
    path = args.log_dir / f'{started:%Y%m%d-%H%M%S}-{report["run_id"]}.json'
    try:
        with publication_lock(args.log_dir / 'notification-job', timeout_seconds=1):
            atomic_report(path, report)
            from src.shared_kernel.config import ConfigLoader
            receipt = wait_for_inspection(args.receipt, enabled_products(), started.date(),
                                          wait_seconds=args.wait_seconds,
                                          max_age_seconds=ConfigLoader.get_cache_ttl_seconds())
            report['inspection_run_id'] = receipt['run_id']
            command = ['--send', '--env-file', str(args.env_file)]
            if args.test_mode:
                command.append('--test-mode')
            if args.mail_config is not None:
                command += ['--mail-config', str(args.mail_config)]
            if sender is None:
                from api.notification.__main__ import main as sender
            output = StringIO()
            with redirect_stdout(output):
                result = sender(command)
            try:
                summary = json.loads(output.getvalue())
            except (TypeError, ValueError):
                summary = {}
            # Retain only fixed operational fields, never raw messages or untrusted output.
            if isinstance(summary, dict):
                for field in ('record_count', 'notification_count', 'sent_count', 'complete',
                              'failed_source_count', 'dry_run'):
                    value = summary.get(field)
                    if type(value) in (int, bool):
                        report[field] = value
            report['status'] = 'success' if result == 0 else 'partial' if result == 3 else 'failed'
            report['exit_code'] = result if type(result) is int and result in (0, 2, 3) else 2
    except GateError as exc:
        report.update(status='failed', code=str(exc), exit_code=2)
    except Exception:
        report.update(status='failed', code='NOTIFICATION_RUN_FAILED', exit_code=2)
    except BaseException:
        report.update(status='failed', code='NOTIFICATION_INTERRUPTED', exit_code=2)
        raise
    finally:
        report['finished_at'] = datetime.now(ZONE).isoformat()
        atomic_report(path, report)
    _write_summary(report)
    return report['exit_code']


if __name__ == '__main__':
    os.chdir(ROOT)
    logging.basicConfig(handlers=[logging.NullHandler()], force=True)
    raise SystemExit(main())
