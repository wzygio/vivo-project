"""Verified production-completion gate and declarative Windows task definitions."""
from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
import json
from math import isfinite
from pathlib import Path
import subprocess
import time
from zoneinfo import ZoneInfo

import psutil

from api.inspection.application import MODULES, required_keys


ZONE = ZoneInfo('Asia/Shanghai')
ROOT = Path(__file__).resolve().parents[2]
INSPECTION_TASK = 'vivo-project Yield IJP Alert Publication'
MAIL_TASK = 'vivo-project Alert Email Push'


class GateError(ValueError):
    """Fixed error codes only; never echo untrusted receipts or credentials."""


def _same_process(pid: int, created: float) -> bool:
    try:
        process = psutil.Process(pid)
        return process.is_running() and abs(process.create_time() - created) < .001
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise GateError('INSPECTION_RECEIPT_INVALID')
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError:
        raise GateError('INSPECTION_RECEIPT_INVALID') from None
    return stamp.replace(tzinfo=ZONE) if stamp.tzinfo is None else stamp.astimezone(ZONE)


def _validate_receipt(value: object, products: tuple[str, ...], day: date, now: datetime,
                      max_age_seconds: float) -> dict:
    if not isinstance(value, dict) or value.get('schema_version') != 1:
        raise GateError('INSPECTION_RECEIPT_INVALID')
    if value.get('reference_date') != day.isoformat():
        raise GateError('INSPECTION_NOT_CURRENT')
    if (value.get('products') != list(products) or not isinstance(value.get('modules'), list)
            or any(not isinstance(module, str) for module in value['modules'])
            or len(value['modules']) != len(MODULES) or set(value['modules']) != set(MODULES)
            or not isinstance(value.get('run_id'), str) or not value['run_id']):
        raise GateError('INSPECTION_SCOPE_CHANGED')
    expected = required_keys(products)
    outcomes = value.get('outcomes')
    if not isinstance(outcomes, list) or any(not isinstance(row, dict) for row in outcomes):
        raise GateError('INSPECTION_RECEIPT_INVALID')
    keys = [(row.get('module'), row.get('product', '')) for row in outcomes]
    if (any(not isinstance(part, str) for key in keys for part in key)
            or len(keys) != len(set(keys)) or set(keys) - expected):
        raise GateError('INSPECTION_SCOPE_INCOMPLETE')
    if any(row.get('status') != 'success' for row in outcomes):
        raise GateError('INSPECTION_PRODUCTION_FAILED')
    monday = day - timedelta(days=day.weekday())
    try:
        coverage_start = date.fromisoformat(value.get('coverage_start', ''))
        coverage_end = date.fromisoformat(value.get('coverage_end', ''))
    except (ValueError, TypeError):
        raise GateError('INSPECTION_RECEIPT_INVALID') from None
    if coverage_start > monday - timedelta(days=7) or coverage_end < monday - timedelta(days=1):
        raise GateError('INSPECTION_WEEK_NOT_COVERED')
    started = _timestamp(value.get('started_at'))
    if started.date() != day or started > now:
        raise GateError('INSPECTION_NOT_CURRENT')
    status = value.get('status')
    if status == 'success':
        if set(keys) != expected:
            raise GateError('INSPECTION_SCOPE_INCOMPLETE')
        finished = _timestamp(value.get('finished_at'))
        if finished < started or finished > now or (now - finished).total_seconds() > max_age_seconds:
            raise GateError('INSPECTION_RESULT_EXPIRED')
    elif status == 'running' and (now - started).total_seconds() > 7200:
        raise GateError('INSPECTION_NOT_CURRENT')
    elif status != 'running':
        raise GateError('INSPECTION_PRODUCTION_FAILED')
    return value


def wait_for_inspection(path: Path, products: Sequence[str], reference_date: date, *,
                        wait_seconds: float = 7200, poll_seconds: float = 5,
                        max_age_seconds: float = 14400,
                        now: Callable[[], datetime] = lambda: datetime.now(ZONE),
                        monotonic: Callable[[], float] = time.monotonic,
                        sleep: Callable[[float], None] = time.sleep,
                        process_alive: Callable[[int, float], bool] = _same_process) -> dict:
    """Wait only on a proven live run; stale/malformed/partial receipts never send."""
    products = tuple(products)
    if (not products or len(set(products)) != len(products)
            or any(not isinstance(item, str) or not item for item in products)
            or any(isinstance(value, bool) or not isinstance(value, (int, float))
                   or not isfinite(value) or value <= 0
                   for value in (wait_seconds, poll_seconds, max_age_seconds))
            or wait_seconds > 7200 or poll_seconds > 60):
        raise GateError('INSPECTION_GATE_CONFIGURATION_INVALID')
    deadline = monotonic() + wait_seconds
    while True:
        try:
            value = json.loads(Path(path).read_text(encoding='utf-8'))
        except (OSError, UnicodeError, ValueError):
            raise GateError('INSPECTION_RECEIPT_UNAVAILABLE') from None
        stamp = now()
        stamp = stamp.replace(tzinfo=ZONE) if stamp.tzinfo is None else stamp.astimezone(ZONE)
        receipt = _validate_receipt(value, products, reference_date, stamp, max_age_seconds)
        if receipt['status'] == 'success':
            return receipt
        pid, created = receipt.get('runner_pid'), receipt.get('runner_created')
        if (type(pid) is not int or pid <= 0 or isinstance(created, bool)
                or not isinstance(created, (int, float)) or not isfinite(created)
                or not process_alive(pid, created)):
            raise GateError('INSPECTION_RUNNER_NOT_LIVE')
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise GateError('INSPECTION_WAIT_TIMEOUT')
        sleep(min(poll_seconds, remaining))


def build_task_specs(root: Path = ROOT, *, env_file: Path | None = None,
                     mail_config: Path | None = None) -> list[dict]:
    root = Path(root).resolve()
    credentials = ['--env-file', str(Path(env_file).resolve())] if env_file is not None else []
    if mail_config is not None:
        credentials += ['--mail-config', str(Path(mail_config).resolve())]
    specs = [
        {'name': INSPECTION_TASK, 'module': 'api.inspection', 'parameters': [],
         'triggers': [{'kind': 'daily', 'time': '07:15'}]},
        {'name': MAIL_TASK, 'module': 'api.notification.scheduled', 'parameters': credentials,
         'triggers': [{'kind': 'weekly', 'weekday': 'Monday', 'time': '07:20'},
                      {'kind': 'weekly', 'weekday': 'Tuesday', 'time': '11:00'}]},
    ]
    return [{**spec, 'execute': str(root / '.venv/Scripts/pythonw.exe'),
             'working_directory': str(root),
             'arguments': subprocess.list2cmdline(['-m', spec['module'], *spec['parameters']])}
            for spec in specs]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Print task definitions only; never register or run jobs.')
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--env-file', type=Path)
    parser.add_argument('--mail-config', type=Path)
    args = parser.parse_args(argv)
    print(json.dumps(build_task_specs(args.root, env_file=args.env_file, mail_config=args.mail_config)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
