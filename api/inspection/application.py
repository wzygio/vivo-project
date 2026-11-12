"""Full dashboard production orchestration, with inspectable completion evidence."""
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
import os
import re
from pathlib import Path
import tempfile
from uuid import uuid4
from zoneinfo import ZoneInfo

import psutil

from src.shared_kernel.infrastructure.file_lock import publication_lock

PRODUCT_MODULES = ('spc', 'ctq', 'aoi_tt', 'aoi_rs', 'yield')
GLOBAL_MODULES = ('qtime', 'ijp_aa', 'ijp_hole', 'parts', 'iqc_evaporation')
MODULES = (*PRODUCT_MODULES, *GLOBAL_MODULES)


@dataclass(frozen=True)
class InspectionJob:
    name: str
    expected: tuple[tuple[str, str], ...]
    produce: Callable[[], list[dict]]


def required_keys(products: Sequence[str]) -> set[tuple[str, str]]:
    return {(module, product) for module in PRODUCT_MODULES for product in products} | {
        (module, '') for module in GLOBAL_MODULES}


def atomic_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix='.inspection-', suffix='.json')
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            json.dump(report, stream, ensure_ascii=False, allow_nan=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _now() -> str:
    return datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()


def _failed(module: str, product: str, code: str) -> dict:
    return {'module': module, **({'product': product} if product else {}),
            'status': 'failed', 'code': code}


def _run_job(job: InspectionJob) -> list[dict]:
    try:
        produced = job.produce()
        if not isinstance(produced, list):
            raise ValueError('Invalid inspection output')
        keys = [(row.get('module'), row.get('product', '')) for row in produced if isinstance(row, dict)]
        if len(keys) != len(produced) or set(keys) != set(job.expected) or len(keys) != len(set(keys)):
            raise ValueError('Inspection output does not match required scope')
        # Only fixed operational fields can enter logs. Do not trust adapter errors
        # or raw external messages to be suitable for unattended persistence.
        outcomes = [{'module': module, **({'product': product} if product else {}),
                 'status': 'success' if row.get('status') == 'success' else 'failed',
                 **({'code': 'PRODUCTION_OR_VERIFICATION_FAILED'} if row.get('status') != 'success' else {})}
                for (module, product), row in zip(keys, produced)]
        for outcome, row in zip(outcomes, produced):
            for key in ('code', 'error_type'):
                value = row.get(key)
                if isinstance(value, str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', value):
                    outcome[key] = value
            if type(row.get('retry_count')) is int and row['retry_count'] in (0, 1):
                outcome['retry_count'] = row['retry_count']
            diagnostics = row.get('diagnostics', row.get('error_chain'))
            if outcome['status'] == 'failed' and isinstance(diagnostics, list):
                safe = []
                for item in diagnostics[:6]:
                    if (isinstance(item, dict)
                            and isinstance(item.get('type'), str)
                            and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', item['type'])
                            and isinstance(item.get('file'), str)
                            and re.fullmatch(r'[A-Za-z0-9_.-]{1,77}\.py', item['file'])
                            and type(item.get('line')) is int and 1 <= item['line'] <= 1000000):
                        safe.append({key: item[key] for key in ('type', 'file', 'line')})
                if safe:
                    outcome['diagnostics'] = safe
        return outcomes
    except Exception as exc:
        return [{**_failed(module, product, 'PRODUCTION_OR_VERIFICATION_FAILED'),
                 'error_type': type(exc).__name__} for module, product in job.expected]


def run_inspection(products: Sequence[str], reference_date: date, jobs: Sequence[InspectionJob],
                   log_dir: Path, *, overlap_lock: Path | None = None) -> dict:
    """Run every source without allowing empty/stale results to imply completion."""
    products = tuple(products)
    expected = [key for job in jobs for key in job.expected]
    if (not products or len(set(products)) != len(products)
            or any(not isinstance(product, str) or not product for product in products)
            or set(expected) != required_keys(products) or len(expected) != len(set(expected))):
        raise ValueError('Inspection must cover every enabled product and module exactly once')
    directory = Path(log_dir)
    with publication_lock(overlap_lock or directory / 'inspection-job', timeout_seconds=1):
        process = psutil.Process(os.getpid())
        monday = reference_date - timedelta(days=reference_date.weekday())
        report = {'schema_version': 1, 'run_id': uuid4().hex,
                  'reference_date': reference_date.isoformat(), 'products': list(products),
                  'modules': list(MODULES), 'status': 'running', 'started_at': _now(),
                  'runner_pid': process.pid, 'runner_created': process.create_time(),
                  'coverage_start': (monday-timedelta(days=7)).isoformat(),
                  'coverage_end': reference_date.isoformat(), 'outcomes': []}

        def publish() -> None:
            atomic_report(directory / (report['run_id'] + '.json'), report)
            atomic_report(directory / 'latest.json', report)

        publish()
        try:
            for job in jobs:
                report['outcomes'].extend(_run_job(job))
                publish()
            report['status'] = ('success' if all(row['status'] == 'success' for row in report['outcomes'])
                                else 'partial')
        except BaseException:
            report.update(status='failed', finished_at=_now(), code='INSPECTION_INTERRUPTED')
            publish()
            raise
        report['finished_at'] = _now()
        publish()
        return report
