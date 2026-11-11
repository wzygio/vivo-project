"""Export final SPC plot points into a separate XLSX for each enabled product."""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re
from typing import Callable, Sequence
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd

from src.shared_kernel.config import ConfigLoader
from . import ExportError
from .workbook import write_product

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Plan:
    products: tuple[str, ...]
    start: date
    end: date
    output: Path
    replay: Path | None
    saved: dict | None
    check: bool


def _project_path(value: str | Path) -> Path:
    path = Path(value)
    return (path if path.is_absolute() else ROOT / path).resolve()


def make_plan(argv: Sequence[str], *, today: date | None = None) -> Plan:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--product', action='append', help='Enabled product; repeat for several. Default: all enabled.')
    parser.add_argument('--start-date', type=date.fromisoformat, help='YYYY-MM-DD, default first day of previous month.')
    parser.add_argument('--end-date', type=date.fromisoformat, help='YYYY-MM-DD, default today in Asia/Shanghai.')
    parser.add_argument('--output-dir', help='New output directory; relative paths are project-relative.')
    parser.add_argument('--from-export', help='Replay a prior export directory containing manifest.json and prepared Parquet points.')
    parser.add_argument('--check', action='store_true', help='Validate arguments only; no database, business reads or output writes.')
    args = parser.parse_args(argv)
    current = today or datetime.now(ZoneInfo('Asia/Shanghai')).date()
    replay = _project_path(args.from_export) if args.from_export else None
    saved = json.loads((replay / 'manifest.json').read_text(encoding='utf-8')) if replay else None
    end = args.end_date or (date.fromisoformat(saved['end_date']) if saved else current)
    default_start = (end.replace(day=1) - timedelta(days=1)).replace(day=1)
    start = args.start_date or (date.fromisoformat(saved['start_date']) if saved else default_start)
    if end > current or start > end or (end - start).days > 93:
        raise ExportError('Use start <= end <= today and a window of at most 93 days')
    if saved and (start.isoformat() != saved['start_date'] or end.isoformat() != saved['end_date']):
        raise ExportError('A replay must retain the original export dates')
    enabled = ConfigLoader.get_enabled_products()
    available = {row['product'] for row in saved['rows']} if saved else set(enabled)
    products = tuple(dict.fromkeys(args.product or [p for p in enabled if p in available]))
    if not products or any(p not in enabled or p not in available or not re.fullmatch(r'[A-Za-z0-9_.-]+', p) for p in products):
        raise ExportError('Select enabled products present in the requested source')
    stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d_%H%M%S')
    output = _project_path(args.output_dir or f'output/reports/spc_point_export/{start}_{end}/{stamp}_{uuid4().hex[:8]}')
    if replay and (output == replay or output in replay.parents):
        raise ExportError('Use a new output directory separate from the replay source')
    return Plan(products, start, end, output, replay, saved, args.check)


def _replay_product(plan: Plan, product: str) -> pd.DataFrame:
    row = next(item for item in plan.saved['rows'] if item['product'] == product)
    base = (plan.replay / plan.saved.get('points_dir', '.')).resolve()
    if not base.is_relative_to(plan.replay):
        raise ExportError('Prepared point directory must stay within its export directory')
    frame = pd.read_parquet(base / f'{product}.parquet')
    if len(frame) != row['row_count'] or list(frame.columns) != row['columns']:
        raise ExportError(f'{product}: prepared points no longer match the saved manifest')
    return frame


def _validate_points(frame: pd.DataFrame, product: str, plan: Plan) -> None:
    if frame.empty:
        return
    if not {'prod_code', 'sheet_start_time'} <= set(frame.columns):
        raise ExportError(f'{product}: required point fields are missing')
    times = pd.to_datetime(frame['sheet_start_time'], errors='coerce')
    if (not frame['prod_code'].eq(product).all() or times.isna().any()
            or not times.ge(pd.Timestamp(plan.start)).all()
            or not times.lt(pd.Timestamp(plan.end) + pd.Timedelta(days=1)).all()):
        raise ExportError(f'{product}: points fall outside the selected product/date window')
    if 'data_type' in frame and not frame['data_type'].eq('SPC').all():
        raise ExportError(f'{product}: points contain a non-SPC data type')


def _save_manifest(directory: Path, manifest: dict) -> None:
    (directory / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')


def run(plan: Plan, *, loader: Callable | None = None) -> int:
    if plan.check:
        print(json.dumps({'status': 'validated', 'products': plan.products,
                          'start_date': plan.start.isoformat(), 'end_date': plan.end.isoformat(),
                          'source': 'prepared-export' if plan.replay else 'current-snapshots',
                          'output': str(plan.output)}, ensure_ascii=False))
        return 0
    if plan.output.exists():
        raise ExportError('Output directory already exists; choose a new directory')
    plan.output.mkdir(parents=True)
    points_dir = plan.output / 'points'
    points_dir.mkdir()
    temporary = ROOT / 'output/tmp/spc_point_export'
    temporary.mkdir(parents=True, exist_ok=True)
    saved = plan.saved or {}
    manifest = {'status': 'running', 'products': plan.products,
                'start_date': plan.start.isoformat(), 'end_date': plan.end.isoformat(),
                'export_at': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
                'cutoff': saved['cutoff'] if 'cutoff' in saved else str(ConfigLoader.get_report_cutoff_policy().boundary()),
                'time_policy': saved['time_policy'] if 'time_policy' in saved else ConfigLoader.get_data_forward_policy().signature,
                'source': str(plan.replay) if plan.replay else 'fresh snapshots or bounded read-only queries; read-only metadata/history',
                'points_dir': 'points', 'rows': []}
    _save_manifest(plan.output, manifest)
    try:
        if loader is None and not plan.replay:
            from .source import load_product, readonly_source
            context = readonly_source()
        else:
            context = nullcontext(None)
        with context as manager:
            for product in plan.products:
                print(f'POINTS {product}: loading', flush=True)
                if plan.replay:
                    frame = _replay_product(plan, product)
                elif loader:
                    frame = loader(product, plan.start, plan.end)
                else:
                    frame = load_product(manager, product, plan.start, plan.end)
                _validate_points(frame, product, plan)
                frame = frame.copy(deep=False)
                frame.attrs = {}
                frame.to_parquet(points_dir / f'{product}.parquet', index=False)
                result = write_product(frame, product, plan.output, manifest, temporary)
                manifest['rows'].append(result)
                _save_manifest(plan.output, manifest)
                print(f"SAVED {plan.output / result['file']} ({result['row_count']:,} rows)", flush=True)
                del frame
        manifest['status'] = 'success'
    except (Exception, KeyboardInterrupt) as exc:
        manifest['status'] = 'interrupted' if isinstance(exc, KeyboardInterrupt) else 'error'
        manifest['error'] = str(exc) if isinstance(exc, ExportError) else type(exc).__name__
        _save_manifest(plan.output, manifest)
        print(f"FAILED {manifest['error']}; see {plan.output / 'manifest.json'}", flush=True)
        return 130 if isinstance(exc, KeyboardInterrupt) else 2
    _save_manifest(plan.output, manifest)
    print(f"COMPLETE {len(manifest['rows'])} product files: {plan.output}", flush=True)
    return 0


def main(argv: Sequence[str] | None = None, *, loader: Callable | None = None,
         today: date | None = None) -> int:
    import sys
    return run(make_plan(list(argv) if argv is not None else sys.argv[1:], today=today), loader=loader)


if __name__ == '__main__':
    import logging
    import sys
    logging.disable(logging.CRITICAL)  # Library failures may embed authenticated SQL details.
    sys.stdout.reconfigure(encoding='utf-8')
    try:
        raise SystemExit(main())
    except ExportError as exc:
        print(f'FAILED {exc}', file=sys.stderr)
        raise SystemExit(2)
    except Exception as exc:
        print(f'FAILED configuration/input unavailable ({type(exc).__name__})', file=sys.stderr)
        raise SystemExit(2)
