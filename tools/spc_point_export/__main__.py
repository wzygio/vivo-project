"""Export final SPC plot points into XLSX files grouped by product."""
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
    start_time: datetime | None = None
    end_time: datetime | None = None
    forwarded_future: bool = False


def _project_path(value: str | Path) -> Path:
    path = Path(value)
    return (path if path.is_absolute() else ROOT / path).resolve()


def _display_datetime(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if result.tzinfo is not None:
        raise argparse.ArgumentTypeError('Use a local Asia/Shanghai timestamp without an offset')
    return result


def make_plan(argv: Sequence[str], *, today: date | None = None) -> Plan:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--product', action='append', help='Enabled product; repeat for several. Default: all enabled.')
    parser.add_argument('--start-date', type=date.fromisoformat, help='YYYY-MM-DD, default first day of previous month.')
    parser.add_argument('--end-date', type=date.fromisoformat, help='YYYY-MM-DD, default today in Asia/Shanghai.')
    parser.add_argument('--start-time', type=_display_datetime, help='Inclusive display timestamp, YYYY-MM-DD HH:MM[:SS], Asia/Shanghai.')
    parser.add_argument('--end-time', type=_display_datetime, help='Inclusive display timestamp, YYYY-MM-DD HH:MM[:SS], Asia/Shanghai.')
    parser.add_argument('--include-forwarded-future', action='store_true',
                        help='Use verified snapshots for already captured facts displayed after the report cutoff.')
    parser.add_argument('--output-dir', help='New output directory; relative paths are project-relative.')
    parser.add_argument('--from-export', help='Replay a prior export directory containing manifest.json and prepared Parquet points.')
    parser.add_argument('--check', action='store_true', help='Validate arguments only; no database, business reads or output writes.')
    args = parser.parse_args(argv)
    current = today or datetime.now(ZoneInfo('Asia/Shanghai')).date()
    replay = _project_path(args.from_export) if args.from_export else None
    saved = json.loads((replay / 'manifest.json').read_text(encoding='utf-8')) if replay else None
    if args.start_time and args.start_date and args.start_time.date() != args.start_date:
        raise ExportError('--start-date conflicts with --start-time')
    if args.end_time and args.end_date and args.end_time.date() != args.end_date:
        raise ExportError('--end-date conflicts with --end-time')
    end = (args.end_time.date() if args.end_time else args.end_date) or (date.fromisoformat(saved['end_date']) if saved else current)
    default_start = (end.replace(day=1) - timedelta(days=1)).replace(day=1)
    start = (args.start_time.date() if args.start_time else args.start_date) or (date.fromisoformat(saved['start_date']) if saved else default_start)
    if start > end or (end - start).days > 93:
        raise ExportError('Use start <= end and a window of at most 93 days')
    if end > current and not (saved or args.include_forwarded_future):
        raise ExportError('Use --include-forwarded-future for a display date after today')
    if saved and (start.isoformat() != saved['start_date'] or end.isoformat() != saved['end_date']):
        raise ExportError('A replay must retain the original export dates')
    start_time = args.start_time or (datetime.fromisoformat(saved['display_start'])
        if saved and saved.get('display_start') else datetime.combine(start, datetime.min.time()))
    end_time = args.end_time or (datetime.fromisoformat(saved['display_end'])
        if saved and saved.get('display_end') else datetime.combine(end, datetime.max.time()))
    cutoff = ConfigLoader.get_report_cutoff_policy().boundary(now=pd.Timestamp(current)).to_pydatetime()
    forwarded_future = bool(saved and saved.get('forwarded_future'))
    if not saved:
        if args.include_forwarded_future and end_time > cutoff:
            policy = ConfigLoader.get_data_forward_policy()
            _, source_end = policy.to_source_window(pd.Timestamp(start_time), pd.Timestamp(end_time))
            if not policy.effective_days or source_end.date() > current:
                raise ExportError('Forwarded export must map to an elapsed source window')
            forwarded_future = True
        elif end == current:
            end_time = min(end_time, cutoff)
    if start_time > end_time:
        raise ExportError('Display start must not exceed the effective display end')
    if saved and (saved.get('display_start') and start_time.isoformat() != saved['display_start']
                  or saved.get('display_end') and end_time.isoformat() != saved['display_end']):
        raise ExportError('A replay must retain the original display timestamp bounds')
    enabled = ConfigLoader.get_enabled_products()
    available = {row['product'] for row in saved['rows']} if saved else set(enabled)
    products = tuple(dict.fromkeys(args.product or [p for p in enabled if p in available]))
    if not products or any(p not in enabled or p not in available or not re.fullmatch(r'[A-Za-z0-9_.-]+', p) for p in products):
        raise ExportError('Select enabled products present in the requested source')
    stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d_%H%M%S')
    output = _project_path(args.output_dir or f'output/reports/spc_point_export/{start}_{end}/{stamp}_{uuid4().hex[:8]}')
    if replay and (output == replay or output in replay.parents):
        raise ExportError('Use a new output directory separate from the replay source')
    return Plan(products, start, end, output, replay, saved, args.check, start_time, end_time, forwarded_future)


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


def _select_display_window(frame: pd.DataFrame, plan: Plan) -> pd.DataFrame:
    """Select exact bounds only after the source completed every point decoration."""
    if frame.empty:
        return frame.copy(deep=False)
    if 'sheet_start_time' not in frame:
        raise ExportError('Decorated points are missing sheet_start_time')
    timestamps = pd.to_datetime(frame['sheet_start_time'], errors='coerce')
    start = pd.Timestamp(plan.start_time or plan.start)
    end = pd.Timestamp(plan.end_time) if plan.end_time else pd.Timestamp(plan.end) + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    return frame.loc[timestamps.ge(start) & timestamps.le(end)].copy(deep=False)


def _save_manifest(directory: Path, manifest: dict) -> None:
    (directory / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')


def run(plan: Plan, *, loader: Callable | None = None) -> int:
    from src.inline_domain.core.spc.spc_calculator import get_period_window_start
    display_end = plan.end_time if plan.forwarded_future else None
    if plan.check:
        print(json.dumps({'status': 'validated', 'products': plan.products,
                          'start_date': plan.start.isoformat(), 'end_date': plan.end.isoformat(),
                          'display_start': plan.start_time.isoformat() if plan.start_time else None,
                          'display_end': plan.end_time.isoformat() if plan.end_time else None,
                          'forwarded_future': plan.forwarded_future,
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
                'point_preparation_start': min(plan.start, get_period_window_start(plan.end)).isoformat(),
                'start_date': plan.start.isoformat(), 'end_date': plan.end.isoformat(),
                'display_start': (plan.start_time or datetime.combine(plan.start, datetime.min.time())).isoformat(),
                'display_end': (plan.end_time or datetime.combine(plan.end, datetime.max.time())).isoformat(),
                'point_contract': 'prepare_decorated_data(scope=spc).raw_measurements_df after traditional OOS and central-band point decoration',
                'export_at': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
                'cutoff': saved.get('cutoff', str(display_end or ConfigLoader.get_report_cutoff_policy().boundary())),
                'forwarded_future': plan.forwarded_future,
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
                    frame = load_product(manager, product, plan.start, plan.end, display_end=display_end)
                frame = _select_display_window(frame, plan)
                _validate_points(frame, product, plan)
                frame = frame.copy(deep=False)
                frame.attrs = {}
                frame.to_parquet(points_dir / f'{product}.parquet', index=False)
                result = write_product(frame, product, plan.output, manifest, temporary)
                manifest['rows'].append(result)
                _save_manifest(plan.output, manifest)
                for entry in result['files']:
                    print(f"SAVED {plan.output / entry['file']} ({entry['row_count']:,} rows)", flush=True)
                del frame
        manifest['status'] = 'success'
    except (Exception, KeyboardInterrupt) as exc:
        manifest['status'] = 'interrupted' if isinstance(exc, KeyboardInterrupt) else 'error'
        manifest['error'] = str(exc) if isinstance(exc, ExportError) else type(exc).__name__
        _save_manifest(plan.output, manifest)
        print(f"FAILED {manifest['error']}; see {plan.output / 'manifest.json'}", flush=True)
        return 130 if isinstance(exc, KeyboardInterrupt) else 2
    _save_manifest(plan.output, manifest)
    count = sum(len(result['files']) for result in manifest['rows'])
    print(f"COMPLETE {count} files for {len(manifest['rows'])} products: {plan.output}", flush=True)
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
