"""Headless publication for IJP, critical parts and evaporation IQC.

The domain services retain all calculations. This adapter selects complete scopes,
rejects swallowed source failures and verifies actual saved publications.
"""
from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import re

import pandas as pd


MODULES = ('ijp_aa', 'ijp_hole', 'parts', 'iqc_evaporation')
_PUBLICATIONS = {
    'ijp_aa': ('indicator_domain', 'ijp_alerts', 'AA区'),
    'ijp_hole': ('indicator_domain', 'ijp_alerts', '孔区'),
    'parts': ('equipment_domain', 'parts_alerts', '超规备件'),
    'iqc_evaporation': ('iqc_domain', 'evaporation_alerts', '超规材料'),
}


def _source(module: str) -> tuple[Path, str]:
    from src.shared_kernel.config import ConfigLoader
    domain, resource, sheet = _PUBLICATIONS[module]
    return ConfigLoader.get_domain_resource_path(domain, resource), sheet


def _week(reference: date) -> tuple[date, date]:
    end = reference - timedelta(days=reference.weekday() + 1)
    return end - timedelta(days=6), end


def _verify_saved(module: str, products: tuple[str, ...], reference: date,
                  count: int, started: datetime) -> None:
    from src.shared_kernel.utils.excel_tools import read_workbook_sheets

    path, sheet = _source(module)
    frames = read_workbook_sheets(path)
    alerts, metadata = frames[sheet], frames['__meta__']
    if module.startswith('ijp_'):
        metadata = metadata.loc[metadata['region'].eq(sheet)]
    if len(metadata) != 1:
        raise ValueError('INSPECTION_PUBLICATION_MISSING_METADATA')
    meta = metadata.iloc[0]
    if meta['status'] != 'success' or int(meta['alert_count']) != count or len(alerts) != count:
        raise ValueError('INSPECTION_PUBLICATION_COUNT_MISMATCH')
    published = pd.Timestamp(meta['updated_at'])
    if published.tzinfo is not None:
        published = published.tz_convert('Asia/Shanghai').tz_localize(None)
    # Writers have second precision. This is production proof, not a file-mtime check.
    if (pd.isna(published) or published.date() != reference
            or published < pd.Timestamp(started).floor('s')
            or published > pd.Timestamp.now() + pd.Timedelta(seconds=1)):
        raise ValueError('INSPECTION_PUBLICATION_NOT_CURRENT_RUN')
    if module == 'parts':
        if str(meta['as_of_date']) != str(reference):
            raise ValueError('INSPECTION_PARTS_SCOPE_MISMATCH')
    else:
        start, end = _week(reference)
        fields = ('period_start', 'period_end') if module.startswith('ijp_') else ('start_date', 'end_date')
        if (str(meta[fields[0]]), str(meta[fields[1]])) != (str(start), str(end)):
            raise ValueError('INSPECTION_PUBLICATION_WINDOW_MISMATCH')
        if module.startswith('ijp_') and str(meta['reference_date']) != str(reference):
            raise ValueError('INSPECTION_IJP_REFERENCE_MISMATCH')
        if module == 'iqc_evaporation' and tuple(str(meta['products']).split('、')) != products:
            raise ValueError('INSPECTION_IQC_PRODUCT_SCOPE_MISMATCH')


def _produce_ijp(module: str, products: tuple[str, ...], reference: date) -> int:
    from src.indicator_domain.application.ijp.service import IjpReportService
    from src.indicator_domain.application.ijp.settings import IjpSettings
    from src.indicator_domain.application.ijp_hole.service import IjpHoleReportService
    from src.indicator_domain.composition import build_ijp_alert_store, build_ijp_repository
    from src.indicator_domain.infrastructure.ijp_hole.repository import IjpHoleRepository
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

    manager = DatabaseManager()
    if manager.engine is None:
        raise RuntimeError('INSPECTION_DATABASE_UNAVAILABLE')
    orders = tuple(ConfigLoader.get_work_order_types())
    store = build_ijp_alert_store()
    if module == 'ijp_aa':
        service = IjpReportService(build_ijp_repository(manager),
            enabled_product_codes=products, work_order_types=orders,
            today_provider=lambda: reference, alert_store=store,
            settings=IjpSettings.model_validate(ConfigLoader.load_domain_config('indicator_domain').get('ijp', {})))
    else:
        service = IjpHoleReportService(IjpHoleRepository(manager), enabled_products=products,
            work_order_types=orders, today_provider=lambda: reference, alert_store=store)
    return len(service.get_alert_report()['alerts'])


@contextmanager
def _source_errors(engine):
    """Detect equipment's legacy empty-on-database-error fallback."""
    from sqlalchemy import event
    failures = []
    def failed(context):
        original = context.original_exception
        state = getattr(original, 'pgcode', None)
        failures.append(state if isinstance(state, str) and re.fullmatch(r'[A-Z0-9]{5}', state) else 'UNKNOWN')
    event.listen(engine, 'handle_error', failed)
    try:
        yield failures
    finally:
        event.remove(engine, 'handle_error', failed)


class _RefreshedPartsPort:
    """Use this run's forced source read, including its successful empty result."""
    def __init__(self, delegate, refreshed: pd.DataFrame):
        self.delegate, self.refreshed = delegate, refreshed.copy()

    def load_baseline(self, path: str) -> pd.DataFrame:
        return self.delegate.load_baseline(path)

    def load_cvd(self, path: str, *, as_of_date: str = '') -> pd.DataFrame:
        return self.delegate.load_cvd(path, as_of_date=as_of_date)

    def load_snapshots(self, spec_df: pd.DataFrame, *, as_of: pd.Timestamp,
                       max_age_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
        from src.equipment_domain.infrastructure.data_loader import (
            filter_recent_part_life_measurements, load_fabricated_part_life_snapshot,
        )
        from src.shared_kernel.report_cutoff_config import load_report_cutoff_policy
        source = filter_recent_part_life_measurements(self.refreshed, as_of=as_of, max_age_days=max_age_days)
        fabricated = load_fabricated_part_life_snapshot(spec_df, now=as_of)
        cutoff = load_report_cutoff_policy()
        return cutoff.filter_frame(source, 'glass_start_time'), cutoff.filter_frame(fabricated, 'glass_start_time')


def _produce_parts(products: tuple[str, ...], reference: date) -> int:
    from src.equipment_domain.application.parts_alert_service import publish_parts_alerts
    from src.equipment_domain.application.parts_service import PartsReportService, build_parts_report_cache_context
    from src.equipment_domain.composition import create_parts_alert_store, create_parts_data_port
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

    manager = DatabaseManager()
    if manager.engine is None:
        raise RuntimeError('INSPECTION_DATABASE_UNAVAILABLE')
    baseline = ConfigLoader.get_domain_resource_path('equipment_domain', 'critical_parts_baseline')
    cvd = ConfigLoader.get_domain_resource_path('equipment_domain', 'cvd_parts')
    # Missing maintained inputs must not be silently regenerated by a batch task.
    if not baseline.is_file() or not cvd.is_file():
        raise ValueError('INSPECTION_PARTS_INPUT_UNAVAILABLE')
    port = create_parts_data_port(manager)
    if port.load_baseline(str(baseline)).empty:
        raise ValueError('INSPECTION_PARTS_BASELINE_EMPTY')
    refresh_started = datetime.now()
    with _source_errors(manager.engine) as failures:
        refreshed = port.refresh(str(baseline))
    proof = refreshed.attrs.get('parts_source_read', {})
    try:
        valid_read = (proof.get('status') == 'success'
                      and pd.Timestamp(proof['started_at']) >= pd.Timestamp(refresh_started)
                      and pd.Timestamp(proof['completed_at']) >= pd.Timestamp(proof['started_at'])
                      and pd.Timestamp(proof['completed_at']) <= pd.Timestamp.now() + pd.Timedelta(seconds=1))
    except (KeyError, TypeError, ValueError):
        valid_read = False
    if not valid_read:
        # Pool pre-ping/metadata may recover before a successful business SELECT.
        # Trust only the new native SELECT's completion proof; never an old file.
        suffix = '_SQLSTATE_' + failures[-1] if failures and failures[-1] != 'UNKNOWN' else ''
        raise RuntimeError('INSPECTION_PARTS_SOURCE_FAILED' + suffix)
    context = build_parts_report_cache_context(baseline, cvd)
    context['as_of_date'] = str(reference)
    report = PartsReportService.get_report_data(manager, str(baseline),
        snapshot_signature=f'inspection:{reference}', _data_port=_RefreshedPartsPort(port, refreshed),
        cvd_path=str(cvd), **context)
    return len(publish_parts_alerts(report.report_df, store=create_parts_alert_store(),
                                   as_of_date=str(reference), last_update=report.last_update))


def _produce_iqc(products: tuple[str, ...], reference: date) -> int:
    from src.iqc_domain.application.eva_materials.alerts import publish_evaporation_alerts
    from src.iqc_domain.composition import build_evaporation_alert_store, build_evaporation_service
    start, end = _week(reference)
    report = build_evaporation_service(enabled_products=products).get_report(start, end, products)
    return len(publish_evaporation_alerts(report, store=build_evaporation_alert_store(),
                                        start=start, end=end, products=products))


@dataclass(frozen=True)
class AuxiliaryRuntime:
    ijp: Callable = _produce_ijp
    parts: Callable = _produce_parts
    iqc: Callable = _produce_iqc
    verify: Callable = _verify_saved


def produce_auxiliary(products: tuple[str, ...], reference_date: date, *,
                      runtime: AuxiliaryRuntime | None = None) -> list[dict]:
    """Execute each domain once for its full scope; isolate source failures."""
    if (not products or len(set(products)) != len(products)
            or any(not isinstance(product, str) or not product for product in products)):
        raise ValueError('INSPECTION_PRODUCT_SCOPE_INVALID')
    runtime = runtime or AuxiliaryRuntime()
    results = []
    for module in MODULES:
        outcome = {'module': module}
        started = datetime.now()
        try:
            count = (runtime.ijp(module, products, reference_date) if module.startswith('ijp_')
                     else runtime.parts(products, reference_date) if module == 'parts'
                     else runtime.iqc(products, reference_date))
            runtime.verify(module, products, reference_date, count, started)
            outcome.update(status='success', alert_count=count)
        except Exception as exc:
            outcome.update(status='failed', error_type=type(exc).__name__)
            code = str(exc)
            if re.fullmatch(r'INSPECTION_[A-Z0-9_]+', code):
                outcome['code'] = code
        results.append(outcome)
    return results
