"""File-only status and details for published IJP, parts and IQC alerts."""

from datetime import date, timedelta
import logging
from pathlib import Path

import pandas as pd
import streamlit as st

from src.indicator_domain.core.ijp.alerts import ALERT_COLUMNS
from src.equipment_domain.core.parts_alerts import PARTS_PUBLIC_COLUMNS
from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.utils.excel_tools import read_workbook_sheets

logger = logging.getLogger(__name__)
PUBLISHED_ALERT_ROWS = ('ijp_aa_oos', 'ijp_hole_oos', 'parts_oos', 'iqc_evaporation_ng')
_SOURCES = {
    'ijp_aa_oos': ('indicator_domain', 'ijp_alerts', 'AA区', tuple(ALERT_COLUMNS)),
    'ijp_hole_oos': ('indicator_domain', 'ijp_alerts', '孔区', tuple(ALERT_COLUMNS)),
    'parts_oos': ('equipment_domain', 'parts_alerts', '超规备件', PARTS_PUBLIC_COLUMNS),
    'iqc_evaporation_ng': ('iqc_domain', 'evaporation_alerts', '超规材料', REPORT_COLUMNS),
}
_EMPTY_METADATA_COLUMNS = {
    'ijp_aa_oos': ('region', 'reference_date', 'period_start', 'period_end', 'updated_at', 'alert_count', 'status'),
    'ijp_hole_oos': ('region', 'reference_date', 'period_start', 'period_end', 'updated_at', 'alert_count', 'status'),
    'parts_oos': ('as_of_date', 'last_update', 'updated_at', 'alert_count', 'status'),
    'iqc_evaporation_ng': ('start_date', 'end_date', 'products', 'updated_at', 'alert_count', 'status'),
}
UNAVAILABLE = '预警结果暂不可用，请更新对应报表后重试。'
EMPTY_TEMPLATE_MESSAGE = '当前暂无预警记录。'


@st.cache_data(max_entries=16, ttl=ConfigLoader.get_cache_ttl_seconds(), show_spinner=False)
def read_snapshot_workbook(path: str, modified: int, size: int) -> dict[str, pd.DataFrame]:
    return read_workbook_sheets(Path(path))


def _source(row_key: str) -> tuple[Path, str, tuple[str, ...]]:
    domain, key, sheet, columns = _SOURCES[row_key]
    return ConfigLoader.get_domain_resource_path(domain, key), sheet, columns


def read_published_alerts(row_key: str, product: str, reference_date: date) -> dict:
    """Validate publication scope/freshness, then slice public saved rows only."""
    try:
        path, sheet, columns = _source(row_key)
        stat = path.stat()
        frames = read_snapshot_workbook(str(path.resolve()), stat.st_mtime_ns, stat.st_size)
        alerts = frames[sheet]
        meta = frames['__meta__']
        scoped_meta = meta.loc[meta.region.eq(sheet)] if row_key.startswith('ijp_') else meta
        # Explicit header-only initialization is not a successful publication.
        required_sheets = ('AA区', '孔区') if row_key.startswith('ijp_') and meta.empty else (sheet,)
        if (scoped_meta.empty and set(_EMPTY_METADATA_COLUMNS[row_key]) <= set(meta)
                and all(name in frames and frames[name].empty
                        and set(columns) <= set(frames[name]) for name in required_sheets)):
            return {'state': 'no_data', 'alerts': pd.DataFrame(columns=columns),
                    'alert_factories': [], 'message': EMPTY_TEMPLATE_MESSAGE}
        meta = scoped_meta
        if len(meta) != 1 or not set(columns) <= set(alerts):
            raise ValueError('Invalid publication schema')
        record = meta.iloc[0]
        if record['status'] != 'success' or int(record.alert_count) != len(alerts):
            raise ValueError('Incomplete publication')
        published = pd.Timestamp(record.updated_at)
        now = pd.Timestamp.now()
        age = (now - published).total_seconds()
        if published.date() != reference_date or age < 0 or age > ConfigLoader.get_cache_ttl_seconds():
            raise ValueError('Expired publication')
        monday = reference_date - timedelta(days=reference_date.weekday())
        start, end = str(monday - timedelta(days=7)), str(monday - timedelta(days=1))
        if row_key.startswith('ijp_'):
            if (str(record.reference_date) != str(reference_date)
                    or str(record.period_start) != start or str(record.period_end) != end):
                raise ValueError('Invalid IJP scope')
            alerts = alerts.loc[alerts.productcode.eq(product)]
        elif row_key == 'parts_oos':
            if str(record.as_of_date) != str(reference_date):
                raise ValueError('Invalid parts scope')
        else:
            if str(record.start_date) != start or str(record.end_date) != end:
                raise ValueError('Invalid IQC window')
            if product not in str(record.products).split('、'):
                raise ValueError('Product outside publication scope')
            alerts = alerts.loc[alerts['产品型号'].fillna('').astype(str).map(
                lambda value: product in value.split('、'),
            )]
        public = alerts.loc[:, list(columns)].copy().reset_index(drop=True)
        if row_key.startswith('ijp_'):
            for column in ('ratio', 'minimum'):
                public[column] = pd.to_numeric(public[column], errors='raise')
                if not public[column].between(0, 1).all():
                    raise ValueError('Invalid IJP ratio')
        factories = []
        if row_key == 'parts_oos':
            public['厂别'] = public['厂别'].astype('string').str.strip().str.upper()
            factories = sorted(public['厂别'].dropna().unique().tolist())
        return {'state': 'alert' if len(public) else 'ok', 'alerts': public,
                'alert_factories': factories, 'message': ''}
    except Exception as exc:
        logger.warning('PUBLISHED_ALERT_UNAVAILABLE row=%s exception_type=%s', row_key, type(exc).__name__)
        return {'state': 'error', 'alerts': pd.DataFrame(), 'alert_factories': [], 'message': UNAVAILABLE}


def published_alert_signature(row_key: str, product: str, reference_date: date) -> str:
    try:
        path, _, _ = _source(row_key)
        stat = path.stat()
        file_version = f'{path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}'
    except (OSError, KeyError, ValueError):
        file_version = 'unavailable'
    # Revalidate age outside cell/detail caches, including unchanged files.
    state = read_published_alerts(row_key, product, reference_date)['state']
    return f'published-alert-v2:{row_key}:{file_version}:{reference_date}:{state}'


def load_published_detail(row_key: str, product: str, reference_date: date) -> dict:
    from app.sections.shared.alert_results import read_alert_result
    return {'kind': 'published_snapshot', 'row_key': row_key,
            **read_alert_result(row_key, product, reference_date)}


def clear_published_snapshot(row_key: str) -> None:
    path, _, _ = _source(row_key)
    try:
        stat = path.stat()
    except FileNotFoundError:
        return
    read_snapshot_workbook.clear(str(path.resolve()), stat.st_mtime_ns, stat.st_size)
