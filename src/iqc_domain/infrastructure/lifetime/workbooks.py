"""Read the maintained WRGB hour limits and publish the latest lifetime failures."""

from datetime import date
from functools import lru_cache
from hashlib import sha256
from pathlib import Path

import numpy as np
import pandas as pd

from src.iqc_domain.core.lifetime.alerts import (
    LIFETIME_ALERT_COLUMNS, merge_lifetime_upload, normalize_lifetime_ledger,
    reconcile_lifetime_ledger, released_lifetime_alerts,
)
from src.shared_kernel.infrastructure.alert_workbook import edit_alert_workbook, snapshot_metadata, workbook_bytes
from src.shared_kernel.infrastructure.alert_workbook import SnapshotChangedError
from src.shared_kernel.core.decoration_revision import VERSION_SHEET, upload_revision
from src.iqc_domain.core.lifetime.alerts import LifetimeLedgerInvalid
from src.iqc_domain.infrastructure.lifetime.publication import lifetime_decision_signature
from src.iqc_domain.infrastructure.lifetime.sample_numbers import lifetime_sample_numbers
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_uploaded_excel_sheet, read_workbook_sheet, read_workbook_sheets


def read_lifetime_specs(path: Path) -> dict[tuple[str, str], float]:
    path = path.resolve()
    stat = path.stat()
    return _read_spec_generation(path, stat.st_mtime_ns, stat.st_size).copy()


@lru_cache(maxsize=8)
def _read_spec_generation(path: Path, mtime_ns: int, size: int) -> dict[tuple[str, str], float]:
    frame = read_workbook_sheet(path, '800nit寿命规格')
    screens = ('W', 'R', 'G', 'B')
    columns = [f'{screen}-Spec (H)' for screen in screens]
    if frame.empty or not {'产品型号', *columns}.issubset(frame.columns):
        raise ValueError('LIFETIME_SPEC_SCHEMA_INVALID')
    products = frame['产品型号'].astype('string').str.strip()
    if products.isna().any() or products.eq('').any() or products.duplicated().any():
        raise ValueError('LIFETIME_SPEC_PRODUCT_INVALID')
    hours = frame[columns].apply(pd.to_numeric, errors='raise').to_numpy(dtype=float)
    if not np.isfinite(hours).all() or (hours <= 0).any():
        raise ValueError('LIFETIME_SPEC_HOUR_INVALID')
    return {
        (product, screen): float(hour)
        for product, row in zip(products, hours)
        for screen, hour in zip(screens, row)
    }


LEDGER_SHEET = '寿命预警'


def _existing_ledger(sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    return sheets.get(LEDGER_SHEET, pd.DataFrame(columns=LIFETIME_ALERT_COLUMNS))


@lru_cache(maxsize=4)
def _download_generation(path: Path, payload: bytes) -> bytes:
    sheets = read_workbook_sheets(path, preserve_text=True) if payload else {}
    rows = _existing_ledger(sheets)
    ledger = normalize_lifetime_ledger(rows)
    return workbook_bytes({LEDGER_SHEET: ledger, VERSION_SHEET: pd.DataFrame({
        'revision': [lifetime_decision_signature(ledger)],
    })})


def _set_ledger(sheets: dict[str, pd.DataFrame], ledger: pd.DataFrame,
                reference_date: date, products: tuple[str, ...]) -> pd.DataFrame:
    released = released_lifetime_alerts(ledger)
    sheets[LEDGER_SHEET] = ledger
    sheets['__meta__'] = snapshot_metadata(
        {'reference_date': str(reference_date), 'products': '、'.join(products)},
        len(released), ledger_count=len(ledger),
    ).assign(decision_signature=lifetime_decision_signature(ledger))
    return released


class LifetimeAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def sample_numbers(self, identities: pd.DataFrame) -> pd.Series:
        return lifetime_sample_numbers(self.path, identities, persist=False)

    def reserve_sample_numbers(self, identities: pd.DataFrame) -> pd.Series:
        return lifetime_sample_numbers(self.path, identities, persist=True)

    def numbering_version(self) -> tuple[str, int, int]:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            return str(self.path.resolve()), 0, 0
        return str(self.path.resolve()), stat.st_mtime_ns, stat.st_size

    def download(self) -> tuple[bytes, str]:
        with publication_lock(self.path):
            if not self.path.exists():
                return _download_generation(self.path, b''), ''
            payload = self.path.read_bytes()
            version = sha256(payload).hexdigest()
            return _download_generation(self.path, payload), version

    def replace_uploaded(self, payload: bytes | memoryview, *, products: tuple[str, ...],
                         expected_digest: str) -> int:
        uploaded = read_uploaded_excel_sheet(payload, sheet_name=LEDGER_SHEET)
        revision = upload_revision(read_uploaded_excel_sheet(payload, sheet_name=VERSION_SHEET))
        with edit_alert_workbook(self.path, LEDGER_SHEET, expected_digest=expected_digest, preserve_text=True) as sheets:
            existing = _existing_ledger(sheets)
            ledger = merge_lifetime_upload(existing, uploaded, products)
            if '__meta__' not in sheets or len(sheets['__meta__']) != 1:
                raise LifetimeLedgerInvalid('请先完成寿命巡检发布后再维护决定。')
            current = lifetime_decision_signature(existing)
            signature = lifetime_decision_signature(ledger)
            if revision != current and (revision is not None or current != signature):
                raise SnapshotChangedError('决定版本已变化或缺失，请重新下载最新台账。')
            sheets[LEDGER_SHEET] = ledger
            sheets['__meta__'] = sheets['__meta__'].assign(
                alert_count=len(released_lifetime_alerts(ledger)), ledger_count=len(ledger),
                decision_signature=signature,
            )
        return len(ledger)

    def save(
        self, evaluations: pd.DataFrame, *, reference_date: date, products: tuple[str, ...],
    ) -> pd.DataFrame:
        with edit_alert_workbook(self.path, LEDGER_SHEET, preserve_text=True) as sheets:
            ledger = reconcile_lifetime_ledger(_existing_ledger(sheets), evaluations)
            released = _set_ledger(sheets, ledger, reference_date, products)
        return released
