"""One atomic Excel transaction for product rates, shared input and metadata."""
from __future__ import annotations

from functools import lru_cache
from io import BytesIO
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import uuid

import pandas as pd

from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets
from src.yield_domain.core.mwd_trend.period_trend import RATE_COLUMNS, period_months


@lru_cache(maxsize=4)
def _cached_read(path: str, modified: int) -> dict[str, pd.DataFrame]:
    return read_workbook_sheets(Path(path))


class PeriodTrendWorkbook:
    def __init__(self, path: Path):
        self.path = path

    def _frames(self):
        if not self.path.exists():
            return {}
        return {k: v.copy() for k, v in _cached_read(str(self.path.resolve()), self.path.stat().st_mtime_ns).items()}

    @staticmethod
    def _product(product):
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,24}', product):
            raise ValueError('Invalid product sheet')

    def read(self, product):
        self._product(product)
        frames = self._frames()
        if product not in frames:
            return None
        meta = frames.get('__meta__', pd.DataFrame())
        if meta.empty or product not in set(meta['product']):
            raise ValueError('Incomplete history transaction')
        rates = frames[product][RATE_COLUMNS].fillna({'defect_code': '', 'defect_desc': ''})
        inputs = frames['投入数']
        inputs = inputs.loc[inputs['product'].eq(product) & inputs['period_type'].eq('month'), ['period', 'total_panels']].rename(columns={'period': 'month'})
        return rates, inputs.reset_index(drop=True), json.loads(meta.loc[meta['product'].eq(product), 'metadata'].iloc[0])

    def save(self, product, rates, inputs, meta):
        self._product(product)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with publication_lock(self.path):
            # Re-read under the OS lock so concurrent product publications merge.
            _cached_read.cache_clear()
            frames = self._frames()
            metadata = frames.get('__meta__', pd.DataFrame(columns=['product', 'metadata']))
            latest = metadata.loc[metadata['product'].eq(product), 'metadata']
            version = json.loads(latest.iloc[0]).get('version') if len(latest) else None
            if meta.get('expected_version') != version:
                raise RuntimeError('History changed concurrently; retry query')
            meta.pop('expected_version', None)
            meta['version'] = uuid.uuid4().hex
            existing = frames.get(product, pd.DataFrame(columns=RATE_COLUMNS))
            months = set(inputs.month)
            kept = existing.loc[~existing.month.isin(months)]
            frames[product] = pd.concat([kept, rates[RATE_COLUMNS]], ignore_index=True) if not kept.empty else rates[RATE_COLUMNS].copy()
            old_input = frames.get('投入数', pd.DataFrame(columns=['product', 'period_type', 'period', 'total_panels']))
            rows = []
            for year in sorted(set(int(m[:4]) for m in inputs.month)):
                source = inputs.loc[inputs.month.str.startswith(str(year))].set_index('month').total_panels
                for period, window in period_months(year, max(int(m[-2:]) for m in source.index)).items():
                    kind = 'half' if '-H' in period else ('quarter' if '-Q' in period else 'month')
                    rows.append({'product': product, 'period_type': kind, 'period': period, 'total_panels': source.reindex(window).sum(min_count=len(window))})
            new_input = pd.DataFrame(rows)
            replaced = old_input['product'].eq(product) & old_input['period'].isin(new_input['period'])
            frames['投入数'] = pd.concat([old_input.loc[~replaced], new_input], ignore_index=True)
            frames['__meta__'] = pd.concat([metadata.loc[metadata['product'].ne(product)], pd.DataFrame([{'product': product, 'metadata': json.dumps(meta)}])], ignore_index=True)
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter', engine_kwargs={'options': {'strings_to_formulas': False, 'strings_to_urls': False}}) as writer:
                for name, frame in frames.items():
                    frame.to_excel(writer, sheet_name=name, index=False)
            # Verify the exact payload before publishing; transparent enterprise
            # protection may encrypt on-disk XLSX immediately after close.
            payload = buffer.getvalue()
            verified = pd.read_excel(BytesIO(payload), sheet_name=None, engine='openpyxl')
            if len(verified[product]) != len(frames[product]) or len(verified['投入数']) != len(frames['投入数']):
                raise ValueError('History verification failed')
            fd, temporary = tempfile.mkstemp(prefix='.period-', suffix='.xlsx', dir=self.path.parent)
            temporary = Path(temporary)
            try:
                with os.fdopen(fd, 'wb') as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                if self.path.exists():
                    shutil.copy2(self.path, self.path.with_suffix('.xlsx.bak'))
                os.replace(temporary, self.path)
            finally:
                temporary.unlink(missing_ok=True)
            _cached_read.cache_clear()
