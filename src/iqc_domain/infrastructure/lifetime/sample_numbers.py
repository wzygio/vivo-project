"""Allocate anonymous sample numbers once per product, batch and screen."""

from collections import defaultdict
import hashlib
import hmac
import json
from pathlib import Path
import re
from secrets import token_hex

import pandas as pd

from src.iqc_domain.core.lifetime.lifetime import GROUP_COLUMNS
from src.iqc_domain.core.lifetime.alerts import normalize_lifetime_ledger
from src.shared_kernel.infrastructure.alert_workbook import write_locked_workbook
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets

NUMBER_SHEET = '__sample_numbers__'
KEY_SHEET = '__sample_number_key__'
NUMBER_COLUMNS = [*GROUP_COLUMNS, '样品摘要', '样品编号']


def lifetime_sample_numbers(path: Path, identities: pd.DataFrame, *, persist: bool) -> pd.Series:
    """Input contains one sorted in-memory row per source sample; persist only scoped digests."""
    if identities.empty:
        return pd.Series(dtype='int64', index=identities.index)
    with publication_lock(path):
        sheets = read_workbook_sheets(path, preserve_text=True) if path.exists() else {}
        if (NUMBER_SHEET in sheets) != (KEY_SHEET in sheets):
            raise ValueError('LIFETIME_SAMPLE_REGISTRY_INCOMPLETE')
        registry = sheets.get(NUMBER_SHEET, pd.DataFrame(columns=NUMBER_COLUMNS))
        if list(registry) != NUMBER_COLUMNS or registry.duplicated([*GROUP_COLUMNS, '样品摘要']).any():
            raise ValueError('LIFETIME_SAMPLE_REGISTRY_INVALID')
        secret = (str(sheets[KEY_SHEET]['key'].item()) if KEY_SHEET in sheets else token_hex(32))
        key = bytes.fromhex(secret)
        if len(key) != 32:
            raise ValueError('LIFETIME_SAMPLE_REGISTRY_INVALID')
        assigned = {}
        maximum = defaultdict(int)
        # Reserve legacy ranks without binding their decisions to an unproven source identity.
        if '寿命预警' in sheets:
            legacy = normalize_lifetime_ledger(sheets['寿命预警'])
            for product, batch, screen, number in legacy[[*GROUP_COLUMNS, '样品编号']].itertuples(index=False, name=None):
                group = (str(product), str(batch), str(screen))
                maximum[group] = max(maximum[group], int(number))
        occupied = set()
        for product, batch, screen, digest, number in registry.itertuples(index=False, name=None):
            group = (str(product), str(batch), str(screen))
            if not re.fullmatch(r'[0-9a-f]{64}', str(digest)) or not str(number).isdecimal():
                raise ValueError('LIFETIME_SAMPLE_REGISTRY_INVALID')
            number = int(number)
            if number < 1 or (group, number) in occupied:
                raise ValueError('LIFETIME_SAMPLE_REGISTRY_INVALID')
            occupied.add((group, number))
            assigned[group, digest] = number
            maximum[group] = max(maximum[group], number)
        new = []
        numbers = []
        for product, batch, screen, panel in identities.itertuples(index=False, name=None):
            group = (str(product), str(batch), str(screen))
            message = json.dumps([*group, str(panel)], ensure_ascii=False).encode()
            digest = hmac.new(key, message, hashlib.sha256).hexdigest()
            if (group, digest) not in assigned:
                maximum[group] += 1
                assigned[group, digest] = maximum[group]
                new.append((*group, digest, maximum[group]))
            numbers.append(assigned[group, digest])
        if new and persist:
            sheets[NUMBER_SHEET] = pd.concat([registry, pd.DataFrame(new, columns=NUMBER_COLUMNS)], ignore_index=True)
            sheets[KEY_SHEET] = pd.DataFrame({'key': [secret]})
            write_locked_workbook(path, sheets, NUMBER_SHEET)
    return pd.Series(numbers, index=identities.index, dtype='int64')
