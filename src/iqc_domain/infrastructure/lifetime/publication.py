"""Read the public selection and verify counts for either lifetime workbook format."""

import pandas as pd
from hashlib import sha256

from src.iqc_domain.core.lifetime.alerts import (
    LIFETIME_KEY_COLUMNS, normalize_lifetime_ledger, released_lifetime_alerts,
)


def lifetime_decision_signature(ledger: pd.DataFrame) -> str:
    rows = normalize_lifetime_ledger(ledger).sort_values(LIFETIME_KEY_COLUMNS)
    decisions = rows[[*LIFETIME_KEY_COLUMNS, 'Flag']].astype(str).to_json(orient='values')
    return sha256(decisions.encode()).hexdigest()


def read_lifetime_publication(ledger: pd.DataFrame, metadata: pd.Series, *,
                              allow_decision_edits: bool = False) -> pd.DataFrame:
    public = released_lifetime_alerts(ledger)
    if 'Flag' in ledger:
        if pd.to_numeric(metadata.get('ledger_count'), errors='coerce') != len(ledger):
            raise ValueError('LIFETIME_LEDGER_COUNT_MISMATCH')
    elif 'ledger_count' in metadata:
        raise ValueError('LIFETIME_LEDGER_COLUMNS_MISSING')
    changed = (allow_decision_edits and pd.notna(metadata.get('decision_signature'))
               and metadata.get('decision_signature') != lifetime_decision_signature(ledger))
    if not changed and pd.to_numeric(metadata['alert_count'], errors='coerce') != len(public):
        raise ValueError('LIFETIME_RELEASE_COUNT_MISMATCH')
    return public
