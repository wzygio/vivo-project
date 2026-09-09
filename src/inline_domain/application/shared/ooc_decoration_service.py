"""Persist the editable OOC decision ledger for subsequent live calculations."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.inline_domain.core.shared.sheet_ooc_decoration import (
    SCOPE_OOC_DECORATION_FILE_NAME,
)
from src.inline_domain.application.shared.decoration_defaults import (
    persist_sheet_oos_decoration_outcome,
)
from src.inline_domain.application.shared.decoration_ports import SheetDecorationPort


def persist_ooc_facts(
    *,
    scope: str,
    prod_code: str,
    detail_df: pd.DataFrame,
    key_columns: list[str],
    product_dir: Path,
    coverage_start: pd.Timestamp,
    coverage_end: pd.Timestamp,
    product_revision: str = "",
    decision_signature: str = "",
    decoration_port: SheetDecorationPort | None = None,
) -> pd.DataFrame:
    """Write the mutable ledger; intermediate alarm results stay in cache."""
    persist_outcome = (
        decoration_port.persist_sheet_oos_decoration_outcome
        if decoration_port is not None
        else persist_sheet_oos_decoration_outcome
    )
    outcome = persist_outcome(
        product_dir,
        detail_df,
        file_name=SCOPE_OOC_DECORATION_FILE_NAME[scope],
        sheet_name=prod_code,
        key_columns=key_columns,
        scope=scope,
        prod_code=prod_code,
        product_revision=product_revision,
        # OOC decisions are independent from the OOS ledger/cache signature.
        # The repository computes the signature from this OOC workbook.
        decision_signature=None,
    )
    return outcome.decoration_df
