"""Persist OOC decisions and update the shared durable fact history."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.inline_domain.composition import build_ooc_history_service
from src.inline_domain.core.shared.sheet_ooc_decoration import (
    SCOPE_OOC_DECORATION_FILE_NAME,
)
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    persist_sheet_oos_decoration_outcome,
)
from src.inline_domain.infrastructure.shared.resource_paths import scope_resource_dir


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
) -> pd.DataFrame:
    """Write the mutable ledger, then save flag-free facts to Parquet."""
    outcome = persist_sheet_oos_decoration_outcome(
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
    configured_dir = scope_resource_dir(scope)
    history_service = build_ooc_history_service(
        None if product_dir.resolve() == configured_dir.resolve() else product_dir
    )
    history_service.update_history(
        scope,
        prod_code,
        outcome.decoration_df,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    return outcome.decoration_df
