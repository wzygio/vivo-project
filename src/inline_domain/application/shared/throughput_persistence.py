"""Persist producer throughput without leaking test fixtures into production data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.inline_domain.application.shared.decorated_data import resolve_product_resource_dir
from src.inline_domain.composition import build_throughput_history_service


def persist_throughput_facts(
    *,
    scope: str,
    prod_code: str,
    details: pd.DataFrame,
    event_time_column: str,
    item_id_column: str,
    product_dir: Path,
    coverage_start: pd.Timestamp,
    coverage_end: pd.Timestamp,
) -> None:
    configured_dir = resolve_product_resource_dir(prod_code, scope=scope).resolve()
    supplied_dir = Path(product_dir).resolve()
    service = (
        build_throughput_history_service()
        if supplied_dir == configured_dir
        else build_throughput_history_service(supplied_dir)
    )
    service.update_from_details(
        scope,
        prod_code,
        details,
        event_time_column=event_time_column,
        item_id_column=item_id_column,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )


__all__ = ["persist_throughput_facts"]
