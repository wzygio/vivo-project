"""Rolling raw-source coverage, independent of dashboard calendar summaries."""

from __future__ import annotations

import pandas as pd

from src.shared_kernel.snapshot_window import snapshot_window_start
from src.shared_kernel.config import ConfigLoader


class IncompleteMonitorSnapshotError(RuntimeError):
    """Source history cannot establish coverage for the requested open periods."""


def inline_snapshot_window_start(end_date: object) -> pd.Timestamp:
    return snapshot_window_start(end_date)


def inline_measurement_snapshot_window_start(prod_code: str, end_date: object) -> pd.Timestamp:
    """Extend only shared measurement history for products with a fixed anchor."""
    rolling_start = inline_snapshot_window_start(end_date)
    retained_start = ConfigLoader.get_inline_measurement_retention_start(prod_code)
    return rolling_start if retained_start is None else min(rolling_start, pd.Timestamp(retained_start))


def metadata_covers_start(metadata: dict, required_start: object) -> bool:
    try:
        covered_start = pd.Timestamp(metadata["covered_from"])
    except (KeyError, TypeError, ValueError):
        return False
    return bool(pd.notna(covered_start) and covered_start <= pd.Timestamp(required_start))
