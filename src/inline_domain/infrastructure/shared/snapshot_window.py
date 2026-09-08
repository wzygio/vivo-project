"""Rolling raw-source coverage, independent of dashboard calendar summaries."""

from __future__ import annotations

import pandas as pd

from src.shared_kernel.snapshot_window import snapshot_window_start


class IncompleteMonitorSnapshotError(RuntimeError):
    """Source history cannot establish coverage for the requested open periods."""


def inline_snapshot_window_start(end_date: object) -> pd.Timestamp:
    return snapshot_window_start(end_date)


def metadata_covers_start(metadata: dict, required_start: object) -> bool:
    try:
        covered_start = pd.Timestamp(metadata["covered_from"])
    except (KeyError, TypeError, ValueError):
        return False
    return bool(pd.notna(covered_start) and covered_start <= pd.Timestamp(required_start))
