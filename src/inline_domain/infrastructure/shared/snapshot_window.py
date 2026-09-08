"""Inline source coverage required by open calendar-period monitor summaries."""

from __future__ import annotations

import pandas as pd

from src.shared_kernel.snapshot_window import snapshot_window_start


class IncompleteMonitorSnapshotError(RuntimeError):
    """Source history cannot establish coverage for the requested open periods."""


def inline_snapshot_window_start(end_date: object) -> pd.Timestamp:
    end = pd.Timestamp(end_date).normalize()
    return min(
        snapshot_window_start(end),
        end.replace(month=1, day=1),
        end - pd.Timedelta(days=end.weekday()),
    )


def metadata_covers_start(metadata: dict, required_start: object) -> bool:
    try:
        covered_start = pd.Timestamp(metadata["covered_from"])
    except (KeyError, TypeError, ValueError):
        return False
    return bool(pd.notna(covered_start) and covered_start <= pd.Timestamp(required_start))
