"""Primitive source-health contract shared by report adapters and consumers.

Coverage always describes source time, never shifted presentation dates.
An empty successful query is fresh; missing metadata is not proof of freshness.
"""

from typing import Literal, TypedDict

import pandas as pd

HealthStatus = Literal["fresh", "stale", "partial", "unavailable", "unknown"]


class DataHealth(TypedDict):
    status: HealthStatus
    source_start: str
    source_end: str
    refreshed_at: str
    error_code: str


def make_data_health(
    status: HealthStatus,
    *,
    source_start: str = "",
    source_end: str = "",
    refreshed_at: str = "",
    error_code: str = "",
) -> DataHealth:
    return {
        "status": status, "source_start": source_start, "source_end": source_end,
        "refreshed_at": refreshed_at, "error_code": error_code,
    }


def get_data_health(frame: pd.DataFrame) -> DataHealth:
    """Return an independent plain mapping; legacy frames remain unknown."""
    value = frame.attrs.get("data_health")
    if not isinstance(value, dict) or value.get("status") not in {
        "fresh", "stale", "partial", "unavailable", "unknown",
    }:
        return make_data_health("unknown")
    return make_data_health(
        value["status"],
        **{key: str(value.get(key, "")) for key in (
            "source_start", "source_end", "refreshed_at", "error_code",
        )},
    )


def attach_data_health(frame: pd.DataFrame, health: DataHealth) -> pd.DataFrame:
    """Attach primitive metadata to an adapter-owned result (without copying rows)."""
    frame.attrs["data_health"] = dict(health)
    return frame
