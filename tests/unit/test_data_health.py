"""Health is explicit and cache-safe, including for legacy adapters."""

import pickle

import pandas as pd

from src.shared_kernel.data_health import (
    attach_data_health, get_data_health, make_data_health,
)


def test_missing_health_is_unknown_and_health_survives_serialization():
    assert get_data_health(pd.DataFrame())["status"] == "unknown"
    frame = attach_data_health(pd.DataFrame({"value": [7]}), make_data_health(
        "stale", source_start="2026-08-01", source_end="2026-09-01",
        refreshed_at="2026-09-01T07:00:00+08:00", error_code="SOURCE_READ_FAILED",
    ))
    loaded = pickle.loads(pickle.dumps(frame))
    health = get_data_health(loaded)
    assert health["status"] == "stale"
    assert health["source_end"] == "2026-09-01"
    health["status"] = "fresh"
    assert get_data_health(loaded)["status"] == "stale"
