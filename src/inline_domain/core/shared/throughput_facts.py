"""Normalize Inline producer rows into lightweight daily throughput facts."""

from __future__ import annotations

import pandas as pd


THROUGHPUT_COLUMNS = [
    "scope",
    "factory",
    "prod_code",
    "event_date",
    "item_id",
]


def build_daily_throughput_facts(
    frame: pd.DataFrame,
    *,
    scope: str,
    prod_code: str,
    event_time_column: str,
    item_id_column: str,
) -> pd.DataFrame:
    """Keep one physical Sheet identifier per factory and calendar day.

    Storing the lightweight identifier instead of a pre-aggregated count lets
    the monitor de-duplicate the same Sheet across parameters and scopes.
    """
    if frame is None or frame.empty:
        return pd.DataFrame(columns=THROUGHPUT_COLUMNS)

    required = {"factory", event_time_column, item_id_column}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"throughput source missing columns: {sorted(missing)}")

    normalized = frame[["factory", event_time_column, item_id_column]].copy()
    normalized["event_date"] = pd.to_datetime(
        normalized[event_time_column], errors="coerce"
    ).dt.normalize()
    normalized = normalized.dropna(subset=["event_date"])
    for column in ["factory", item_id_column]:
        normalized[column] = normalized[column].fillna("").astype(str)
    normalized = normalized[normalized[item_id_column].str.strip().ne("")]
    facts = normalized.drop_duplicates(
        ["factory", "event_date", item_id_column]
    ).rename(columns={item_id_column: "item_id"})
    facts.insert(0, "prod_code", str(prod_code).strip())
    facts.insert(0, "scope", str(scope).strip().lower())
    return facts[THROUGHPUT_COLUMNS]


__all__ = ["THROUGHPUT_COLUMNS", "build_daily_throughput_facts"]
