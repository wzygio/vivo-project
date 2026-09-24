"""Shared measurement filtering rules."""

from __future__ import annotations

import logging

import pandas as pd

EXCLUDED_PARAM_NAME_KEYWORDS = ("LOSS",)


def keep_latest_measurements(
    dataframe: pd.DataFrame, key_columns: list[str], time_column: str,
) -> pd.DataFrame:
    """Keep each key's latest complete row, sorting only duplicate candidates.

    Callers normalize timestamps first. Equal times keep the last input row;
    missing times rank below valid times. Retained rows keep their input order.
    Raw snapshots and the caller's DataFrame are never modified.
    """
    if dataframe.empty:
        return dataframe.copy()
    duplicate = dataframe.duplicated(key_columns, keep=False).to_numpy()
    if not duplicate.any():
        return dataframe.copy()

    positions = duplicate.nonzero()[0]
    candidates = dataframe.iloc[positions].copy()
    # Use positions rather than source labels, which need not be unique.
    candidates.index = positions
    winners = candidates.sort_values(
        time_column, kind="stable", na_position="first",
    ).drop_duplicates(key_columns, keep="last")
    keep = ~duplicate
    keep[winners.index] = True
    return dataframe.iloc[keep].copy()


def filter_excluded_param_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Remove measurement parameters that must not enter SPC calculations."""
    if dataframe.empty or "param_name" not in dataframe.columns:
        return dataframe.copy()

    param_names = dataframe["param_name"].fillna("").astype(str).str.upper()
    excluded = pd.Series(False, index=dataframe.index)
    for keyword in EXCLUDED_PARAM_NAME_KEYWORDS:
        excluded |= param_names.str.contains(keyword, regex=False, na=False)

    if not excluded.any():
        return dataframe.copy()

    logging.info(
        "[Measurement] Excluded measurement parameters containing %s: %s -> %s",
        ", ".join(EXCLUDED_PARAM_NAME_KEYWORDS),
        len(dataframe),
        len(dataframe) - int(excluded.sum()),
    )
    return dataframe.loc[~excluded].copy()
