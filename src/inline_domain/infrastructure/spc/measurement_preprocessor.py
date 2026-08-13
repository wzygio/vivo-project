"""SPC-specific measurement filtering rules."""

from __future__ import annotations

import logging

import pandas as pd

SPC_EXCLUDED_PARAM_NAME_KEYWORDS = ("LOSS",)


def filter_excluded_spc_param_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Remove measurement parameters that must not enter SPC calculations."""
    if dataframe.empty or "param_name" not in dataframe.columns:
        return dataframe.copy()

    param_names = dataframe["param_name"].fillna("").astype(str).str.upper()
    excluded = pd.Series(False, index=dataframe.index)
    for keyword in SPC_EXCLUDED_PARAM_NAME_KEYWORDS:
        excluded |= param_names.str.contains(keyword, regex=False, na=False)

    if not excluded.any():
        return dataframe.copy()

    logging.info(
        "[SPC] Excluded measurement parameters containing %s: %s -> %s",
        ", ".join(SPC_EXCLUDED_PARAM_NAME_KEYWORDS),
        len(dataframe),
        len(dataframe) - int(excluded.sum()),
    )
    return dataframe.loc[~excluded].copy()
