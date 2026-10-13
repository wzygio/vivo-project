"""Filter Indicator display dates without changing source facts or decisions."""

from __future__ import annotations

import pandas as pd

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy


def apply_indicator_date_exclusion(
    frame: pd.DataFrame, *, time_column: str,
) -> pd.DataFrame:
    """Apply the current inclusive date policy to a copy of an output projection."""
    exclusion = ConfigLoader.get_indicator_data_exclusion()
    if exclusion is None or frame.empty:
        return frame.copy()
    if time_column not in frame:
        raise ValueError(f"Indicator date exclusion requires column: {time_column}")
    # Q-Time source keys use compact timestamps; other adapters use ISO/datetime.
    values = frame[time_column].astype("string").str.replace(
        r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{6})?$",
        r"\1-\2-\3 \4:\5:\6", regex=True,
    )
    dates = pd.to_datetime(values, errors="coerce", format="mixed").dt.tz_localize(None).dt.normalize()
    excluded = dates.between(pd.Timestamp(exclusion[0]), pd.Timestamp(exclusion[1]), inclusive="both")
    return frame.loc[~excluded].copy()


def indicator_source_date_predicate(
    time_expression: str, policy: DataForwardPolicy,
) -> tuple[str, dict[str, str]]:
    """Exclude source facts before SQL GROUP BY/LIMIT using bound display dates.

    The expression is a fixed adapter-owned SQL column, never user input.
    """
    exclusion = ConfigLoader.get_indicator_data_exclusion()
    if exclusion is None:
        return "", {}
    start, end = policy.to_source_window(
        pd.Timestamp(exclusion[0]), pd.Timestamp(exclusion[1]) + pd.Timedelta(days=1),
    )
    return (
        f" AND ({time_expression} < :excluded_start_time OR {time_expression} >= :excluded_end_time)",
        {"excluded_start_time": start.isoformat(sep=" "),
         "excluded_end_time": end.isoformat(sep=" ")},
    )
