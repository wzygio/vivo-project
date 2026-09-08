"""Explicit demo data access and filtering; no production quality decisions."""

from datetime import date

import pandas as pd

from src.iqc_domain.infrastructure.demo_repository import read_demo_report


def load_demo_report(report: str) -> pd.DataFrame:
    return read_demo_report(report)


def filter_demo_report(
    frame: pd.DataFrame,
    selections: dict[str, list[str]],
    date_column: str,
    date_range: tuple[date, date],
    keyword: str = "",
) -> pd.DataFrame:
    start, end = date_range
    mask = frame[date_column].dt.date.between(start, end)
    for column, values in selections.items():
        if values:
            mask &= frame[column].isin(values)
    if keyword.strip():
        matches = frame.fillna("").astype(str).apply(
            lambda column: column.str.contains(keyword.strip(), case=False, regex=False)
        )
        mask &= matches.any(axis=1)
    return frame.loc[mask].copy()
