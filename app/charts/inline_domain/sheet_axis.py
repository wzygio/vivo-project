"""Shared categorical-axis labels for Sheet charts."""

from __future__ import annotations

import pandas as pd


def build_sheet_time_axis_labels(
    source_df: pd.DataFrame,
    *,
    time_column: str,
) -> tuple[list[str], dict[str, str]]:
    """Return time-ordered Sheet IDs and compact labels without changing axis semantics."""
    required_columns = {"sheet_id", time_column}
    if source_df.empty or not required_columns.issubset(source_df.columns):
        return [], {}

    axis_df = source_df[["sheet_id", time_column]].dropna(subset=["sheet_id"]).copy()
    axis_df["sheet_id"] = axis_df["sheet_id"].astype(str)
    axis_df[time_column] = pd.to_datetime(axis_df[time_column], errors="coerce")
    axis_df = (
        axis_df.sort_values([time_column, "sheet_id"], kind="stable", na_position="last")
        .drop_duplicates(subset=["sheet_id"], keep="first")
    )

    sheet_order = axis_df["sheet_id"].tolist()
    label_by_sheet: dict[str, str] = {}
    for row in axis_df.itertuples(index=False):
        pass_time = getattr(row, time_column)
        label_by_sheet[row.sheet_id] = (
            f"{row.sheet_id}<br>{pass_time:%m-%d %H}时"
            if pd.notna(pass_time)
            else row.sheet_id
        )
    return sheet_order, label_by_sheet
