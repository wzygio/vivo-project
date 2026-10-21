"""Presentation-neutral rows for Inline product-column weekly summaries."""
from dataclasses import dataclass
from datetime import date
from math import isfinite

import pandas as pd


@dataclass(frozen=True)
class WeeklySummaryRow:
    step_id: str
    layer: str
    code: str
    label: str
    values: tuple[str, ...]
    within_spec: tuple[bool | None, ...] = ()


@dataclass(frozen=True)
class WeeklySummaryTable:
    factory: str
    products: tuple[str, ...]
    rows: tuple[WeeklySummaryRow, ...]
    week_start: date
    week_end: date


def format_weekly_average(value: object) -> str:
    """Keep four decimals without hiding tiny nonzero quantities."""
    if pd.isna(value) or not isfinite(float(value)):
        return "数据不足"
    numeric = float(value)
    rounded = f"{numeric:.4f}".rstrip("0").rstrip(".")
    return f"{numeric:.4g}" if numeric != 0 and float(rounded) == 0 else rounded


def judge_weekly_density(value: object, upper_limit: object) -> bool | None:
    """Judge full-precision values; missing or nonfinite inputs are unclassified."""
    try:
        density, limit = float(value), float(upper_limit)
    except (TypeError, ValueError, OverflowError):
        return None
    return density <= limit if isfinite(density) and isfinite(limit) else None
