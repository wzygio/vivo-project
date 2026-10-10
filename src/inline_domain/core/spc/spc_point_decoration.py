"""SPC point decoration: original OOS limits, then a time-scoped central band."""

from __future__ import annotations

import math

import pandas as pd

from src.inline_domain.core.shared.decoration_window import decoration_window_mask
from src.inline_domain.core.shared.sheet_oos_decoration import (
    clip_point_values_to_limits,
    normalize_boolean_decisions as normalize_spc_decisions,
    prepare_point_decoration,
)

SpcPointDecorationPolicy = tuple[float, str, str]


def _apply_spc_special_decoration(
    points: pd.DataFrame, point_policy: SpcPointDecorationPolicy,
) -> pd.DataFrame:
    """Use original spec bounds and traditional output values for the central-band pass."""
    fraction, start_date, end_date = point_policy
    if isinstance(fraction, bool) or not math.isfinite(fraction) or not 0 < fraction <= 1:
        raise ValueError("SPC central_fraction must be in (0, 1]")
    active = decoration_window_mask(points["sheet_start_time"], (start_date, end_date))
    limits = points.reindex(columns=["usl", "lsl"]).apply(pd.to_numeric, errors="coerce")
    upper, lower = limits["usl"], limits["lsl"]
    midpoint = (upper + lower) / 2
    half_span = (upper - lower) * fraction / 2
    return clip_point_values_to_limits(
        points, midpoint - half_span, midpoint + half_span, eligible=active,
    )


def apply_spc_point_decoration(
    measurements: pd.DataFrame, specs: pd.DataFrame, decisions: pd.DataFrame,
    *, point_policy: SpcPointDecorationPolicy | None = None,
) -> pd.DataFrame:
    """Apply traditional OOS clipping before the optional SPC central-band pass.

    Both passes preserve False/legacy Delete values, rows, times and original
    specification columns. The special pass only acts in its inclusive date
    window, and hashes the first pass's output values for its margins.
    """
    if measurements.empty or specs.empty:
        return measurements.copy()
    prepared = prepare_point_decoration(measurements, specs, decisions)
    limits = prepared.reindex(columns=["usl", "lsl"]).apply(pd.to_numeric, errors="coerce")
    traditional = clip_point_values_to_limits(
        prepared, limits["lsl"], limits["usl"],
    )
    decorated = (
        _apply_spc_special_decoration(traditional, point_policy)
        if point_policy is not None else traditional
    )
    return decorated.drop(columns="_decorate")
