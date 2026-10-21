"""AOI_TT Sheet projection: configurable UCL threshold and bounded integer hash."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable

import pandas as pd

from src.inline_domain.core.shared.sheet_oos_decoration import merge_detail_with_decoration_flags

AOI_TT_DECORATION_POLICY_VERSION = "factory-window-ucl-sheet-ratios-integer-v1"
DEFAULT_SHEET_SPEC_RATIOS = (0.5, 0.25)
_HASH_KEYS = ["prod_code", "factory", "step_id", "tt_name", "sheet_id"]


def _replacement_quantity(row: pd.Series, upper_ratio: float) -> int:
    seed = json.dumps([str(row[key]) for key in _HASH_KEYS], ensure_ascii=False)
    fraction = int.from_bytes(hashlib.sha256(seed.encode("utf-8")).digest()[:8], "big") / 2**64
    # TT quantities remain integers; floor also preserves a fractional upper bound.
    return math.floor(float(row["_ooc_ucl"]) * upper_ratio * fraction)


def apply_aoi_tt_special_decoration(
    attached: pd.DataFrame,
    decoration_df: pd.DataFrame,
    *,
    key_columns: list[str],
    exempt_param_name_contains: Iterable[str] = (),
    sheet_spec_ratios: tuple[float, float] = DEFAULT_SHEET_SPEC_RATIOS,
) -> pd.DataFrame:
    """Apply tri-state decisions to active Sheets with an attached UCL."""
    if attached.empty:
        return attached.copy()
    result = merge_detail_with_decoration_flags(attached, decoration_df, key_columns)
    result = result.loc[result["flag"].ne("Delete")].copy()
    threshold_ratio, upper_ratio = sheet_spec_ratios
    values = pd.to_numeric(result["tt_qty"], errors="coerce")
    limits = pd.to_numeric(result["_ooc_ucl"], errors="coerce")
    over = (
        result["flag"].eq(True) & limits.ge(0)
        & limits.map(lambda value: pd.notna(value) and math.isfinite(value))
        & values.gt(limits * threshold_ratio)
    ).fillna(False)
    for token in exempt_param_name_contains:
        if token is not None and str(token).strip():
            over &= ~result["tt_name"].astype("string").str.contains(
                str(token).strip(), case=False, regex=False, na=False,
            )
    if over.any():
        result.loc[over, "tt_qty"] = result.loc[over].apply(
            _replacement_quantity, axis=1, upper_ratio=upper_ratio,
        )
    return result.reindex(columns=attached.columns)
