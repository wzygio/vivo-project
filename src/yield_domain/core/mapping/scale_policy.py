"""One monthly Mapping scale policy for ledger writeback and row scaling."""

import logging
import math

MIN_MONTHLY_SCALE_FACTOR = 0.3
MAX_MONTHLY_SCALE_FACTOR = 3.0


def normalize_monthly_scale_factor(value: object) -> float:
    """Clip finite factors; unreadable/non-finite values retain neutral scaling."""
    try:
        factor = float(value)
    except (TypeError, ValueError):
        factor = math.nan
    if not math.isfinite(factor):
        logging.error("Mapping 月度倍率无效，按 1.0 处理: %r", value)
        return 1.0
    return min(MAX_MONTHLY_SCALE_FACTOR, max(MIN_MONTHLY_SCALE_FACTOR, factor))
