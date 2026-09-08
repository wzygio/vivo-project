"""Retired migration entry point for the Yield modifier ledger.

The one-time migration used to copy legacy trend overrides and baselines into
``指定良损``. That column is now business-owned and may only be edited manually,
so keeping an executable backfill would be an unsafe hidden write path.
"""

from __future__ import annotations

import argparse
import logging
from typing import NoReturn

import pandas as pd


class LegacySpecifiedBackfillDisabledError(RuntimeError):
    """Raised when obsolete automatic population of 指定良损 is requested."""


def backfill_sheet(
    table_df: pd.DataFrame,
    override_map: dict[tuple[str, str], float],
    baseline_map: dict[tuple[str, str], float],
) -> NoReturn:
    """Reject the retired migration without mutating the supplied table."""
    del table_df, override_map, baseline_map
    raise LegacySpecifiedBackfillDisabledError(
        '历史“指定良损”自动回填已停用；该列仅允许人工维护。'
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description='已停用：禁止自动补全入库良率修饰表的“指定良损”列'
    )
    parser.add_argument("--product", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--dry-run", action="store_true", help=argparse.SUPPRESS)
    parser.parse_args()
    logging.basicConfig(level=logging.ERROR, format="%(levelname)s %(message)s")
    logging.error('历史“指定良损”自动回填已停用；该列仅允许人工维护。')
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
