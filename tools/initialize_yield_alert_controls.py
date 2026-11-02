"""Operator-only initialization of Yield Lot release controls; never run by API GET.

Existing publications receive default-deny decisions without refreshing their
publication age. This command does not run Yield analysis or query a database.
"""
from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
import logging
from pathlib import Path
import sys
from typing import Protocol

ROOT = Path(__file__).resolve().parents[1]
for import_path in (ROOT, ROOT / "src"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))


class LotControlStore(Protocol):
    def initialize_lot_controls(self, product: str) -> bool: ...


def _unavailable(code: str) -> int:
    print(json.dumps({"status": "unavailable", "error": code}))
    return 2


def main(
    argv: Sequence[str] | None = None, *, store: LotControlStore | None = None,
    enabled_products: Sequence[str] | None = None,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product", required=True, help="One enabled product code.")
    args = parser.parse_args(argv)

    try:
        if enabled_products is None:
            from src.shared_kernel.config import ConfigLoader

            enabled_products = ConfigLoader.get_enabled_products(strict=True)
        if args.product not in enabled_products:
            return _unavailable("invalid_product")
        if store is None:
            from src.yield_domain.composition import build_yield_alert_results

            store = build_yield_alert_results().store
        created = store.initialize_lot_controls(args.product)
    except Exception:
        # Driver errors and workbook exceptions can include paths or contents.
        return _unavailable("result_unavailable")

    print(json.dumps({"status": "success", "created": bool(created),
                      "initialized_products": int(bool(created))}))
    return 0


if __name__ == "__main__":
    logging.basicConfig(handlers=[logging.NullHandler()], force=True)
    raise SystemExit(main())
