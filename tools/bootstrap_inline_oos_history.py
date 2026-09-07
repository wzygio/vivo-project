"""Bootstrap long-term Inline OOS Parquet history from current audit workbooks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for search_path in (PROJECT_ROOT, PROJECT_ROOT / "src"):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))

from src.inline_domain.application.shared.oos_history_service import (  # noqa: E402
    SCOPE_DECORATION_FILE_NAME,
)
from src.inline_domain.composition import build_oos_history_service  # noqa: E402
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (  # noqa: E402
    load_sheet_oos_decoration,
)
from src.shared_kernel.config import ConfigLoader  # noqa: E402
from src.inline_domain.infrastructure.shared.resource_paths import (  # noqa: E402
    scope_resource_dir,
)


def bootstrap(products: list[str], scopes: list[str], *, dry_run: bool) -> int:
    service = build_oos_history_service()
    migrated = 0
    for scope in scopes:
        resource_dir = scope_resource_dir(scope)
        contract = service.scope_contract(scope)
        for product in products:
            frame = load_sheet_oos_decoration(
                resource_dir,
                file_name=SCOPE_DECORATION_FILE_NAME[scope],
                sheet_name=product,
                key_columns=contract.key_columns,
            )
            if frame.empty or contract.event_time_column not in frame.columns:
                print(f"SKIP {scope}/{product}: no current facts")
                continue
            times = pd.to_datetime(frame[contract.event_time_column], errors="coerce").dropna()
            if times.empty:
                print(f"SKIP {scope}/{product}: no valid event time")
                continue
            coverage_start = times.min().normalize()
            coverage_end = times.max().normalize() + pd.Timedelta(days=1)
            print(
                f"{'DRY-RUN' if dry_run else 'MIGRATE'} {scope}/{product}: "
                f"rows={len(frame)} window=[{coverage_start}, {coverage_end})"
            )
            if not dry_run:
                service.update_history(
                    scope,
                    product,
                    frame,
                    coverage_start=coverage_start,
                    coverage_end=coverage_end,
                )
            migrated += 1
    return migrated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--products", nargs="*", default=ConfigLoader.get_enabled_products()
    )
    parser.add_argument(
        "--scopes", nargs="*", default=["spc", "ctq", "aoi_tt", "aoi_rs"]
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    migrated = bootstrap(args.products, args.scopes, dry_run=args.dry_run)
    print(f"Completed: {migrated} scope/product snapshots")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
