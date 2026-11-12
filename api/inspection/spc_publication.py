"""Publish SPC By Sheet membership through the normal bounded report workflow.

This maintenance command is separate from HTTP. It uses the same preparation,
capability publication and member publication as the SPC report page.
"""
from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from datetime import date, timedelta
import json
import logging
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
for import_path in (ROOT, ROOT / "src"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))


def publish_product(product: str, start: date, reference: date) -> dict:
    """Run an ordinary SPC report with finite database statement timeouts."""
    from sqlalchemy import event
    from src.inline_domain.application.spc.dtos import SpcQueryConfig
    from src.inline_domain.application.spc.spc_service import SpcReportService
    from src.inline_domain.composition import (
        build_spc_input_signature, build_spc_repository,
        build_spc_sheet_membership_policy_signature, build_spc_sheet_membership_store,
        build_spc_sheet_membership_coverage,
    )
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

    manager = DatabaseManager()
    engine = manager.engine
    if engine is None:
        raise OSError("SPC source unavailable")

    def set_timeout(connection, record, proxy):
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = 60000")
            cursor.execute("SET default_transaction_read_only = on")

    event.listen(engine, "checkout", set_timeout)
    try:
        config = SpcQueryConfig(prod_code=product, start_date=start.isoformat(),
                                end_date=reference.isoformat(), data_type_filter="SPC")
        store = build_spc_sheet_membership_store()
        report = SpcReportService.get_spc_report_data(
            _data_port=build_spc_repository(manager, product),
            query_config_json=config.model_dump_json(),
            snapshot_signature=build_spc_input_signature(product),
            _sheet_membership_store=store,
            sheet_membership_policy_signature=build_spc_sheet_membership_policy_signature(),
            _sheet_membership_coverage=build_spc_sheet_membership_coverage(product),
        )
        previous_monday = reference - timedelta(days=reference.weekday() + 7)
        published = store.read(product, previous_monday)
        if (published is None
                or published.get("policy_signature") != build_spc_sheet_membership_policy_signature()):
            raise ValueError("Target SPC member week was not published")
        year, week, _ = previous_monday.isocalendar()
        return {"product": product, "status": "success",
                "verified_week": f"{year}-W{week:02d}",
                "prepared_points": len(report.raw_measurements_df),
                "prepared_sheets": int(report.raw_measurements_df["sheet_id"].nunique())
                if "sheet_id" in report.raw_measurements_df else 0}
    finally:
        event.remove(engine, "checkout", set_timeout)
        engine.dispose()


def main(argv: Sequence[str] | None = None, *, producer: Callable | None = None,
         today: date | None = None) -> int:
    from src.inline_domain.core.spc.spc_calculator import get_period_window_start
    from src.shared_kernel.config import ConfigLoader

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product", action="append", help="Enabled product; repeat to select several. Default: all enabled products.")
    parser.add_argument("--reference-date", type=date.fromisoformat,
                        help="Report reference day (YYYY-MM-DD), default today. Capability week is its previous ISO week.")
    parser.add_argument("--start-date", type=date.fromisoformat,
                        help="Prepared point window start, default first day of previous month; maximum 93 days.")
    parser.add_argument("--check", action="store_true", help="Validate selection and dates only, without business reads, DB or writes.")
    args = parser.parse_args(argv)
    current = today or date.today()
    reference = args.reference_date or current
    start = args.start_date or get_period_window_start(reference)
    previous_monday = reference - timedelta(days=reference.weekday() + 7)
    enabled = ConfigLoader.get_enabled_products()
    products = list(dict.fromkeys(args.product or enabled))
    if not products or any(product not in enabled for product in products):
        parser.error("Select enabled products only.")
    if reference > current or start > previous_monday or (reference - start).days > 93:
        parser.error("Use a past or current reference, cover its complete previous ISO week, and limit the window to 93 days.")
    if args.check:
        print(json.dumps({"status": "validated", "products": products,
                          "start_date": start.isoformat(), "reference_date": reference.isoformat()}))
        return 0
    produce = producer or publish_product
    results = []
    for product in products:
        try:
            results.append(produce(product, start, reference))
        except Exception as exc:
            logging.getLogger(__name__).error("SPC_MEMBERSHIP_SYNC_FAILED product=%s exception_type=%s",
                                             product, type(exc).__name__)
            results.append({"product": product, "status": "error", "code": "SPC_MEMBERSHIP_SYNC_FAILED"})
    success = all(item["status"] == "success" for item in results)
    print(json.dumps({"status": "success" if success else "error", "results": results}, ensure_ascii=False))
    return 0 if success else 2


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
