"""Refresh expired Yield/IJP publications independently of alert display readers.

Uses the existing report producers. Valid publications require no database access;
this command is deliberately separate from the file-only matrix warmup.
"""
from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
import sys
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
for import_path in (ROOT, ROOT / "src"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

YIELD_ROWS = {"trend": "yield_trend_fluctuation", "lot": "yield_lot_oos"}
IJP_ROWS = ("ijp_aa_oos", "ijp_hole_oos")


def publish_ijp_region(row_key: str) -> None:
    from sqlalchemy import event
    from src.indicator_domain.composition import build_ijp_service, build_ijp_hole_service
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

    builder = {"ijp_aa_oos": build_ijp_service, "ijp_hole_oos": build_ijp_hole_service}[row_key]
    manager = DatabaseManager()
    if manager.engine is None:
        raise RuntimeError("IJP source unavailable")
    engine = manager.engine

    def set_timeout(connection, record, proxy):
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = 120000")

    event.listen(engine, "checkout", set_timeout)
    try:
        # Existing producers constrain time, products, equipment, codes and orders.
        builder(manager).get_alert_report()
    finally:
        event.remove(engine, "checkout", set_timeout)
        engine.dispose()


def refresh_results(products: Sequence[str], reference_date: date, *, reader: Callable,
                    yield_producer: Callable, ijp_producer: Callable) -> list[dict]:
    outcomes = []

    def valid(row: str, product: str) -> bool:
        state = reader(row, product, reference_date)["state"]
        return state in ({"ok", "alert"} if row in IJP_ROWS else {"ok", "alert", "no_data"})

    for product in products:
        outcome = {"module": "yield", "product": product}
        try:
            kinds = tuple(kind for kind, row in YIELD_ROWS.items() if not valid(row, product))
            if kinds:
                yield_producer(product, kinds=kinds)
            good = all(valid(row, product) for row in YIELD_ROWS.values())
            outcome["status"] = ("success" if kinds else "current") if good else "failed"
        except Exception as exc:
            outcome.update(status="failed", error_type=type(exc).__name__)
        outcomes.append(outcome)
    for row in IJP_ROWS:
        outcome = {"module": row}
        try:
            current = all(valid(row, product) for product in products)
            if not current:
                ijp_producer(row)  # Each region publishes every enabled product once.
            good = all(valid(row, product) for product in products)
            outcome["status"] = ("current" if current else "success") if good else "failed"
        except Exception as exc:
            outcome.update(status="failed", error_type=type(exc).__name__)
        outcomes.append(outcome)
    return outcomes


def main(argv: Sequence[str] | None = None, *, reader: Callable | None = None,
         yield_producer: Callable | None = None, ijp_producer: Callable | None = None,
         log_dir: Path | None = None) -> int:
    from src.shared_kernel.config import ConfigLoader

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Validate paths only; no business reads or writes.")
    args = parser.parse_args(argv)
    products = ConfigLoader.get_enabled_products(strict=True)
    if not products:
        raise ValueError("No enabled products")
    for domain, key in (("yield_domain", "lot_oos_results"),
                        ("yield_domain", "trend_alert_results"), ("indicator_domain", "ijp_alerts")):
        ConfigLoader.get_domain_resource_path(domain, key)
    if args.check:
        if sys.stdout is not None:
            print(json.dumps({"status": "checked", "products": products}))
        return 0
    if reader is None:
        from app.sections.shared.alert_results import read_alert_result
        reader = read_alert_result
    if yield_producer is None:
        from tools.publish_yield_alert_results import publish_product
        yield_producer = publish_product
    started = datetime.now()
    directory = log_dir or ROOT / "output/logs/alert_result_publication"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{started:%Y%m%d-%H%M%S}-{uuid4().hex[:8]}.json"
    report = {"started_at": started.isoformat(), "status": "running"}
    path.write_text(json.dumps(report), encoding="utf-8")
    # Lock the whole job, including preflight: duplicate manual/scheduled invocations
    # must not simultaneously query and publish the same enabled product scope.
    from src.shared_kernel.infrastructure.file_lock import publication_lock
    try:
        with publication_lock(directory / "publication-job"):
            outcomes = refresh_results(products, date.today(), reader=reader,
                                       yield_producer=yield_producer,
                                       ijp_producer=ijp_producer or publish_ijp_region)
        failed = any(item["status"] == "failed" for item in outcomes)
        report.update(status="partial" if failed else "success", results=outcomes)
    except Exception as exc:
        failed = True
        report.update(status="failed", error_type=type(exc).__name__)
    report["finished_at"] = datetime.now().isoformat()
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if sys.stdout is not None:
        print(json.dumps(report, ensure_ascii=False))
    return int(failed)


if __name__ == "__main__":
    os.chdir(ROOT)
    logging.basicConfig(handlers=[logging.NullHandler()], force=True)
    raise SystemExit(main())
