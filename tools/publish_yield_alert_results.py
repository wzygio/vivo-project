"""Publish all/selected products through the same Yield producer used by the report page."""
from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from datetime import date
import json
import logging
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for import_path in (ROOT, ROOT / "src"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))


def publish_product(product: str, *, analysis_start_date: str = "", analysis_end_date: str = "",
                    kinds: tuple[str, ...] = ("trend", "lot")) -> dict:
    from app.components.indicator_cache import build_indicator_product_cache_signature
    from app.sections.yield_domain.group_trend_data import YIELD_SNAPSHOT_SIGNATURE_BASE
    from app.sections.yield_domain.resource_config import get_dashboard_resource_config
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager
    from src.yield_domain.application.alert_publication import generate_yield_alert_publications
    from src.yield_domain.composition import build_yield_alert_results
    from yield_domain.application.yield_service import YieldAnalysisService

    trend_signature = build_indicator_product_cache_signature(
        YIELD_SNAPSHOT_SIGNATURE_BASE, product, ("yield_trend_fluctuation",))
    lot_signature = build_indicator_product_cache_signature(
        YIELD_SNAPSHOT_SIGNATURE_BASE, product, ("yield_lot_oos",))
    config = get_dashboard_resource_config(ConfigLoader.load_config(product), trend_signature)
    product_dir = ConfigLoader.get_domain_resource_dir("yield_domain") / product
    context = YieldAnalysisService.build_cache_context(
        config, product_dir, resource_revision=YIELD_SNAPSHOT_SIGNATURE_BASE)
    if analysis_start_date or analysis_end_date:
        YieldAnalysisService.resolve_analysis_window(analysis_start_date, analysis_end_date)
        # August's month-over-month alarm needs July as a baseline. Publication
        # still contains only the requested period, and source reads stay bounded.
        baseline = (pd.Timestamp(analysis_start_date).replace(day=1)-pd.DateOffset(months=1)).date().isoformat()
        context = {**context, "analysis_start_date": baseline,
                   "analysis_end_date": analysis_end_date}
    publisher = build_yield_alert_results()
    publication_window = {"publication_start_date":analysis_start_date} if analysis_start_date else {}
    result = generate_yield_alert_publications(
        analysis=YieldAnalysisService, publisher=publisher, config=config,
        product_dir=product_dir, cache_context=context, trend_signature=trend_signature,
        lot_signature=lot_signature, db_manager=DatabaseManager(), kinds=kinds, **publication_window)
    states = {kind: publisher.read(product, kind, date.fromisoformat(context["analysis_end_date"])).status
              for kind in kinds}
    failed_kinds = (set(result["errors"]) | set(result["publication_errors"])
                    | {kind for kind, state in states.items() if state not in {"ready", "no_data"}})
    failed = bool(failed_kinds)
    return {"product": product, "status": "partial" if failed else "success",
            "failed_kinds": sorted(failed_kinds), "result_states": states,
            "analysis_start_date": analysis_start_date or context["analysis_start_date"],
            "baseline_start_date": context["analysis_start_date"],
            "analysis_end_date": context["analysis_end_date"]}


def main(argv: Sequence[str] | None = None, *, producer: Callable[..., dict] | None = None) -> int:
    from src.shared_kernel.config import ConfigLoader
    parser = argparse.ArgumentParser(description=__doc__)
    enabled = ConfigLoader.get_enabled_products(strict=True)
    parser.add_argument("--products", nargs="+", choices=enabled, help="Default: all enabled products.")
    parser.add_argument("--check", action="store_true", help="Validate paths only; no data reads or writes.")
    parser.add_argument("--start-date", help="Inclusive analysis start (YYYY-MM-DD); requires --end-date.")
    parser.add_argument("--end-date", help="Inclusive analysis end (YYYY-MM-DD); requires --start-date.")
    args = parser.parse_args(argv)
    window = {}
    if args.start_date or args.end_date:
        try:
            start, end = date.fromisoformat(args.start_date or ""), date.fromisoformat(args.end_date or "")
        except ValueError:
            parser.error("Both --start-date and --end-date must be ISO dates.")
        if start > end:
            parser.error("Start date must not be after end date.")
        window = {"analysis_start_date": start.isoformat(), "analysis_end_date": end.isoformat()}
    if args.check:
        from src.yield_domain.composition import build_yield_alert_results
        build_yield_alert_results()
        print(json.dumps({"status": "checked", "products": args.products or enabled}))
        return 0
    failures = False
    for product in args.products or enabled:
        try:
            result = (producer or publish_product)(product, **window)
        except Exception as exc:
            # Exception text may contain SQL, paths or connection details.
            result = {"product": product, "status": "failed", "error_type": type(exc).__name__}
        failures |= result["status"] != "success"
        print(json.dumps(result, ensure_ascii=False))
    return 1 if failures or not enabled else 0


if __name__ == "__main__":
    logging.basicConfig(handlers=[logging.NullHandler()], force=True)
    raise SystemExit(main())
