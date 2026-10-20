"""Publish all/selected products through the same Yield producer used by the report page."""
from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
import json
import logging
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for import_path in (ROOT, ROOT / "src"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))


def publish_product(product: str) -> dict:
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
    result = generate_yield_alert_publications(
        analysis=YieldAnalysisService, publisher=build_yield_alert_results(), config=config,
        product_dir=product_dir, cache_context=context, trend_signature=trend_signature,
        lot_signature=lot_signature, db_manager=DatabaseManager())
    failed = bool(result["errors"] or result["publication_errors"]
                  or any(health.get("status") != "fresh" for health in result["health"].values()))
    return {"product": product, "status": "partial" if failed else "success",
            "failed_kinds": sorted(set(result["errors"]) | set(result["publication_errors"]))}


def main(argv: Sequence[str] | None = None, *, producer: Callable[[str], dict] | None = None) -> int:
    from src.shared_kernel.config import ConfigLoader
    parser = argparse.ArgumentParser(description=__doc__)
    enabled = ConfigLoader.get_enabled_products(strict=True)
    parser.add_argument("--products", nargs="+", choices=enabled, help="Default: all enabled products.")
    parser.add_argument("--check", action="store_true", help="Validate paths only; no data reads or writes.")
    args = parser.parse_args(argv)
    if args.check:
        from src.yield_domain.composition import build_yield_alert_results
        build_yield_alert_results()
        print(json.dumps({"status": "checked", "products": args.products or enabled}))
        return 0
    failures = False
    for product in args.products or enabled:
        try:
            result = (producer or publish_product)(product)
        except Exception as exc:
            # Exception text may contain SQL, paths or connection details.
            result = {"product": product, "status": "failed", "error_type": type(exc).__name__}
        failures |= result["status"] != "success"
        print(json.dumps(result, ensure_ascii=False))
    return 1 if failures or not enabled else 0


if __name__ == "__main__":
    logging.basicConfig(handlers=[logging.NullHandler()], force=True)
    raise SystemExit(main())
