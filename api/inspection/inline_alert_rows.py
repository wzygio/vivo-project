"""Publish HTTP-readable Inline rows from existing application results, without raw-data access."""
from datetime import date
import logging

from src.inline_domain import composition as inline
from src.inline_domain.core.monitor.cpk_summary import CPK_THRESHOLD
from src.inline_domain.infrastructure.monitor import published_alert_rows as store
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache, WorkbookVersionError
from src.shared_kernel.infrastructure.file_lock import publication_lock


def publish_event(product, scope, event, reference, reader):
    path = store.publication_path(product, scope, event)
    with publication_lock(path):
        # Read the retained ledger, including older weeks. No database is involved.
        window = dict(start_date="1678-01-03", end_date=reference.isoformat())
        if event in ("CPK", "CPM"):
            query = inline.build_capability_alert_query(event.lower(), workbook_reader=reader)
            result = query.read(product, reference, **window)
            rows = result["records"].assign(lower_bound=CPK_THRESHOLD)
        else:
            query = inline.build_sheet_alert_query(scope, event, workbook_reader=reader, strict_empty=True)
            result = query.read(scope, product, reference, **window)
            rows = result["records"]
        if result.get("metadata", {}).get("availability") == "missing":
            # A missing product tab is a valid empty publication. An absent or
            # unreadable workbook is an incomplete maintenance run.
            from src.inline_domain.infrastructure.shared.resource_paths import scope_decoration_path
            workbook = (ConfigLoader.get_domain_resource_path("inline_domain", "spc_cpk_cpm_decoration")
                        if event in ("CPK", "CPM") else scope_decoration_path(scope, event))
            workbook.stat()
        fields = ("prod_code", "factory", "step_id", "param_name", "sheet_id", "sheet_start_time",
                  "start_time", "tt_name", "rs_code", "chart_kind", "point_id", "oos_type", "ooc_type",
                  "period_type", "period_label", "cpk", "cpm", "lower_bound")
        rows = rows.loc[:, [name for name in fields if name in rows]].assign(flag=False)
        rows.attrs.clear()
        store.write_publication(path, rows)
    return len(rows)


def publish_scope(product, scope, reference, reader):
    events = ("OOS", "OOC", "CPK", "CPM") if scope == "spc" else ("OOS", "OOC")
    return {event: publish_event(product, scope, event, reference, reader) for event in events}


def main():
    reader = BoundedWorkbookCache()
    reference = date.today()
    failed = False
    for product in ConfigLoader.get_enabled_products():
        for scope in ("spc", "ctq", "aoi_tt", "aoi_rs"):
            try:
                try:
                    counts = publish_scope(product, scope, reference, reader)
                except WorkbookVersionError:
                    counts = publish_scope(product, scope, reference, reader)
                print(product, scope, counts)
            except Exception as exc:
                logging.getLogger(__name__).error("Inline alert publication failed: %s/%s (%s)",
                                                  product, scope, type(exc).__name__)
                failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
