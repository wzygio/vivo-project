"""Existing report calculations publish Lot/trend results independently of charts."""
from datetime import date
import hashlib
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.application.alert_service import AlertService
from src.yield_domain.core.sheet_lot.lot_alerts import compute_lot_oos_records

logger = logging.getLogger(__name__)


def generate_yield_alert_publications(
    *, analysis: Any, publisher: YieldAlertResults, config: Any, product_dir: Path,
    cache_context: dict[str, str], trend_signature: str, lot_signature: str, db_manager: Any,
) -> dict[str, Any]:
    """Use the same service calls/cache keys as the report; never load Sheet/Mapping.

    Invoked outside calculation caches, so a missing result file is recreated even
    when the report inputs hit memory. Errors are isolated per result kind.
    """
    product = config.data_source.product_code
    reference = date.fromisoformat(cache_context["analysis_end_date"])
    common = {"analysis_start_date": cache_context["analysis_start_date"],
              "analysis_end_date": cache_context["analysis_end_date"]}
    outcome: dict[str, Any] = {
        "mwd_group_data": {}, "mwd_code_data": {}, "lot_data": None, "warning_lines": {},
        "lot_records": [], "trend_records": [], "total_recent_lots": 0,
        "health": {}, "errors": {}, "publication_errors": {},
    }
    for kind, signature in (("trend", trend_signature), ("lot", lot_signature)):
        try:
            initial_policy = publisher.policy_signature(product)
            health = analysis.get_data_health(config, _db_manager=db_manager,
                snapshot_signature=signature, **common)
            outcome["health"][kind] = health
            if kind == "trend":
                kwargs = {"_db_manager": db_manager, "snapshot_signature": signature,
                          "modifier_signature": cache_context["modifier_signature"], **common}
                outcome["mwd_group_data"] = analysis.get_mwd_trend_data(config, product_dir, **kwargs)
                outcome["mwd_code_data"] = analysis.get_code_level_trend_data(config, product_dir, **kwargs)
                records = AlertService.get_dashboard_alert_records(
                    outcome["mwd_group_data"] or {}, outcome["mwd_code_data"] or {})
                has_data = any(isinstance(frame, pd.DataFrame) and not frame.empty for group in (
                    outcome["mwd_group_data"] or {}, outcome["mwd_code_data"] or {}) for frame in group.values())
                outcome["trend_records"] = records
                coverage_start = common["analysis_start_date"]
            else:
                outcome["lot_data"] = analysis.get_lot_defect_rates(config, product_dir,
                    _db_manager=db_manager, snapshot_signature=signature, **cache_context)
                outcome["warning_lines"] = analysis.load_static_warning_lines(config, product_dir,
                    signature, warning_signature=cache_context["warning_signature"])
                records, count = compute_lot_oos_records(outcome["lot_data"], outcome["warning_lines"])
                outcome["lot_records"], outcome["total_recent_lots"] = records, count
                has_data = count > 0
                coverage_start = _lot_scan_start(outcome["lot_data"], common["analysis_start_date"])
            calculation = hashlib.sha256(repr((signature, sorted(cache_context.items()))).encode()).hexdigest()
            try:
                if publisher.policy_signature(product) != initial_policy:
                    raise ValueError("Yield policy changed during calculation")
                publisher.publish(product, kind, records, reference_date=reference,
                    coverage_start=coverage_start, coverage_end=common["analysis_end_date"],
                    has_data=has_data, health=health, calculation_signature=calculation)
            except Exception as exc:
                logger.error("YIELD_ALERT_PUBLICATION_FAILED kind=%s type=%s", kind, type(exc).__name__)
                outcome["publication_errors"][kind] = "YIELD_ALERT_PUBLICATION_FAILED"
                _mark_failed(publisher, product, kind, reference, outcome["publication_errors"][kind], outcome)
        except Exception as exc:
            logger.error("YIELD_ALERT_ANALYSIS_FAILED kind=%s type=%s", kind, type(exc).__name__)
            outcome["errors"][kind] = "YIELD_ALERT_ANALYSIS_FAILED"
            _mark_failed(publisher, product, kind, reference, outcome["errors"][kind], outcome)
    return outcome


def _mark_failed(publisher: YieldAlertResults, product: str, kind: str, reference: date,
                 code: str, outcome: dict) -> None:
    try:
        publisher.mark_failed(product, kind, reference, code)
    except Exception as exc:
        logger.error("YIELD_ALERT_FAILURE_STATUS_NOT_SAVED kind=%s type=%s", kind, type(exc).__name__)
        outcome["publication_errors"][kind] = "YIELD_ALERT_FAILURE_STATUS_NOT_SAVED"


def _lot_scan_start(lot_data: dict | None, analysis_start: str) -> str:
    latest = []
    for frame in (lot_data or {}).get("code_level_details", {}).values():
        if not frame.empty and {"lot_id", "warehousing_time"}.issubset(frame.columns):
            parsed = pd.to_datetime(frame["warehousing_time"], format="%Y%m%d", errors="coerce").max()
            if pd.notna(parsed):
                latest.append(parsed)
    start = pd.Timestamp(analysis_start)
    if latest:
        start = max(start, max(latest) - pd.Timedelta(days=30))
    return start.date().isoformat()
