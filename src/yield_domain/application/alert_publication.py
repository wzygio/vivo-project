"""Existing report calculations publish Lot/trend results independently of charts."""
from datetime import date, timedelta
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
    publication_start_date: str | None = None,
    kinds: tuple[str, ...] = ("trend", "lot"),
) -> dict[str, Any]:
    """Use the same service calls/cache keys as the report; never load Sheet/Mapping.

    Invoked outside calculation caches, so a missing result file is recreated even
    when the report inputs hit memory. Errors are isolated per result kind.
    """
    product = config.data_source.product_code
    if not kinds or set(kinds) - {"lot", "trend"}:
        raise ValueError("Invalid Yield publication kinds")
    reference = date.fromisoformat(cache_context["analysis_end_date"])
    common = {"analysis_start_date": cache_context["analysis_start_date"],
              "analysis_end_date": cache_context["analysis_end_date"]}
    publish_start = publication_start_date or common["analysis_start_date"]
    if not (common["analysis_start_date"] <= publish_start <= common["analysis_end_date"]):
        raise ValueError("Publication window must be inside the calculation window")
    outcome: dict[str, Any] = {
        "mwd_group_data": {}, "mwd_code_data": {}, "lot_data": None, "warning_lines": {},
        "lot_records": [], "trend_records": [], "total_recent_lots": 0,
        "health": {}, "errors": {}, "publication_errors": {},
    }
    for kind in kinds:
        signature = trend_signature if kind == "trend" else lot_signature
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
                outcome["trend_records"] = AlertService.get_dashboard_alert_records(
                    outcome["mwd_group_data"] or {}, outcome["mwd_code_data"] or {})
                records = AlertService.get_dashboard_alert_history(
                    outcome["mwd_group_data"] or {}, outcome["mwd_code_data"] or {})
                records = [record for record in records if _trend_period_overlaps(record, publish_start,
                           common["analysis_end_date"])]
                has_data = any(isinstance(frame, pd.DataFrame) and not frame.empty for group in (
                    outcome["mwd_group_data"] or {}, outcome["mwd_code_data"] or {}) for frame in group.values())
                coverage_start = publish_start
            else:
                outcome["lot_data"] = analysis.get_lot_defect_rates(config, product_dir,
                    _db_manager=db_manager, snapshot_signature=signature, **cache_context)
                outcome["warning_lines"] = analysis.load_static_warning_lines(config, product_dir,
                    signature, warning_signature=cache_context["warning_signature"])
                outcome["lot_records"], outcome["total_recent_lots"] = compute_lot_oos_records(
                    outcome["lot_data"], outcome["warning_lines"])
                records, count = compute_lot_oos_records(
                    outcome["lot_data"], outcome["warning_lines"], time_period=None)
                records = [record for record in records if publish_start <=
                    date.fromisoformat(record["入库时间"].replace("/", "-")).isoformat() <= common["analysis_end_date"]]
                has_data = count > 0
                coverage_start = publish_start
            calculation = hashlib.sha256(repr((signature, sorted(cache_context.items()))).encode()).hexdigest()
            try:
                if publisher.policy_signature(product) != initial_policy:
                    raise ValueError("Yield policy changed during calculation")
                selection_metadata = {}
                if kind == "lot" and outcome["lot_data"]:
                    detail_frames = outcome["lot_data"].get("code_level_details", {}).values()
                    source_dates = [pd.to_datetime(frame["warehousing_time"], format="%Y%m%d", errors="coerce")
                                    for frame in detail_frames if not frame.empty and "warehousing_time" in frame]
                    latest = pd.concat(source_dates, ignore_index=True).max() if source_dates else pd.NaT
                    if pd.notna(latest):
                        selection_metadata["lot_reference_date"] = latest.date().isoformat()
                publisher.publish(product, kind, records, reference_date=reference,
                    coverage_start=coverage_start, coverage_end=common["analysis_end_date"],
                    has_data=has_data, health=health, calculation_signature=calculation, **selection_metadata)
            except Exception as exc:
                logger.error("YIELD_ALERT_PUBLICATION_FAILED kind=%s type=%s", kind, type(exc).__name__)
                outcome["publication_errors"][kind] = "YIELD_ALERT_PUBLICATION_FAILED"
                _mark_failed(publisher, product, kind, reference, outcome["publication_errors"][kind], outcome)
        except Exception as exc:
            logger.error("YIELD_ALERT_ANALYSIS_FAILED kind=%s type=%s", kind, type(exc).__name__)
            outcome["errors"][kind] = "YIELD_ALERT_ANALYSIS_FAILED"
            _mark_failed(publisher, product, kind, reference, outcome["errors"][kind], outcome)
    return outcome


def _trend_period_overlaps(record: dict, start: str, end: str) -> bool:
    value = str(record["time_period"])
    if record["period_scope"] == "monthly":
        period = pd.Period(value.removesuffix("月"), freq="M")
        first, last = period.start_time.date(), period.end_time.date()
    else:
        year, week = value.split("-W")
        first = date.fromisocalendar(int(year), int(week), 1)
        last = first + timedelta(days=6)
    return first <= date.fromisoformat(end) and last >= date.fromisoformat(start)


def _mark_failed(publisher: YieldAlertResults, product: str, kind: str, reference: date,
                 code: str, outcome: dict) -> None:
    try:
        publisher.mark_failed(product, kind, reference, code)
    except Exception as exc:
        logger.error("YIELD_ALERT_FAILURE_STATUS_NOT_SAVED kind=%s type=%s", kind, type(exc).__name__)
        outcome["publication_errors"][kind] = "YIELD_ALERT_FAILURE_STATUS_NOT_SAVED"
