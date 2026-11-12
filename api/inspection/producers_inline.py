"""Headless Inline production through the ordinary report application services.

One product refresh feeds SPC/CTQ/TT; RS retains its independent source pair.
Every scope publishes before validating generated artifacts, including explicit
empty products. Consumer ``ok`` alone is never evidence that production ran.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
from typing import Callable, Iterator
from uuid import uuid4

import pandas as pd


INLINE_SCOPES = ("spc", "ctq", "aoi_tt", "aoi_rs")
_VERIFICATION_CODES = {
    "Source refresh fell back to an older snapshot": "INLINE_SOURCE_FALLBACK",
    "Source snapshot does not cover the report window": "INLINE_SOURCE_UNCOVERED",
    "RS source refresh was not successful": "INLINE_RS_SOURCE_REFRESH_FAILED",
    "Applicable product specifications unavailable": "INLINE_SPECIFICATIONS_MISSING",
    "Generated alarm ledger is not from this production run": "INLINE_LEDGER_GENERATION_MISMATCH",
    "Generated alarm ledger did not validate": "INLINE_LEDGER_INVALID",
    "Generated capability workbook missing": "INLINE_CAPABILITY_MISSING",
    "Obsolete capability records remain in an empty source week": "INLINE_STALE_CAPABILITY_WEEK",
    "Generated capability result did not validate": "INLINE_CAPABILITY_INVALID",
    "SPC complete-week membership did not validate": "INLINE_MEMBERSHIP_INVALID",
    "Trend result publication did not validate": "INLINE_TREND_INVALID",
    "SPC logical preparation policy changed during production": "INLINE_MEMBERSHIP_POLICY_CHANGED",
}
_WORKBOOK_READ_BOUNDARIES = {
    "CapabilityDecorationReadError", "SheetOosDecorationReadError", "ExcelAlarmReadError",
    "WorkbookReadError", "OutlierFilterConfigurationError",
}


def _exception_chain(error: Exception) -> list[BaseException]:
    nodes, seen = [], set()
    node: BaseException | None = error
    while node is not None and id(node) not in seen and len(nodes) < 6:
        nodes.append(node)
        seen.add(id(node))
        node = node.__cause__ if node.__cause__ is not None else node.__context__
    return nodes


def _safe_error_chain(error: Exception) -> list[dict]:
    """Retain code locations/types only; exception messages are never persisted."""
    records = []
    for node in _exception_chain(error):
        trace = node.__traceback__
        if trace is None:
            continue
        while trace.tb_next is not None:
            trace = trace.tb_next
        records.append({"type": type(node).__name__,
                        "file": Path(trace.tb_frame.f_code.co_filename).name,
                        "line": trace.tb_lineno})
    return records


def _retryable_workbook_read(error: Exception) -> bool:
    """Retry only proven file-version or protected-file read transport failures."""
    chain = _exception_chain(error)
    if any(type(node).__name__ == "WorkbookVersionError" for node in chain):
        return True
    if not any(type(node).__name__ in _WORKBOOK_READ_BOUNDARIES for node in chain):
        return False
    return any(isinstance(node, PermissionError)
               or type(node).__name__ == "com_error"
               or (type(node).__module__ == "pywintypes" and type(node).__name__ == "error")
               for node in chain)


def _configure_read_only_connection(connection, record=None, proxy=None) -> None:
    """Configure DBAPI's next transactions before executing any source query."""
    connection.set_session(readonly=True)
    with connection.cursor() as cursor:
        cursor.execute("SET statement_timeout = 120000")
    # Commit only the session-setting statement. No manufacturing SQL has run.
    connection.commit()


def produce_inline(product: str, reference_date: date, *,
                   scopes: tuple[str, ...] = INLINE_SCOPES,
                   _session_factory: Callable | None = None) -> list[dict]:
    """Produce full-product results, isolating failures per business module."""
    if not product or not scopes or len(set(scopes)) != len(scopes) or set(scopes) - set(INLINE_SCOPES):
        raise ValueError("Invalid Inline inspection scope")
    outcomes = []
    factory = _session_factory or _production_session
    try:
        with factory(product, reference_date) as session:
            for scope in scopes:
                outcome = {"module": scope, "product": product}
                try:
                    session.refresh(scope)
                    try:
                        artifacts = session.produce(scope)
                    except Exception as first_error:
                        if not _retryable_workbook_read(first_error):
                            raise
                        # The source was already proven fresh. Retry no acquisition;
                        # use a new computation generation and a cleared readback.
                        session.retry(scope)
                        outcome["retry_count"] = 1
                        artifacts = session.produce(scope)
                    outcome.update(status="success", **artifacts)
                except Exception as exc:
                    # Driver/Office exception messages can contain private data.
                    phase = getattr(session, "phase", "PRODUCTION")
                    if phase not in ("SOURCE_REFRESH", "REPORT", "LEDGER_VERIFICATION", "CAPABILITY_VERIFICATION", "TREND_PUBLICATION"):
                        phase = "PRODUCTION"
                    outcome.update(status="failed", error_type=type(exc).__name__,
                                   code=_VERIFICATION_CODES.get(str(exc), f"INLINE_{phase}_FAILED"),
                                   error_chain=_safe_error_chain(exc))
                outcomes.append(outcome)
    except Exception as exc:
        # Opening a DB runtime must still produce an outcome for every scope.
        completed = {item["module"] for item in outcomes}
        outcomes.extend({"module": scope, "product": product, "status": "failed",
                         "error_type": type(exc).__name__, "code": "INLINE_RUNTIME_UNAVAILABLE"}
                        for scope in scopes if scope not in completed)
    return outcomes


@contextmanager
def _production_session(product: str, reference_date: date) -> Iterator["InlineProductionSession"]:
    from sqlalchemy import event
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

    manager = DatabaseManager()
    engine = manager.engine
    if engine is None:
        raise OSError("Inline source unavailable")

    event.listen(engine, "checkout", _configure_read_only_connection)
    try:
        yield InlineProductionSession(manager, product, reference_date)
    finally:
        event.remove(engine, "checkout", _configure_read_only_connection)
        engine.dispose()


class InlineProductionSession:
    """Own one source refresh generation and the report adapters for one product."""

    def __init__(self, manager: object, product: str, reference_date: date):
        from src.inline_domain.core.spc.spc_calculator import get_period_window_start

        self.manager, self.product, self.reference = manager, product, reference_date
        self.start = get_period_window_start(reference_date)
        self.revision = f"inspection:{reference_date.isoformat()}:{uuid4().hex}"
        self._refreshes: dict[str, Exception | None] = {}
        self.phase = "SOURCE_REFRESH"
        self._spc_policy = None
        self._spc_logical_policy = None
        self._spc_source_proof = None
        from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache
        self._readback = BoundedWorkbookCache()

    def refresh(self, scope: str) -> None:
        from src.inline_domain import composition as inline

        self.phase = "SOURCE_REFRESH"
        group = "rs" if scope == "aoi_rs" else "shared"
        if group not in self._refreshes:
            try:
                if group == "shared":
                    result = inline.build_raw_measurement_repository(self.manager, self.product).refresh_measurements(
                        self.product, self.reference.isoformat())
                    if not result.refreshed_from_db:
                        raise ValueError("Source refresh fell back to an older snapshot")
                    proof = inline.build_spc_sheet_membership_coverage(self.product)()
                    if (pd.Timestamp(proof["start_date"]).date() > self.start
                            or pd.Timestamp(proof["end_date"]).date() < self.reference):
                        raise ValueError("Source snapshot does not cover the report window")
                else:
                    query = self._query(scope)
                    if not inline.build_aoi_rs_repository(self.manager, self.product,
                                                         require_complete_coverage=True).refresh(query):
                        raise ValueError("RS source refresh was not successful")
                self._refreshes[group] = None
            except Exception as exc:
                self._refreshes[group] = exc
        failure = self._refreshes[group]
        if failure is not None:
            raise failure

    def _query(self, scope: str):
        from src.inline_domain.application.spc.dtos import SpcQueryConfig
        from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
        from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig

        fields = {"prod_code": self.product, "start_date": self.start.isoformat(),
                  "end_date": self.reference.isoformat()}
        if scope in ("spc", "ctq"):
            return SpcQueryConfig(**fields, data_type_filter=scope.upper())
        return {"aoi_tt": AoiTtQueryConfig, "aoi_rs": AoiRsQueryConfig}[scope](**fields)

    def retry(self, scope: str) -> None:
        """Begin one new result generation without refreshing the proven source."""
        self.revision = f"inspection:{self.reference.isoformat()}:{uuid4().hex}"
        self._readback.clear()
        if scope == "spc":
            self._spc_policy = None
            self._spc_logical_policy = None
            self._spc_source_proof = None

    def produce(self, scope: str) -> dict:
        from src.inline_domain import composition as inline
        from src.inline_domain.application.spc.spc_service import SpcReportService
        from src.inline_domain.application.ctq.ctq_service import CtqReportService
        from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportService
        from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportService

        self.phase = "REPORT"
        builders = {"spc": inline.build_spc_repository, "ctq": inline.build_ctq_repository,
                    "aoi_tt": inline.build_aoi_tt_repository, "aoi_rs": inline.build_aoi_rs_repository}
        port = builders[scope](self.manager, self.product)
        query = self._query(scope)
        spec_loader = (port.get_spc_spec_limits if scope in ("spc", "ctq") else
                       port.get_tt_spec_limits if scope == "aoi_tt" else port.get_rs_spec_limits)
        specifications = spec_loader(self.product)
        if not isinstance(specifications, pd.DataFrame) or specifications.empty:
            raise ValueError("Applicable product specifications unavailable")
        kwargs = {"_data_port": port, "query_config_json": query.model_dump_json(),
                  "snapshot_signature": self.revision, "product_revision": self.revision}
        if scope == "spc":
            # Loading the protected rule workbook can activate the enterprise
            # file view and change stat size without changing logical rules.
            # Resolve that view before taking the ordinary membership signature.
            self._spc_logical_policy = self._logical_membership_policy()
            self._spc_policy = inline.build_spc_sheet_membership_policy_signature()
            owner = self
            class PreparedPort:
                def __getattr__(self, name): return getattr(port, name)
                def get_spc_measurements(self, config, force_refresh=False):
                    frame = port.get_spc_measurements(config, force_refresh)
                    proof = frame.attrs.get("spc_source_coverage")
                    owner._spc_source_proof = dict(proof) if isinstance(proof, dict) else None
                    return frame
            kwargs["_data_port"] = PreparedPort()
            kwargs.update(_sheet_membership_store=inline.build_spc_sheet_membership_store(),
                          sheet_membership_policy_signature=self._spc_policy,
                          _sheet_membership_coverage=inline.build_spc_sheet_membership_coverage(self.product))
        service = {"spc": SpcReportService.get_spc_report_data, "ctq": CtqReportService.get_ctq_report_data,
                   "aoi_tt": AoiTtReportService.get_aoi_tt_report_data,
                   "aoi_rs": AoiRsReportService.get_aoi_rs_report_data}[scope]
        report = service(**kwargs)
        if self._is_empty(scope, report):
            self._publish_empty_oos(scope, specifications)
        self.phase = "LEDGER_VERIFICATION"
        counts = self._verify_ledgers(scope)
        if scope == "spc":
            self.phase = "CAPABILITY_VERIFICATION"
            self._ensure_capability_headers()
            self._verify_capability(report)
            counts["verified_events"] = ["OOS", "OOC", "CPK", "CPM"]
        elif scope in ("aoi_tt", "aoi_rs"):
            self.phase = "TREND_PUBLICATION"
            publisher = inline.build_trend_alert_results(scope)
            publisher.publish(self.product, self.reference, report.trend_evaluation_df)
            trend = publisher.read(self.product, self.reference)
            if trend["state"] not in ("ok", "alert", "no_data"):
                raise ValueError("Trend result publication did not validate")
            counts["trend_state"] = trend["state"]
            counts["verified_events"] = ["OOS", "OOC", "trend"]
        else:
            counts["verified_events"] = ["OOS", "OOC"]
        return {**counts, "coverage_start": self.start.isoformat(),
                "coverage_end": self.reference.isoformat()}

    @staticmethod
    def _is_empty(scope: str, report: object) -> bool:
        column = {"spc": "raw_measurements_df", "ctq": "raw_measurements_df",
                  "aoi_tt": "tt_details_df", "aoi_rs": "rs_details_df"}[scope]
        frame = getattr(report, column)
        return isinstance(frame, pd.DataFrame) and frame.empty

    def _publish_empty_oos(self, scope: str, specifications: pd.DataFrame) -> None:
        """Existing report early returns do not always commit a zero generation."""
        from src.inline_domain.infrastructure.shared.resource_paths import scope_decoration_path
        from src.inline_domain.application.shared.sheet_oos_decoration_service import prepare_sheet_oos_decoration
        from src.inline_domain.application.aoi_tt.decoration_service import prepare_aoi_tt_decoration
        from src.inline_domain.application.aoi_rs.decoration_service import prepare_aoi_rs_decoration

        path = scope_decoration_path(scope, "OOS")
        common = {"scope": scope, "prod_code": self.product, "product_revision": self.revision}
        if scope in ("spc", "ctq"):
            prepare_sheet_oos_decoration(pd.DataFrame(), pd.DataFrame(), path.parent,
                                        decoration_file_name=path.name, decoration_sheet_name=self.product,
                                        force=True, **common)
        elif scope == "aoi_tt":
            prepare_aoi_tt_decoration(pd.DataFrame(), specifications, path.parent,
                                     workbook_path=path, **common)
        else:
            prepare_aoi_rs_decoration(pd.DataFrame(), pd.DataFrame(), specifications, path.parent,
                                     workbook_path=path, **common)

    def _verify_ledgers(self, scope: str) -> dict:
        from src.inline_domain.composition import build_sheet_alert_query
        from src.inline_domain.infrastructure.shared.resource_paths import scope_decoration_path
        from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import load_refresh_meta

        counts = {}
        for event in ("OOS", "OOC"):
            path = scope_decoration_path(scope, event)
            meta = load_refresh_meta(path.parent, path.name, scope, self.product)
            if not meta or meta.get("product_revision") != self.revision:
                raise ValueError("Generated alarm ledger is not from this production run")
            result = build_sheet_alert_query(scope, event, strict_empty=True, workbook_reader=self._readback).read(
                scope, self.product, self.reference)
            if result["state"] not in ("ok", "alert"):
                raise ValueError("Generated alarm ledger did not validate")
            counts[event.lower() + "_count"] = len(result["records"])
        return counts

    def _verify_capability(self, report: object) -> None:
        from src.inline_domain import composition as inline

        previous_monday = self.reference - timedelta(days=self.reference.weekday() + 7)
        year, week, _ = previous_monday.isocalendar()
        expected_label = f"{year}-W{week:02d}"
        for metric in ("cpk", "cpm"):
            saved = inline.build_capability_latest_reader(metric, strict_empty=True,
                                                         workbook_reader=self._readback).read_product(self.product)
            if saved is None:
                raise ValueError("Generated capability workbook missing")
            selected = saved.loc[saved["period_type"].eq("week") & saved["period_label"].eq(expected_label)]
            if report.period_capability_df.empty and not selected.empty:
                raise ValueError("Obsolete capability records remain in an empty source week")
            result = inline.build_capability_alert_query(metric, workbook_reader=self._readback).read(self.product, self.reference)
            if result["state"] not in ("ok", "alert"):
                raise ValueError("Generated capability result did not validate")
        self._finalize_membership_policy(report)
        members = inline.build_spc_sheet_membership_store().read(self.product, previous_monday)
        if (members is None or members.get("policy_signature") != inline.build_spc_sheet_membership_policy_signature()
                or members.get("coverage_start") != previous_monday.isoformat()
                or members.get("coverage_end_exclusive") != (previous_monday + timedelta(days=7)).isoformat()):
            raise ValueError("SPC complete-week membership did not validate")

    @staticmethod
    def _logical_membership_policy() -> str:
        """Compare actual preparation rules, independent of protected stat views."""
        from src.shared_kernel.config import ConfigLoader
        from src.inline_domain.infrastructure.shared.outlier_filter_rules import load_outlier_filter_rules
        from src.inline_domain.core.monitor.monitor_param_classifier import PARAM_CLASSIFICATION_SIGNATURE

        path = ConfigLoader.get_domain_resource_path("inline_domain", "spc_outlier_filters")
        rules = load_outlier_filter_rules(path, ConfigLoader.get_project_root() / "output/decrypted_files")
        policy = [PARAM_CLASSIFICATION_SIGNATURE, ConfigLoader.get_data_forward_policy().signature,
                  ConfigLoader.get_inline_data_exclusion(), str(path.resolve()),
                  rules.to_json(orient="split", index=False, date_format="iso")]
        return hashlib.sha256(json.dumps(policy, ensure_ascii=False).encode()).hexdigest()

    def _ensure_capability_headers(self) -> None:
        """Add missing generated headers only; retain every existing value/sheet."""
        from src.shared_kernel.config import ConfigLoader
        from src.inline_domain.core.spc.cpk_decoration import capability_decoration_columns
        from src.shared_kernel.utils.excel_tools import replace_workbook_sheets

        if not isinstance(self._spc_source_proof, dict):
            raise ValueError("Prepared source coverage unavailable")
        path = ConfigLoader.get_domain_resource_path("inline_domain", "spc_cpk_cpm_decoration")
        try:
            stat = path.stat()
        except FileNotFoundError:
            sheets = {}
        else:
            sheets = self._readback(str(path.resolve()), stat.st_mtime_ns, stat.st_size)
        updates = {}
        for metric in ("cpk", "cpm"):
            name = self.product if metric == "cpk" else f"{self.product}_cpm"
            frame = sheets.get(name)
            columns = capability_decoration_columns(metric)
            if frame is None:
                updates[name] = pd.DataFrame(columns=columns)
            elif frame.empty and set(columns) - set(frame):
                updates[name] = frame.reindex(columns=[*frame.columns, *(key for key in columns if key not in frame)])
        if updates:
            result = replace_workbook_sheets(path, updates)
            if not result.written:
                raise OSError("Empty capability publication could not be saved")
            self._readback.clear()

    def _finalize_membership_policy(self, report: object) -> None:
        """Republish current prepared points only for a proven physical-view shift."""
        from src.inline_domain import composition as inline
        from src.inline_domain.application.spc.spc_service import _verified_membership_window
        from src.inline_domain.application.spc.sheet_membership import publish_spc_sheet_memberships

        if self._spc_logical_policy is None:
            return  # Read-only diagnostic callers do not republish anything.
        if self._logical_membership_policy() != self._spc_logical_policy:
            raise ValueError("SPC logical preparation policy changed during production")
        final = inline.build_spc_sheet_membership_policy_signature()
        if final == self._spc_policy:
            return
        coverage = inline.build_spc_sheet_membership_coverage(self.product)
        start, end = _verified_membership_window(self._query("spc"), coverage,
            source_coverage=self._spc_source_proof, require_native=True)
        if not isinstance(self._spc_source_proof, dict):
            raise ValueError("Prepared source coverage unavailable")
        anchors = [report.period_capability_df]
        for metric in ("cpk", "cpm"):
            decoration = getattr(report, f"{metric}_decoration_result", None)
            if decoration is not None:
                anchors.append(decoration.decoration_df)
        publish_spc_sheet_memberships(report.raw_measurements_df,
            store=inline.build_spc_sheet_membership_store(), product=self.product,
            start_date=start, end_date=end, policy_signature=final,
            source_generation=self._spc_source_proof["source_generation"],
            reference_date=self.reference, capability_records=anchors)
        self._spc_policy = final
