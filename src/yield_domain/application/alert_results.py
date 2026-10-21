"""Publish prepared Yield alerts and read them without analysis or database access."""
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
import json
from typing import Protocol

import pandas as pd

LOT_COLUMNS = ["超规 Lot ID", "异常 Code", "管控规格线", "实际不良率", "不良panel数", "入库时间", "阵列投入时间"]
TREND_COLUMNS = ["level", "defect_group", "defect_desc", "time_period", "curr_rate", "prev_rate", "rules", "period_scope"]
RESULT_COLUMNS = {"lot": LOT_COLUMNS, "trend": TREND_COLUMNS}
RESULT_SCHEMA_VERSION = 1


class YieldAlertStore(Protocol):
    def read_product(self, product: str, kind: str) -> dict | None: ...
    def write_product(self, product: str, kind: str, records: pd.DataFrame | None,
                      metadata: dict) -> bool: ...
    def source_signature(self, kind: str) -> str: ...


@dataclass(frozen=True)
class PublishedYieldAlerts:
    status: str
    records: pd.DataFrame
    metadata: dict


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, default=str,
                                     allow_nan=False).encode()).hexdigest()


def _utc_timestamp(value: object) -> pd.Timestamp:
    """Legacy source health uses naive server-local (Asia/Shanghai) timestamps."""
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return pd.NaT
    if timestamp.tz is None:
        timestamp = timestamp.tz_localize("Asia/Shanghai")
    return timestamp.tz_convert("UTC")


class YieldAlertResults:
    def __init__(self, store: YieldAlertStore, *, policy_signature: Callable[[str], str],
                 ttl_seconds: int, clock: Callable[[], datetime] | None = None) -> None:
        self.store = store
        self.policy_signature = policy_signature
        self.ttl_seconds = ttl_seconds
        self.clock = clock or (lambda: datetime.now().astimezone())

    def publish(self, product: str, kind: str, records: Sequence[Mapping], *,
                reference_date: date, coverage_start: str, coverage_end: str,
                has_data: bool, health: Mapping, calculation_signature: str) -> bool:
        required_columns = RESULT_COLUMNS[kind]
        columns = required_columns + (["is_current"] if kind == "trend" else [])
        rows = []
        for record in records:
            if missing := set(required_columns) - record.keys():
                raise ValueError(f"Yield result columns missing: {sorted(missing)}")
            row = {key: record[key] for key in required_columns}
            if kind == "trend":
                if record.get("is_current", True) not in (True, False):
                    raise ValueError("Invalid current trend marker")
                row["is_current"] = bool(record.get("is_current", True))
            if isinstance(row.get("rules"), (list, tuple)):
                row["rules"] = json.dumps(row["rules"], ensure_ascii=False)
            rows.append(row)
        frame = pd.DataFrame(rows, columns=columns)
        rows = frame.astype(object).where(frame.notna(), None).to_dict("records")
        frame = pd.DataFrame(rows, columns=columns)
        if rows and not has_data:
            raise ValueError("Alert rows require a nonempty source")
        if pd.Timestamp(coverage_start) > pd.Timestamp(coverage_end):
            raise ValueError("Invalid Yield result coverage")
        metadata = {
            "schema_version": RESULT_SCHEMA_VERSION, "product": product, "kind": kind,
            "reference_date": reference_date.isoformat(), "coverage_start": coverage_start,
            "coverage_end": coverage_end, "has_data": bool(has_data), "row_count": len(frame),
            "source_status": str(health.get("status", "unknown")),
            "source_refreshed_at": str(health.get("refreshed_at", "")),
            "source_start": str(health.get("source_start", "")),
            "source_end": str(health.get("source_end", "")),
            "policy_signature": self.policy_signature(product),
            "calculation_signature": calculation_signature, "status": "ready",
        }
        if metadata["source_status"] != "fresh":
            return self.mark_failed(product, kind, reference_date, "YIELD_SOURCE_NOT_CURRENT")
        if pd.isna(_utc_timestamp(metadata["source_refreshed_at"])):
            raise ValueError("Yield source refresh time is required")
        metadata["fingerprint"] = _digest({"metadata": metadata, "records": rows})
        metadata["generated_at"] = self.clock().isoformat()
        return self.store.write_product(product, kind, frame, metadata)

    def mark_failed(self, product: str, kind: str, reference_date: date, error_code: str) -> bool:
        metadata = {"status": "failed", "attempt_date": reference_date.isoformat(),
                    "error_code": error_code}
        metadata["fingerprint"] = _digest(metadata)
        metadata["attempted_at"] = self.clock().isoformat()
        return self.store.write_product(product, kind, None, metadata)

    def read(self, product: str, kind: str, reference_date: date) -> PublishedYieldAlerts:
        payload = self.store.read_product(product, kind)
        if payload is None:
            return PublishedYieldAlerts("missing", pd.DataFrame(columns=RESULT_COLUMNS[kind]), {})
        frame, meta = payload["records"], payload["metadata"]
        if meta.get("status") == "failed":
            return PublishedYieldAlerts("failed", frame, meta)
        required = {"schema_version", "product", "kind", "reference_date", "policy_signature",
                    "coverage_start", "coverage_end", "row_count", "has_data", "generated_at",
                    "source_status", "source_refreshed_at", "fingerprint"}
        if required - meta.keys() or meta.get("status") != "ready":
            raise ValueError("Incomplete Yield publication metadata")
        if (meta["schema_version"] != RESULT_SCHEMA_VERSION or meta["product"] != product
                or meta["kind"] != kind or set(RESULT_COLUMNS[kind]) - set(frame.columns)
                or len(frame) != meta["row_count"] or meta["has_data"] not in (True, False)):
            raise ValueError("Invalid Yield publication payload")
        stale = (meta["reference_date"] != reference_date.isoformat()
                 or meta["policy_signature"] != self.policy_signature(product)
                 or meta["source_status"] != "fresh")
        now = pd.Timestamp(self.clock())
        if now.tz is None:
            now = now.tz_localize("Asia/Shanghai")
        for key in ("generated_at", "source_refreshed_at"):
            timestamp = _utc_timestamp(meta[key])
            age = (now.tz_convert("UTC") - timestamp).total_seconds() if pd.notna(timestamp) else -1
            stale |= age < 0 or age > self.ttl_seconds
        if stale:
            return PublishedYieldAlerts("stale", frame, meta)
        monday = reference_date - timedelta(days=reference_date.weekday())
        required_start = monday - timedelta(days=7) if kind == "lot" else reference_date
        required_end = monday - timedelta(days=1) if kind == "lot" else reference_date
        start, end = pd.Timestamp(meta["coverage_start"]), pd.Timestamp(meta["coverage_end"])
        if start > pd.Timestamp(required_start) or end < pd.Timestamp(required_end):
            return PublishedYieldAlerts("uncovered", frame, meta)
        if kind == "lot" and not frame.empty:
            times = pd.to_datetime(frame["入库时间"], format="%Y/%m/%d", errors="coerce")
            if times.isna().any():
                raise ValueError("Invalid published Lot event date")
            frame = frame.loc[(times >= pd.Timestamp(monday-timedelta(days=7)))
                              & (times < pd.Timestamp(monday))].reset_index(drop=True)
        if kind == "trend" and "is_current" in frame:
            if not frame["is_current"].isin([True, False]).all():
                raise ValueError("Invalid published current trend marker")
            frame = frame.loc[frame["is_current"].eq(True)].reset_index(drop=True)
        return PublishedYieldAlerts("ready" if bool(meta["has_data"]) else "no_data", frame, meta)

    def source_signature(self, product: str, kind: str, reference_date: date) -> str:
        result = self.read(product, kind, reference_date)
        return _digest({"file": self.store.source_signature(kind), "status": result.status,
                        "policy": self.policy_signature(product), "as_of": str(reference_date)})
