"""Publish density evaluations separately from their read-only alert display."""
from __future__ import annotations

from datetime import date, datetime
import hashlib
from typing import Protocol

import pandas as pd

from src.inline_domain.core.shared.density_trend_alerts import DENSITY_TREND_POLICY_VERSION, EVALUATION_VALUE_COLUMNS


class TrendResultStore(Protocol):
    def read(self, product: str) -> tuple[pd.DataFrame, dict] | None: ...
    def save(self, product: str, evaluation: pd.DataFrame, metadata: dict) -> None: ...
    def signature(self) -> str: ...


class TrendAlertResults:
    def __init__(self, store: TrendResultStore, *, scope: str, ttl_seconds: int,
                 policy: str, clock=None) -> None:
        if scope not in ("aoi_rs", "aoi_tt"):
            raise ValueError("Unsupported density trend scope")
        self.store, self.scope = store, scope
        self.ttl_seconds, self.policy = ttl_seconds, policy
        self.clock = clock or datetime.now

    def publish(self, product: str, reference_date: date, evaluation: pd.DataFrame) -> None:
        frame = evaluation.copy()
        required = ["prod_code", "factory", "step_id", "rs_code" if self.scope == "aoi_rs" else "tt_name", *EVALUATION_VALUE_COLUMNS]
        if frame.empty:
            frame = frame.reindex(columns=required)
        if set(required) - set(frame) or (not frame.empty and not frame.prod_code.astype(str).eq(product).all()):
            raise ValueError("Invalid trend evaluation publication")
        fingerprint = hashlib.sha256((str(reference_date) + self.policy + frame.to_json(date_format="iso")).encode()).hexdigest()
        previous = self.store.read(product)
        now = pd.Timestamp(self.clock())
        if previous is not None:
            meta = previous[1]
            generated = pd.to_datetime(meta.get("generated_at"), errors="coerce")
            if (meta.get("fingerprint") == fingerprint and pd.notna(generated)
                    and 0 <= (now - generated).total_seconds() < self.ttl_seconds):
                return
        self.store.save(product, frame, {"product": product, "reference_date": str(reference_date),
            "policy": self.policy, "generated_at": now.isoformat(), "row_count": len(frame),
            "fingerprint": fingerprint, "status": "success"})

    def read(self, product: str, reference_date: date) -> dict:
        payload = self.store.read(product)
        empty = {"state": "error", "records": pd.DataFrame(), "alerts": pd.DataFrame(),
                 "evaluation": pd.DataFrame(), "alert_factories": [],
                 "message": "趋势预警结果暂不可用，请更新对应报表后重试。", "metadata": {}}
        if payload is None:
            return empty
        frame, meta = payload
        generated = pd.to_datetime(meta.get("generated_at"), errors="coerce")
        age = (pd.Timestamp(self.clock()) - generated).total_seconds() if pd.notna(generated) else -1
        if (meta.get("status") != "success" or meta.get("product") != product
                or str(meta.get("reference_date")) != str(reference_date) or meta.get("policy") != self.policy
                or int(meta.get("row_count", -1)) != len(frame) or not 0 <= age <= self.ttl_seconds):
            return empty
        if not frame.empty:
            if not frame.prod_code.astype(str).eq(product).all() or "is_alert" not in frame:
                raise ValueError("Invalid published trend rows")
            for column in ("is_alert", "daily_alert", "daily_evaluable"):
                flags = frame[column].astype(str).str.lower()
                if not flags.isin(["true", "false", "1", "0"]).all():
                    raise ValueError("Invalid published trend flag")
                frame[column] = flags.isin(["true", "1"])
        rows = frame.loc[frame["is_alert"].eq(True)].copy() if "is_alert" in frame else frame.copy()
        return {"state": "no_data" if frame.empty else "alert" if len(rows) else "ok",
                "records": rows, "alerts": pd.DataFrame(), "evaluation": frame,
                "alert_factories": sorted(rows["factory"].dropna().astype(str).unique().tolist()) if len(rows) else [],
                "message": "", "metadata": meta}

    def source_signature(self, product: str, reference_date: date) -> str:
        result = self.read(product, reference_date)
        return f"{self.store.signature()}:{self.policy}:{reference_date}:{result['state']}"


def trend_policy_signature(date_exclusion: object) -> str:
    return f"{DENSITY_TREND_POLICY_VERSION}:{date_exclusion!r}"
