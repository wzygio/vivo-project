"""Published Q-Time decisions consumed without loading measurements or a database."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
import hashlib
import json
import logging
from typing import Protocol

import pandas as pd

QTIME_ALERT_COLUMNS = [
    "shop", "prodcode", "f_step", "t_step", "step_desc", "lot_id", "q_spec",
    "wait_time", "over_hours", "timekey",
]
QTIME_SHOPS = ("ARRAY", "OLED", "TP")
QTIME_METADATA_COLUMNS = [
    "shop", "reference_date", "coverage_start", "coverage_end", "updated_at", "status",
    "source_status", "source_refreshed_at", "policy_signature", "alert_count", "product_count", "fingerprint",
]
UNAVAILABLE = "Q-Time 预警结果暂不可用，请更新对应报表后重试。"
logger = logging.getLogger(__name__)


class QTimeAlertStore(Protocol):
    def read(self) -> dict[str, pd.DataFrame] | None: ...
    def save(self, shop: str, alerts: pd.DataFrame, products: pd.DataFrame, metadata: dict) -> None: ...
    def source_signature(self) -> str: ...


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


class QTimeAlertResults:
    """Publish complete shop projections; readers never fill missing shops by querying."""

    def __init__(
        self, store: QTimeAlertStore, *, ttl_seconds: int,
        policy_signature: Callable[[], str] = lambda: "qtime-published-v1",
        clock: Callable[[], datetime] = datetime.now,
    ) -> None:
        self.store = store
        self.ttl_seconds = ttl_seconds
        self.policy_signature = policy_signature
        self.clock = clock

    def publish(
        self, shop: str, details: pd.DataFrame, alerts: pd.DataFrame,
        health: Mapping[str, str], reference_date: date,
    ) -> None:
        if shop not in QTIME_SHOPS:
            raise ValueError("Unknown Q-Time publication shop")
        if health.get("status") != "fresh":
            # Stale input must never overwrite a successful result as current.
            raise ValueError("Q-Time publication requires fresh source data")
        if not alerts.empty and set(QTIME_ALERT_COLUMNS[1:]) - set(alerts):
            raise ValueError("Incomplete Q-Time alert projection")
        saved = alerts.assign(shop=shop).reindex(columns=QTIME_ALERT_COLUMNS).copy()
        membership = (
            details[["prodcode", "step_desc", "lot_id", "timekey"]].drop_duplicates().copy()
            if not details.empty else pd.DataFrame(columns=["prodcode", "step_desc", "lot_id", "timekey"])
        )
        previous_month = (reference_date.replace(day=1) - timedelta(days=1)).replace(day=1)
        metadata = {
            "shop": shop, "reference_date": reference_date.isoformat(),
            "coverage_start": previous_month.isoformat(), "coverage_end": reference_date.isoformat(),
            "updated_at": self.clock().isoformat(), "status": "success",
            "source_status": health["status"], "source_refreshed_at": health.get("refreshed_at", ""),
            "policy_signature": self.policy_signature(), "alert_count": len(saved),
            "product_count": len(membership),
        }
        metadata["fingerprint"] = _digest({
            "metadata": {key: value for key, value in metadata.items() if key != "updated_at"},
            "alerts": saved.astype(object).where(saved.notna(), None).to_dict("records"),
            "products": membership.to_dict("records"),
        })
        self.store.save(shop, saved, membership, metadata)

    def read(
        self, product: str, reference_date: date, *, shops: Sequence[str] = QTIME_SHOPS,
        step_descriptions: Sequence[str] = (), start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        empty = pd.DataFrame(columns=QTIME_ALERT_COLUMNS)
        try:
            if not shops or set(shops) - set(QTIME_SHOPS):
                raise ValueError("Invalid Q-Time requested shops")
            sheets = self.store.read()
            if sheets is None or "__meta__" not in sheets:
                raise ValueError("Missing Q-Time publication")
            meta = sheets["__meta__"]
            if meta.empty and set(QTIME_METADATA_COLUMNS).issubset(meta):
                initialized = all(
                    f"alerts_{shop}" in sheets and f"products_{shop}" in sheets
                    and sheets[f"alerts_{shop}"].empty and sheets[f"products_{shop}"].empty
                    and set(QTIME_ALERT_COLUMNS).issubset(sheets[f"alerts_{shop}"])
                    and {"prodcode", "step_desc", "lot_id", "timekey"}.issubset(sheets[f"products_{shop}"])
                    for shop in QTIME_SHOPS
                )
                if initialized:
                    return {"state": "no_data", "records": empty, "alerts": empty.copy(),
                            "alert_factories": [], "message": "当前暂无预警记录。",
                            "metadata": {"status": "initialized", "total_lots": 0}}
            monday = reference_date - timedelta(days=reference_date.weekday())
            start = pd.Timestamp(start_date or monday - timedelta(days=7)).normalize()
            end = (pd.Timestamp(end_date).normalize() + pd.Timedelta(days=1)
                   if end_date else pd.Timestamp(monday))
            if start >= end:
                raise ValueError("Invalid Q-Time publication display window")
            frames = []
            lot_ids: set[str] = set()
            has_product = False
            now = pd.Timestamp(self.clock())
            for shop in dict.fromkeys(shops):
                metadata = meta.loc[meta["shop"].eq(shop)]
                if len(metadata) != 1:
                    raise ValueError("Missing or duplicate Q-Time shop metadata")
                row = metadata.iloc[0]
                saved, products = sheets[f"alerts_{shop}"], sheets[f"products_{shop}"]
                if (set(QTIME_ALERT_COLUMNS) - set(saved)
                        or {"prodcode", "step_desc", "lot_id", "timekey"} - set(products)
                        or len(saved) != int(row.alert_count) or len(products) != int(row.product_count)
                        or row.status != "success" or row.source_status != "fresh"
                        or str(row.reference_date) != str(reference_date)
                        or row.policy_signature != self.policy_signature()
                        or pd.Timestamp(row.coverage_start) > start
                        or pd.Timestamp(row.coverage_end) < end - pd.Timedelta(days=1)):
                    raise ValueError("Invalid Q-Time publication scope")
                for field in ("updated_at", "source_refreshed_at"):
                    stamp = pd.Timestamp(row[field])
                    if pd.isna(stamp):
                        raise ValueError("Missing Q-Time publication time")
                    if stamp.tzinfo is not None:
                        stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
                    age = (now - stamp).total_seconds()
                    if age < 0 or age > self.ttl_seconds:
                        raise ValueError("Expired Q-Time publication")
                if not saved.empty and not saved["shop"].eq(shop).all():
                    raise ValueError("Q-Time shop sheet contains another shop")
                product_rows = products.loc[products["prodcode"].astype(str).eq(str(product))]
                product_times = pd.to_datetime(product_rows["timekey"].astype(str).str.slice(0, 14),
                                               format="%Y%m%d%H%M%S", errors="coerce")
                if product_times.isna().any():
                    raise ValueError("Invalid Q-Time published source time")
                product_rows = product_rows.loc[(product_times >= start) & (product_times < end)]
                if step_descriptions:
                    product_rows = product_rows.loc[product_rows["step_desc"].isin(step_descriptions)]
                has_product |= not product_rows.empty
                lot_ids.update(product_rows["lot_id"].dropna().astype(str))
                selected = saved.loc[saved["prodcode"].astype(str).eq(str(product))].copy()
                times = pd.to_datetime(selected["timekey"].astype(str).str.slice(0, 14),
                                       format="%Y%m%d%H%M%S", errors="coerce")
                if times.isna().any():
                    raise ValueError("Invalid Q-Time published event time")
                if step_descriptions:
                    selected = selected.loc[selected["step_desc"].isin(step_descriptions)]
                    times = times.reindex(selected.index)
                frames.append(selected.loc[(times >= start) & (times < end)])
            records = pd.concat(frames, ignore_index=True).reindex(columns=QTIME_ALERT_COLUMNS)
            state = "alert" if not records.empty else "ok" if has_product else "no_data"
            return {
                "state": state, "records": records, "alerts": records.copy(),
                "alert_factories": sorted(records["shop"].unique().tolist()), "message": "",
                "metadata": {"total_lots": len(lot_ids), "period_start": str(start.date()),
                             "period_end": str((end-pd.Timedelta(days=1)).date())},
            }
        except (KeyError, TypeError, ValueError, OSError) as exc:
            logger.warning("Q-Time published alerts unavailable: product=%s cause=%s", product, type(exc).__name__)
            return {"state": "error", "records": empty, "alerts": empty.copy(),
                    "alert_factories": [], "message": UNAVAILABLE, "metadata": {}}

    def source_signature(self, product: str, reference_date: date, **selection) -> str:
        # Recheck validity outside the result cache so age expiry invalidates it.
        state = self.read(product, reference_date, **selection)["state"]
        return _digest((self.store.source_signature(), self.policy_signature(), str(reference_date), selection, state))
