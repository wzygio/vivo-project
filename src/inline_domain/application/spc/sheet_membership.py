"""Publish prepared BySheet membership; HTTP consumers only read the saved port."""
from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
import hashlib
import json
import math
from typing import Protocol
from zoneinfo import ZoneInfo

import pandas as pd

MEMBERSHIP_POLICY = "spc-sheet-membership-v1"
POINT_COLUMNS = ("prod_code", "factory", "step_id", "param_name", "sheet_id", "sheet_start_time", "param_value")
GROUP_COLUMNS = ("factory", "step_id", "param_name")


class SpcSheetMembershipUnavailable(ValueError):
    """Saved membership cannot establish the requested complete capability scope."""


class SpcSheetMembershipStore(Protocol):
    def read(self, product: str, start_date: date) -> dict | None: ...
    def publish(self, snapshot: dict) -> None: ...


def _day(value: object) -> date:
    return pd.Timestamp(value).date()


def _stamp(value: object) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("Missing membership source time")
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return stamp


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                      separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _whole_weeks(start_date: object, end_date: object, reference_date: object) -> list[date]:
    start, end, reference = _day(start_date), _day(end_date), _day(reference_date)
    if start > end:
        raise ValueError("Invalid membership publication window")
    monday = reference - timedelta(days=reference.weekday())
    end = min(end, monday - timedelta(days=1))
    first_ordinal = start.toordinal() + (7 - start.weekday()) % 7
    return [date.fromordinal(day) for day in range(first_ordinal, end.toordinal() - 5, 7)]


def _prepared_points(frame: pd.DataFrame, product: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=list(POINT_COLUMNS))
    if missing := set(POINT_COLUMNS) - set(frame.columns):
        raise ValueError(f"Prepared membership columns unavailable: {sorted(missing)}")
    points = frame[list(POINT_COLUMNS)].copy()
    if not points.empty and not points["prod_code"].astype(str).str.strip().eq(product).all():
        raise ValueError("Prepared membership contains another product")
    for column in ("prod_code", *GROUP_COLUMNS, "sheet_id"):
        points[column] = points[column].astype("string").str.strip()
    points["factory"] = points["factory"].str.upper()
    points["param_value"] = pd.to_numeric(points["param_value"], errors="coerce")
    points["sheet_start_time"] = points["sheet_start_time"].map(
        lambda value: _stamp(value) if pd.notna(value) else pd.NaT)
    valid = points["param_value"].map(lambda value: pd.notna(value) and math.isfinite(float(value)))
    for column in ("prod_code", *GROUP_COLUMNS, "sheet_id"):
        valid &= points[column].notna() & points[column].ne("")
    return points.loc[valid & points["sheet_start_time"].notna()].reset_index(drop=True)


def _capability_anchors(records: pd.DataFrame | Sequence[pd.DataFrame] | None, *, product: str,
                        label: str, key: tuple[str, str, str]) -> dict[str, list[dict]]:
    anchors: dict[str, list[dict]] = {}
    frames = [records] if isinstance(records, pd.DataFrame) else records or ()
    for frame in frames:
        required = {"prod_code", *GROUP_COLUMNS, "period_type", "period_label", "period_start", "period_end"}
        if not isinstance(frame, pd.DataFrame) or not required.issubset(frame):
            continue
        selected = frame.loc[frame["prod_code"].astype(str).str.strip().eq(product)
                             & frame["period_type"].eq("week") & frame["period_label"].eq(label)]
        for column, value in zip(GROUP_COLUMNS, key):
            column_values = selected[column].astype(str).str.strip()
            if column == "factory":
                column_values = column_values.str.upper()
            selected = selected.loc[column_values.eq(value)]
        for _, row in selected.iterrows():
            for metric in ("cpk", "cpm"):
                column = f"{metric}_corrected" if f"{metric}_corrected" in row else metric
                raw = row.get(column)
                if raw is None or pd.isna(raw):
                    continue
                value = float(raw)
                if not math.isfinite(value):
                    continue
                anchor = {"value": value, "period_start": _stamp(row["period_start"]).isoformat(),
                          "period_end": _stamp(row["period_end"]).isoformat()}
                bucket = anchors.setdefault(metric, [])
                if anchor not in bucket:
                    bucket.append(anchor)
    return anchors


def publish_spc_sheet_memberships(
    raw_measurements_df: pd.DataFrame, *, store: SpcSheetMembershipStore, product: str,
    start_date: str | date, end_date: str | date, policy_signature: str = MEMBERSHIP_POLICY,
    source_generation: str = "", reference_date: date | None = None,
    capability_records: pd.DataFrame | Sequence[pd.DataFrame] | None = None,
) -> tuple[str, ...]:
    """Explicit producer entry: save only complete covered weeks, retaining older shards.

    Values affect the internal generation but never replace the prepared Sheet IDs.
    The supplied query window is coverage; observed min/max are only data spans.
    """
    if not isinstance(policy_signature, str) or not policy_signature:
        raise ValueError("Membership policy signature unavailable")
    weeks = _whole_weeks(start_date, end_date, reference_date or date.today())
    if not weeks:
        return ()
    points = _prepared_points(raw_measurements_df, product)
    published = []
    for start in weeks:
        end_exclusive = start + timedelta(days=7)
        subset = points.loc[points["sheet_start_time"].ge(pd.Timestamp(start))
                            & points["sheet_start_time"].lt(pd.Timestamp(end_exclusive))]
        iso = start.isocalendar()
        label = f"{iso.year}-W{iso.week:02d}"
        groups = []
        for key, group in subset.groupby(list(GROUP_COLUMNS), sort=True, dropna=False):
            groups.append({"factory": key[0], "step_id": key[1], "parameter_name": key[2],
                           "sheet_ids": sorted(set(group["sheet_id"].astype(str))),
                           "data_start": group["sheet_start_time"].min().isoformat(),
                           "data_end": group["sheet_start_time"].max().isoformat(),
                           "anchors": _capability_anchors(capability_records, product=product,
                                                          label=label, key=key)})
        canonical_points = subset.copy()
        canonical_points["sheet_start_time"] = canonical_points["sheet_start_time"].map(lambda stamp: stamp.isoformat())
        canonical_points = canonical_points.drop_duplicates().sort_values(list(POINT_COLUMNS), kind="stable")
        # A new raw snapshot for another week does not invalidate historical generations.
        generation = _digest({"policy": policy_signature, "product": product, "period": label,
                              "points": canonical_points.to_dict("records"), "groups": groups})
        snapshot = {"schema_version": 1, "product": product, "period_label": label,
                    "coverage_start": start.isoformat(), "coverage_end_exclusive": end_exclusive.isoformat(),
                    "policy_signature": policy_signature, "source_generation": generation,
                    "upstream_generation": _digest(source_generation) if source_generation else None,
                    "published_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(), "groups": groups}
        store.publish(snapshot)
        published.append(label)
    return tuple(published)


class SpcSheetMembershipQuery:
    """Read-only capability membership; never reconstruct absent data from a producer."""

    def __init__(self, store: SpcSheetMembershipStore, *,
                 policy_signature: str | Callable[[], str] = MEMBERSHIP_POLICY) -> None:
        self._store = store
        self._policy_signature = policy_signature

    def sheet_ids(self, *, product: str, factory: str, step_id: str, parameter_name: str,
                  start_date: date | str, end_date: date | str,
                  expected_period_start: object = None, expected_period_end: object = None,
                  metric: str | None = None, expected_value: object = None,
                  expected_source_generation: str | None = None) -> list[str]:
        try:
            return self._read(product, factory, step_id, parameter_name, start_date, end_date,
                              expected_period_start, expected_period_end, metric, expected_value,
                              expected_source_generation)
        except (OSError, KeyError, TypeError, ValueError, OverflowError):
            raise SpcSheetMembershipUnavailable("Published SPC Sheet membership unavailable") from None

    def _read(self, product, factory, step_id, parameter_name, start_date, end_date,
              expected_start, expected_end, metric, expected_value, expected_generation):
        start, end = _day(start_date), _day(end_date)
        if start.weekday() != 0 or end != start + timedelta(days=6):
            raise ValueError("Membership requires one complete ISO week")
        snapshot = self._store.read(product, start)
        policy = self._policy_signature() if callable(self._policy_signature) else self._policy_signature
        if (not isinstance(snapshot, dict) or snapshot.get("schema_version") != 1
                or snapshot.get("product") != product or snapshot.get("policy_signature") != policy
                or not policy or snapshot.get("coverage_start") != start.isoformat()
                or snapshot.get("coverage_end_exclusive") != (end + timedelta(days=1)).isoformat()
                or not isinstance(snapshot.get("source_generation"), str)
                or len(snapshot["source_generation"]) != 64):
            raise ValueError("Membership metadata unavailable")
        iso = start.isocalendar()
        if snapshot.get("period_label") != f"{iso.year}-W{iso.week:02d}":
            raise ValueError("Membership period unavailable")
        if expected_generation is not None and snapshot["source_generation"] != expected_generation:
            raise ValueError("Membership generation changed")
        groups = snapshot.get("groups")
        if not isinstance(groups, list):
            raise ValueError("Membership groups unavailable")
        matching = [item for item in groups if isinstance(item, dict)
                    and (item.get("factory"), item.get("step_id"), item.get("parameter_name"))
                    == (str(factory).strip().upper(), str(step_id).strip(), str(parameter_name).strip())]
        if len(matching) != 1:
            raise ValueError("Membership scope unavailable")
        group = matching[0]
        data_start, data_end = _stamp(group["data_start"]), _stamp(group["data_end"])
        if (data_start > data_end or data_start < pd.Timestamp(start)
                or data_end >= pd.Timestamp(end + timedelta(days=1))):
            raise ValueError("Membership data span is outside the requested week")
        ids = group.get("sheet_ids")
        if (not isinstance(ids, list) or not ids or any(not isinstance(value, str) or not value for value in ids)
                or ids != sorted(set(ids))):
            raise ValueError("Membership identities unavailable")
        if (expected_start is None) != (expected_end is None):
            raise ValueError("Incomplete capability span")
        span = None
        if expected_start is not None:
            first, last = _stamp(expected_start), _stamp(expected_end)
            if (first > last or first < pd.Timestamp(start) or last >= pd.Timestamp(end + timedelta(days=1))
                    or data_start > first or data_end < last):
                raise ValueError("Membership does not cover capability source span")
            span = (first, last)
        if metric is not None:
            if metric not in ("cpk", "cpm") or expected_value is None or not math.isfinite(float(expected_value)):
                raise ValueError("Capability generation anchor unavailable")
            if not isinstance(group.get("anchors"), dict):
                raise ValueError("Capability generation anchors unavailable")
            anchors = group["anchors"].get(metric, [])
            matched = any(math.isclose(float(anchor["value"]), float(expected_value), rel_tol=1e-12, abs_tol=1e-12)
                          and (span is None or (_stamp(anchor["period_start"]), _stamp(anchor["period_end"])) == span)
                          for anchor in anchors)
            if not matched:
                raise ValueError("Capability and membership generations differ")
        return list(ids)
