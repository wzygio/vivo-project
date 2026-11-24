"""Previous ISO week's conclusions from the published Yield workbooks."""
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
from zoneinfo import ZoneInfo

import pandas as pd

from src.yield_domain.application.alert_results import (
    RESULT_COLUMNS,
    YieldAlertResults,
    is_lot_released,
    merge_release_controls,
)

NO_ANOMALIES = "无异常波动，无新增异常，无新增超规lot"
_ALERT_KINDS = ("trend", "lot")


@dataclass(frozen=True)
class ReportWeek:
    start: date

    @classmethod
    def previous(cls, today: date | None = None) -> "ReportWeek":
        if today is None:
            today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        return cls(today - timedelta(days=today.weekday() + 7))

    @property
    def end(self) -> date:
        return self.start + timedelta(days=6)

    @property
    def label(self) -> str:
        iso = self.start.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"


def _previous_week_rows(frame: pd.DataFrame, kind: str, week: ReportWeek) -> pd.DataFrame:
    if kind == "trend":
        # A newer week can supersede the board's is_current marker.
        return frame.loc[frame["period_scope"].eq("weekly") & frame["time_period"].eq(week.label)]
    dates = pd.to_datetime(frame["入库时间"], format="mixed", errors="raise").dt.normalize()
    if dates.isna().any():
        raise ValueError("Invalid published Yield Lot date")
    return frame.loc[dates.between(pd.Timestamp(week.start), pd.Timestamp(week.end))]


def _read_week(
    results: YieldAlertResults, product: str, kind: str, week: ReportWeek,
) -> pd.DataFrame | None:
    empty = pd.DataFrame(columns=RESULT_COLUMNS[kind])
    payload = results.store.read_product(product, kind)
    if payload is None and results.store.is_product_absent(product, kind):
        return None
    if payload is None:
        raise ValueError(f"Incomplete Yield publication for {product}/{kind}")
    frame, meta = payload["records"], payload["metadata"]
    if results.publication_status(product, kind, frame, meta) != "ready":
        raise ValueError(f"Unavailable Yield publication for {product}/{kind}")
    start, end = (pd.Timestamp(meta[key]) for key in ("coverage_start", "coverage_end"))
    if not (start <= pd.Timestamp(week.start) and end >= pd.Timestamp(week.end)):
        raise ValueError(f"Insufficient Yield coverage for {product}/{kind}/{week.label}")
    rows = _previous_week_rows(frame, kind, week)
    if rows.empty:
        return empty
    if payload["decisions"] is not None:
        rows, _ = merge_release_controls(kind, rows, payload["decisions"], add_missing=False)
    if "flag" not in rows:
        return empty
    return rows.loc[rows["flag"].map(is_lot_released)]


def _text(value: object) -> str:
    if pd.isna(value) or not (text := str(value).strip()):
        raise ValueError("Missing published Yield conclusion text")
    return text


def _rules_text(value: object) -> str:
    text = _text(value)
    if not text.startswith("["):
        return text
    rules = json.loads(text)
    if not isinstance(rules, list) or not rules or not all(isinstance(rule, str) for rule in rules):
        raise ValueError("Invalid published Yield trend rules")
    return "、".join(_text(rule) for rule in rules)


def _format_conclusion(trend: pd.DataFrame | None, lot: pd.DataFrame | None) -> str:
    if trend is None:
        trends = ["趋势预警记录暂无"]
    else:
        trends = [
            f"{_text(group if level == 'group' else code)}-{_rules_text(rules)}"
            for level, group, code, rules in
            trend[["level", "defect_group", "defect_desc", "rules"]].itertuples(index=False, name=None)
        ]
    if lot is None:
        lots = ["Lot 超规记录暂无"]
    else:
        lots = [
            f"{_text(lot_id)}-{_text(code)}超规"
            for lot_id, code in lot[["超规 Lot ID", "异常 Code"]].itertuples(index=False, name=None)
        ]
    return "；".join(dict.fromkeys((*trends, *lots))) or NO_ANOMALIES


def load_weekly_conclusions(
    results: YieldAlertResults, products: tuple[str, ...], week: ReportWeek,
) -> dict[str, str]:
    """Read a stable version of both workbooks and expose only customer-facing text."""
    versions = tuple(results.store.source_signature(kind) for kind in _ALERT_KINDS)
    result = {
        product: _format_conclusion(
            _read_week(results, product, "trend", week), _read_week(results, product, "lot", week),
        )
        for product in products
    }
    if versions != tuple(results.store.source_signature(kind) for kind in _ALERT_KINDS):
        raise ValueError("Yield publications changed during weekly report generation")
    return result
