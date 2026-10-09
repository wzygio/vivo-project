"""Replace this week's contribution in user-supplied totals, never rebuild history."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date

import pandas as pd

from src.inline_domain.core.monitor.period_summary import (
    MONITOR_SUMMARY_COLUMNS,
    current_period_windows,
)

WEEK_MARKER = "已计入周"
CONTRIBUTION_COLUMNS = {
    "OOS": "本周OOS已计入片数",
    "OOC": "本周OOC已计入片数",
    "Total": "本周Total已计入片数",
}
WEEK_TRACKING_COLUMNS = [WEEK_MARKER, *CONTRIBUTION_COLUMNS.values()]


def _zero_period(
    product: str, scope_key: str, factory_key: str,
    period_type: str, label: str, display: str, week_label: str,
) -> dict[str, object]:
    row: dict[str, object] = dict.fromkeys(MONITOR_SUMMARY_COLUMNS, 0)
    row.update({
        "产品": product, "监控类型": scope_key, "厂别": factory_key,
        "周期类型": period_type, "时间标签": label, "显示标签": display,
        WEEK_MARKER: week_label,
    })
    row.update(dict.fromkeys(CONTRIBUTION_COLUMNS.values(), 0))
    return row


def _missing_periods(
    selected: pd.DataFrame, *, product: str, scope_key: str,
    factory_key: str, as_of: pd.Timestamp,
) -> list[dict[str, object]]:
    """Fill history gaps and the displayed timeline; never count closed periods."""
    windows = current_period_windows(as_of)
    missing: list[dict[str, object]] = []
    for window in windows:
        known = set(selected.loc[selected["周期类型"].eq(window.period_type), "时间标签"])
        first = min(known | {window.time_label})
        if window.period_type == "年度":
            periods = [
                (str(year), f"Y{year % 100:02d}")
                for year in range(int(first), pd.Timestamp(as_of).year + 1)
            ]
        elif window.period_type == "季度":
            first = min(first, f"{pd.Timestamp(as_of).year}-Q1")
            quarters = pd.period_range(
                first.replace("-", ""), window.time_label.replace("-", ""), freq="Q",
            )
            periods = [(f"{p.year}-Q{p.quarter}", f"Q{p.quarter}") for p in quarters]
        elif window.period_type == "月度":
            first = min(first, str(pd.Period(as_of, freq="M") - 2))
            periods = [
                (str(p), f"M{p.month}")
                for p in pd.period_range(first, window.time_label, freq="M")
            ]
        else:
            year, week = first.split("-W")
            start = min(
                pd.Timestamp(date.fromisocalendar(int(year), int(week), 1)),
                window.start - pd.Timedelta(weeks=3),
            )
            periods = [
                (f"{day.isocalendar().year}-W{day.isocalendar().week:02d}",
                 f"W{day.isocalendar().week}")
                for day in pd.date_range(start, window.start, freq="7D")
            ]
        for label, display in periods:
            if label not in known:
                missing.append(_zero_period(
                    product, scope_key, factory_key, window.period_type,
                    label, display, windows[-1].time_label,
                ))
    return missing


def _count(frame: pd.DataFrame, kind: str, start: pd.Timestamp, end: pd.Timestamp) -> int:
    if frame.empty:
        return 0
    times = pd.to_datetime(frame["event_time"], errors="raise")
    selected = frame[times.ge(start) & times.lt(end)]
    if "chart_kind" in selected and "scope" in selected:
        selected = selected.loc[~selected["scope"].eq("aoi_rs") | selected["chart_kind"].eq("sheet")]
    if kind != "Total":
        selected = selected[selected["alarm_type"].eq(kind)]
    return len(selected.drop_duplicates(["factory", "prod_code", "item_id"]))


def plan_weekly_replacement(
    records: pd.DataFrame, alerts: pd.DataFrame, *, products: Iterable[str],
    scope_key: str, factory_key: str, as_of: pd.Timestamp,
    available_types: Mapping[str, set[str]],
    reset_current_week: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    """Plan current rows from an exact persisted baseline.

    Missing calendar rows start at zero. Cross-boundary weeks need the existing
    period's recorded week contribution;
    lacking that baseline is an explicit user-data gap, not an assumed zero.
    Each alarm kind updates atomically across the four open periods.
    """
    windows = current_period_windows(as_of)
    week = next(window for window in windows if window.period_type == "周度")
    rows: list[dict] = []
    problems: list[str] = []
    for product in dict.fromkeys(products):
        selected = records[
            records["产品"].eq(product) & records["监控类型"].eq(scope_key)
            & records["厂别"].eq(factory_key)
        ]
        initialized = _missing_periods(selected, product=product, scope_key=scope_key,
                                       factory_key=factory_key, as_of=as_of)
        open_labels = {window.time_label for window in windows}
        rows.extend(row for row in initialized if row["时间标签"] not in open_labels)
        if initialized:
            selected = pd.concat([selected.astype(object), pd.DataFrame(initialized)], ignore_index=True)
        existing = {}
        for window in windows:
            matched = selected[selected["时间标签"].eq(window.time_label)
                               & selected["周期类型"].eq(window.period_type)]
            if len(matched) != 1:
                raise ValueError(f"Duplicate current-period records: {product}/{window.time_label}")
            existing[window.time_label] = matched.iloc[0].to_dict()
        if reset_current_week:
            existing[week.time_label] = _zero_period(
                product, scope_key, factory_key, week.period_type,
                week.time_label, week.display_label, week.time_label,
            )
            # Exclusion changes only the weekly row, including its tracking fields.
            # Preserve maintained year/quarter/month totals and contributions exactly.
            rows.extend(existing[window.time_label] for window in windows)
            continue
        facts = alerts[alerts["prod_code"].eq(product)] if not alerts.empty else alerts
        available = set(available_types.get(product, set()))
        if {"OOS", "OOC"}.issubset(available):
            available.add("Total")
        for kind in ("OOS", "OOC", "Total"):
            if kind not in available:
                problems.append(f"{product}：{kind} Excel明细不完整，保留汇总值")
                continue
            count_column = f"{kind}报警片数"
            tracking = CONTRIBUTION_COLUMNS[kind]
            old_week = existing[week.time_label].get(count_column)
            changes = {}
            for window in windows:
                row = existing[window.time_label]
                old_total = row.get(count_column)
                if pd.isna(old_total):
                    problems.append(f"{product}/{window.display_label}：缺少{count_column}基线")
                    break
                new_contribution = _count(facts, kind, max(week.start, window.start), window.end)
                if window.period_type == "周度":
                    new_total = new_contribution
                else:
                    marker = row.get(WEEK_MARKER)
                    same_week = isinstance(marker, str) and marker == week.time_label
                    old_contribution = row.get(tracking) if same_week else None
                    if pd.isna(old_contribution):
                        if week.start >= window.start or (pd.notna(old_week) and old_week == 0):
                            old_contribution = old_week
                    if pd.isna(old_contribution):
                        problems.append(f"{product}/{window.display_label}：跨月/季/年周缺少{tracking}基线")
                        break
                    if int(old_contribution) < 0 or int(old_contribution) > int(old_total):
                        problems.append(f"{product}/{window.display_label}：已计入本周数量超出汇总基线，请核对Excel")
                        break
                    new_total = int(old_total) - int(old_contribution) + new_contribution
                    if new_total < 0:
                        problems.append(f"{product}/{window.display_label}：汇总小于已计入本周数量，请核对基线")
                        break
                changes[window.time_label] = (new_total, new_contribution)
            else:
                for label, (total, contribution) in changes.items():
                    row = existing[label]
                    row[count_column] = total
                    volume = row.get("过货量")
                    row[f"{kind}报警率"] = (
                        total / volume if pd.notna(volume) and volume > 0
                        else 0.0
                    )
                    # Do not retag another kind's old contribution as this week.
                    marker = row.get(WEEK_MARKER)
                    if not isinstance(marker, str) or marker != week.time_label:
                        for other in CONTRIBUTION_COLUMNS.values():
                            row[other] = pd.NA
                    row[WEEK_MARKER] = week.time_label
                    row[tracking] = contribution
        rows.extend(existing[window.time_label] for window in windows)
    return pd.DataFrame(rows), problems
