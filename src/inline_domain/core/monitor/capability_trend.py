"""Pure CPK/CPM history repair and replacement of weekly contributions.

ISO 8601 defines week years, not ownership by calendar month. This module uses
the calendar month containing an ISO week's Sunday. Counts, rather than rates,
are added at every level; a period with no monitored projects has a rate of one.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, timedelta
import re

import pandas as pd

from src.inline_domain.core.monitor.cpk_summary import capability_summary_columns


HISTORY_KEYS = ["产品", "监控类型", "厂别", "周期类型", "时间标签"]
SCOPE_COLUMNS = HISTORY_KEYS[:3]
MonthKey = tuple[str, str, str, str]
PERIOD_ORDER = {"年度": 0, "季度": 1, "月度": 2, "周度": 3}


def _week_sunday(label: str) -> date:
    if not isinstance(label, str) or not re.fullmatch(r"\d{4}-W\d{2}", label):
        raise ValueError(f"Invalid ISO week label: {label}")
    year, week = label.split("-W")
    return date.fromisocalendar(int(year), int(week), 1) + timedelta(days=6)


def week_owner_month(label: str) -> str:
    """Return the calendar month of Sunday, including ISO/calendar year crossings."""
    return _week_sunday(label).strftime("%Y-%m")


def _display_label(period_type: str, label: str) -> str:
    if period_type == "周度":
        week_owner_month(label)
        return f"W{int(label[-2:])}"
    if period_type == "月度" and re.fullmatch(r"\d{4}-\d{2}", label):
        parsed = date.fromisoformat(label + "-01")
        return f"M{parsed.month}"
    if period_type == "季度" and re.fullmatch(r"\d{4}-Q[1-4]", label):
        return label[-2:]
    if period_type == "年度" and re.fullmatch(r"\d{4}", label):
        return f"Y{int(label) % 100:02d}"
    raise ValueError(f"Invalid capability period: {period_type}/{label}")


def parse_capability_history_column(values: pd.Series, name: str, *, rate: bool = False) -> pd.Series:
    """Parse maintained values without treating invalid nonblank cells as missing."""
    normalized = values.replace(r"^\s*$", pd.NA, regex=True)
    if rate:
        percent = normalized.astype("string").str.strip().str.endswith("%", na=False)
        normalized = normalized.where(~percent, normalized.astype("string").str.rstrip("%"))
    numeric = pd.to_numeric(normalized, errors="coerce")
    if (normalized.notna() & numeric.isna()).any():
        raise ValueError(f"Capability history contains a nonnumeric {name}")
    if rate:
        numeric = numeric.where(~percent, numeric / 100)
    present = numeric.dropna()
    if (present.isin([float("inf"), float("-inf")]) | present.lt(0)).any():
        raise ValueError(f"Capability history {name} must be finite and nonnegative")
    if (present.gt(1) if rate else present.mod(1).ne(0)).any():
        raise ValueError(f"Capability history {name} is outside its valid range")
    return numeric.astype("Float64" if rate else "Int64")


def normalize_capability_history(
    frame: pd.DataFrame, *, metric: str = "cpk", repair_missing: bool = False,
) -> pd.DataFrame:
    """Normalize one metric without mutating the caller's maintained history.

    Display labels are derived solely from period identity and need not be stored.
    Repair follows the requested order: total=0, qualified=total, warning=total-
    qualified, and rate=qualified/total. Existing contradictory counts fail rather
    than being silently clamped. Existing nonzero-period rates remain maintained
    values; all zero-project periods use one regardless of their previous rate.
    """
    columns = capability_summary_columns(metric)
    total, rate = columns[6:8]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    aliases = {
        f"{metric.upper()}总项目": total,
        f"{metric.upper()}总项数": total,
        f"{metric.upper()}≥1.33达标率": rate,
    }
    result = frame.rename(columns={old: new for old, new in aliases.items()
                                 if old in frame and new not in frame}).copy()
    if result.columns.duplicated().any():
        raise ValueError("Capability history contains duplicate columns")
    result = result.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all")
    if result.empty:
        return pd.DataFrame(columns=columns)
    for column, value in (("监控类型", "SPC"), ("厂别", "ALL")):
        if column not in result:
            result[column] = value
        else:
            result[column] = result[column].fillna(value)
    if missing := set(HISTORY_KEYS) - set(result.columns):
        raise ValueError(f"Capability history missing identity columns: {sorted(missing)}")
    for column in HISTORY_KEYS:
        result[column] = result[column].astype("string").str.strip()
    if result[HISTORY_KEYS].isna().any().any() or result[HISTORY_KEYS].eq("").any().any():
        raise ValueError("Capability history identity must not be blank")
    if result.duplicated(HISTORY_KEYS).any():
        raise ValueError("Capability history contains duplicate business keys")
    result["显示标签"] = [_display_label(kind, label)
                        for kind, label in result[["周期类型", "时间标签"]].itertuples(index=False, name=None)]
    for column in columns[6:]:
        if column not in result:
            if not repair_missing:
                raise ValueError(f"Capability history missing value column: {column}")
            result[column] = pd.NA
        result[column] = parse_capability_history_column(result[column], column, rate=column == rate)
    if repair_missing:
        result[total] = result[total].fillna(0)
        result["达标项目数"] = result["达标项目数"].fillna(result[total])
        result["预警项目数"] = result["预警项目数"].fillna(result[total] - result["达标项目数"])
        result[rate] = result[rate].fillna(result["达标项目数"] / result[total].replace(0, 1))
    if result[columns[6:]].isna().any().any():
        raise ValueError("Capability history has missing values")
    if (result["预警项目数"].lt(0)
            | result[total].ne(result["达标项目数"] + result["预警项目数"])).any():
        raise ValueError("Capability history total must equal qualified plus warning projects")
    result.loc[result[total].eq(0), rate] = 1.0
    return _sorted_history(result[columns])


def _sorted_history(frame: pd.DataFrame) -> pd.DataFrame:
    return (frame.assign(_period_order=frame["周期类型"].map(PERIOD_ORDER))
            .sort_values([*SCOPE_COLUMNS, "_period_order", "时间标签"], kind="stable")
            .drop(columns="_period_order").reset_index(drop=True))


def capability_history_for_storage(frame: pd.DataFrame, *, metric: str = "cpk") -> pd.DataFrame:
    """Remove the redundant derived label from the durable workbook contract."""
    return normalize_capability_history(frame, metric=metric).drop(columns="显示标签")


def _aggregate_rows(rows: pd.DataFrame, scope: tuple[str, str, str],
                    kind: str, label: str, metric: str) -> dict[str, object]:
    total_column, rate_column = capability_summary_columns(metric)[6:8]
    counts = {column: int(rows[column].sum())
              for column in (total_column, "达标项目数", "预警项目数")}
    return {**dict(zip(SCOPE_COLUMNS, scope)), "周期类型": kind, "时间标签": label,
            "显示标签": _display_label(kind, label), **counts,
            rate_column: counts["达标项目数"] / counts[total_column] if counts[total_column] else 1.0}


def _replace_rows(current: pd.DataFrame, updates: pd.DataFrame) -> pd.DataFrame:
    if updates.empty:
        return current.copy()
    update_keys = pd.MultiIndex.from_frame(updates[HISTORY_KEYS])
    keys = pd.MultiIndex.from_frame(current[HISTORY_KEYS])
    preserved = current.loc[~keys.isin(update_keys)]
    return updates.copy().reset_index(drop=True) if preserved.empty else pd.concat([preserved, updates], ignore_index=True)


def _rollup_scopes(history: pd.DataFrame, month_keys: set[MonthKey], metric: str) -> pd.DataFrame:
    if not month_keys:
        return history.copy()
    quarter_keys = {(product, scope, factory, f"{month[:4]}-Q{(int(month[-2:]) - 1) // 3 + 1}")
                    for product, scope, factory, month in month_keys}
    quarter_rows = []
    for product, scope, factory, quarter in sorted(quarter_keys):
        selected = history.loc[history["周期类型"].eq("月度")
                               & history["产品"].eq(product) & history["监控类型"].eq(scope)
                               & history["厂别"].eq(factory)].copy()
        labels = selected["时间标签"]
        in_quarter = labels.str[:4].eq(quarter[:4]) & ((labels.str[-2:].astype(int) - 1) // 3 + 1).eq(int(quarter[-1]))
        selected = selected.loc[in_quarter]
        quarter_rows.append(_aggregate_rows(selected, (product, scope, factory), "季度", quarter, metric))
    result = _replace_rows(history, pd.DataFrame(quarter_rows))
    year_rows = []
    for product, scope, factory, year in sorted({(*key[:3], key[3][:4]) for key in quarter_keys}):
        selected = result.loc[result["周期类型"].eq("季度") & result["时间标签"].str[:4].eq(year)
                              & result["产品"].eq(product) & result["监控类型"].eq(scope)
                              & result["厂别"].eq(factory)]
        year_rows.append(_aggregate_rows(selected, (product, scope, factory), "年度", year, metric))
    return normalize_capability_history(_replace_rows(result, pd.DataFrame(year_rows)), metric=metric)


def rollup_calendar_history(
    frame: pd.DataFrame, *, metric: str = "cpk", affected_months: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Replace quarters from monthly counts, then years from quarterly counts.

    When ``affected_months`` is omitted, all existing monthly scopes are rebuilt.
    Missing months contribute no counts, and periods without any owning months
    remain maintained history. Rates are recomputed from aggregate counts.
    """
    history = normalize_capability_history(frame, metric=metric)
    months = history.loc[history["周期类型"].eq("月度")]
    if affected_months is not None:
        months = months.loc[months["时间标签"].isin(set(affected_months))]
    month_keys = set(months[[*SCOPE_COLUMNS, "时间标签"]].itertuples(index=False, name=None))
    return _rollup_scopes(history, month_keys, metric)


def update_weekly_history(
    current: pd.DataFrame, weekly_updates: pd.DataFrame, *, metric: str = "cpk",
    complete_months: Iterable[MonthKey] = (), as_of: object | None = None,
) -> pd.DataFrame:
    """Replace weeks idempotently and update only their owning calendar periods.

    An explicitly complete month is recomputed from all stored weekly records.
    Otherwise its maintained monthly baseline is adjusted by the difference
    between old and replacement weeks. Every replaced week must then exist in
    the old history: without it, the old baseline's contribution is unknown.
    ``complete_months`` contains ``(product, monitoring_type, factory, YYYY-MM)``.
    This explicit coverage contract prevents partial repair from wiping history.
    When ``as_of`` is supplied, open/future weekly rows remain stored but never
    contribute to month, quarter or year aggregates. Completion starts on the
    Monday after the weekly Sunday, including when ``as_of`` itself is Sunday.
    """
    history = normalize_capability_history(current, metric=metric)
    updates = normalize_capability_history(weekly_updates, metric=metric)
    if updates.empty:
        return history
    if not updates["周期类型"].eq("周度").all():
        raise ValueError("Weekly replacement accepts only weekly records")
    complete = set(complete_months)
    total_column = capability_summary_columns(metric)[6]
    value_columns = [total_column, "达标项目数", "预警项目数"]
    updated = _replace_rows(history, updates)
    incoming = updates.assign(_month=updates["时间标签"].map(week_owner_month))
    old_weeks = history.loc[history["周期类型"].eq("周度")]
    all_weeks = updated.loc[updated["周期类型"].eq("周度")].copy()
    all_weeks["_month"] = all_weeks["时间标签"].map(week_owner_month)
    if as_of is not None:
        timestamp = pd.Timestamp(as_of)
        if pd.isna(timestamp):
            raise ValueError("Invalid capability history reference date")
        monday = timestamp.date() - timedelta(days=timestamp.weekday())
        all_weeks = all_weeks.loc[all_weeks["时间标签"].map(_week_sunday).lt(monday)]
        incoming = incoming.loc[incoming["时间标签"].map(_week_sunday).lt(monday)]
    month_rows, affected = [], set()
    for key, group in incoming.groupby([*SCOPE_COLUMNS, "_month"], sort=True):
        product, scope, factory, month = key
        affected.add(key)
        scope_mask = (updated["产品"].eq(product) & updated["监控类型"].eq(scope)
                      & updated["厂别"].eq(factory))
        if key in complete:
            selected = all_weeks.loc[all_weeks["产品"].eq(product) & all_weeks["监控类型"].eq(scope)
                                     & all_weeks["厂别"].eq(factory) & all_weeks["_month"].eq(month)]
            row = _aggregate_rows(selected, key[:3], "月度", month, metric)
        else:
            previous = old_weeks.loc[old_weeks["产品"].eq(product) & old_weeks["监控类型"].eq(scope)
                                     & old_weeks["厂别"].eq(factory) & old_weeks["时间标签"].isin(group["时间标签"])]
            if set(previous["时间标签"]) != set(group["时间标签"]):
                raise ValueError(f"Cannot replace unknown weekly contributions for {product}/{month}; complete month coverage required")
            baseline = updated.loc[scope_mask & updated["周期类型"].eq("月度") & updated["时间标签"].eq(month)]
            if baseline.empty:
                raise ValueError(f"Missing maintained monthly baseline for {product}/{month}; complete month coverage required")
            row = baseline.iloc[0].to_dict()
            if any(int(row[column]) < int(previous[column].sum()) for column in value_columns):
                raise ValueError(f"Weekly replacement exceeds maintained baseline for {product}/{month}")
            for column in value_columns:
                row[column] = int(row[column]) - int(previous[column].sum()) + int(group[column].sum())
            if min(row[column] for column in value_columns) < 0:
                raise ValueError(f"Weekly replacement exceeds maintained baseline for {product}/{month}")
            rate_column = capability_summary_columns(metric)[7]
            row[rate_column] = row["达标项目数"] / row[total_column] if row[total_column] else 1.0
        month_rows.append(row)
    with_months = _replace_rows(updated, pd.DataFrame(month_rows))
    return _rollup_scopes(with_months, affected, metric)
