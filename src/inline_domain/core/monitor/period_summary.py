"""Calendar-period summary contracts used by the automatic warning table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


SUMMARY_ROW_ORDER = [
    "过货量",
    "OOC报警片数",
    "SOOS报警片数",
    "OOS报警片数",
    "Total报警片数",
]
MONITOR_SUMMARY_COLUMNS = [
    "产品",
    "监控类型",
    "厂别",
    "周期类型",
    "时间标签",
    "显示标签",
    "过货量",
    "OOC报警片数",
    "OOC报警率",
    "SOOS报警片数",
    "SOOS报警率",
    "OOS报警片数",
    "OOS报警率",
    "Total报警片数",
    "Total报警率",
]


@dataclass(frozen=True)
class PeriodWindow:
    period_type: str
    time_label: str
    display_label: str
    start: pd.Timestamp
    end: pd.Timestamp


def _period_windows(end_date: pd.Timestamp) -> list[PeriodWindow]:
    end = pd.Timestamp(end_date).normalize()
    exclusive_end = end + pd.Timedelta(days=1)
    windows = [
        PeriodWindow(
            "年度",
            str(end.year),
            f"Y{end.year % 100:02d}",
            pd.Timestamp(end.year, 1, 1),
            exclusive_end,
        )
    ]
    current_quarter = (end.month - 1) // 3 + 1
    for quarter in range(1, current_quarter + 1):
        start = pd.Timestamp(end.year, (quarter - 1) * 3 + 1, 1)
        windows.append(
            PeriodWindow(
                "季度",
                f"{end.year}-Q{quarter}",
                f"Q{quarter}",
                start,
                min(start + pd.DateOffset(months=3), exclusive_end),
            )
        )
    month_start = end.replace(day=1)
    for offset in (2, 1, 0):
        start = month_start - pd.DateOffset(months=offset)
        windows.append(
            PeriodWindow(
                "月度",
                f"{start.year}-{start.month:02d}",
                f"M{start.month}",
                start,
                min(start + pd.DateOffset(months=1), exclusive_end),
            )
        )
    iso = end.isocalendar()
    week_start = end - pd.Timedelta(days=end.weekday())
    windows.append(
        PeriodWindow(
            "周度",
            f"{iso.year}-W{iso.week:02d}",
            f"W{iso.week}",
            week_start,
            exclusive_end,
        )
    )
    return windows


def current_period_windows(end_date: pd.Timestamp) -> tuple[PeriodWindow, ...]:
    """Return only the four open periods affected by ``end_date``."""
    windows = _period_windows(end_date)
    return (
        windows[0],
        next(window for window in reversed(windows) if window.period_type == "季度"),
        next(window for window in reversed(windows) if window.period_type == "月度"),
        windows[-1],
    )


def _normalize_facts(
    alerts_df: pd.DataFrame, throughput_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    alerts = alerts_df.copy() if isinstance(alerts_df, pd.DataFrame) else pd.DataFrame()
    throughput = (
        throughput_df.copy() if isinstance(throughput_df, pd.DataFrame) else pd.DataFrame()
    )
    if not alerts.empty:
        alerts["event_time"] = pd.to_datetime(alerts["event_time"], errors="coerce")
    if not throughput.empty:
        throughput["event_date"] = pd.to_datetime(
            throughput["event_date"], errors="coerce"
        )
    return alerts, throughput


def _window_counts(
    alerts: pd.DataFrame,
    throughput: pd.DataFrame,
    window: PeriodWindow,
) -> tuple[int, int, int, int, int]:
    period_alerts = (
        alerts[
            (alerts["event_time"] >= window.start)
            & (alerts["event_time"] < window.end)
        ]
        if not alerts.empty
        else alerts
    )
    period_throughput = (
        throughput[
            (throughput["event_date"] >= window.start)
            & (throughput["event_date"] < window.end)
        ].drop_duplicates(["factory", "prod_code", "item_id"])
        if not throughput.empty
        else throughput
    )

    def alarm_sheet_count(alarm_type: str) -> int:
        if period_alerts.empty:
            return 0
        return len(
            period_alerts[period_alerts["alarm_type"].eq(alarm_type)].drop_duplicates(
                ["factory", "prod_code", "item_id"]
            )
        )

    ooc = alarm_sheet_count("OOC")
    soos = 0
    oos = alarm_sheet_count("OOS")
    total = (
        len(period_alerts.drop_duplicates(["factory", "prod_code", "item_id"]))
        if not period_alerts.empty
        else 0
    )
    return len(period_throughput), ooc, soos, oos, total


def build_period_summary(
    alerts_df: pd.DataFrame,
    throughput_df: pd.DataFrame,
    *,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """Build an in-memory summary; retained as a test/fallback seam."""
    alerts, throughput = _normalize_facts(alerts_df, throughput_df)
    result: dict[str, list[int | str]] = {"报警类型": SUMMARY_ROW_ORDER.copy()}
    for window in _period_windows(pd.Timestamp(end_date)):
        result[window.display_label] = list(_window_counts(alerts, throughput, window))
    return pd.DataFrame(result)


def build_current_period_records(
    alerts_df: pd.DataFrame,
    throughput_df: pd.DataFrame,
    *,
    products: Iterable[str],
    scope_key: str,
    factory_key: str,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """Build one row per product for the four currently open periods."""
    alerts, throughput = _normalize_facts(alerts_df, throughput_df)
    records: list[dict[str, object]] = []
    for product in dict.fromkeys(str(value) for value in products):
        product_alerts = (
            alerts[alerts["prod_code"].astype(str).eq(product)]
            if not alerts.empty
            else alerts
        )
        product_throughput = (
            throughput[throughput["prod_code"].astype(str).eq(product)]
            if not throughput.empty
            else throughput
        )
        for window in current_period_windows(end_date):
            volume, ooc, soos, oos, total = _window_counts(
                product_alerts, product_throughput, window
            )
            rates = {
                "OOC报警率": ooc / volume if volume else 0.0,
                "SOOS报警率": soos / volume if volume else 0.0,
                "OOS报警率": oos / volume if volume else 0.0,
                "Total报警率": total / volume if volume else 0.0,
            }
            records.append(
                {
                    "产品": product,
                    "监控类型": scope_key,
                    "厂别": factory_key,
                    "周期类型": window.period_type,
                    "时间标签": window.time_label,
                    "显示标签": window.display_label,
                    "过货量": volume,
                    "OOC报警片数": ooc,
                    "SOOS报警片数": soos,
                    "OOS报警片数": oos,
                    "Total报警片数": total,
                    **rates,
                }
            )
    return pd.DataFrame(records, columns=MONITOR_SUMMARY_COLUMNS)


def build_period_summary_from_records(
    records_df: pd.DataFrame,
    *,
    end_date: pd.Timestamp,
    expected_products: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Pivot persisted long-form records into the dashboard table."""
    records = (
        records_df.copy()
        if isinstance(records_df, pd.DataFrame)
        else pd.DataFrame()
    )
    expected = {str(value) for value in expected_products or ()}
    result: dict[str, list[object]] = {"报警类型": SUMMARY_ROW_ORDER.copy()}
    for window in _period_windows(end_date):
        rows = (
            records[records["时间标签"].astype(str).eq(window.time_label)]
            if not records.empty
            else records
        )
        complete_products = not expected or (
            not rows.empty and set(rows["产品"].astype(str)) == expected
        )
        values: list[object] = []
        for column in SUMMARY_ROW_ORDER:
            numeric = pd.to_numeric(rows[column], errors="coerce")
            if not complete_products or rows.empty or numeric.isna().any():
                values.append("—")
            else:
                values.append(int(numeric.sum()))
        result[window.display_label] = values
    return pd.DataFrame(result)


__all__ = [
    "MONITOR_SUMMARY_COLUMNS",
    "SUMMARY_ROW_ORDER",
    "build_current_period_records",
    "build_period_summary",
    "build_period_summary_from_records",
    "current_period_windows",
]
