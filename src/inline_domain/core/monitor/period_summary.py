"""Calendar-period summary used by the automatic warning table."""

from __future__ import annotations

import pandas as pd


SUMMARY_ROW_ORDER = [
    "过货量",
    "OOC报警片数",
    "SOOS报警片数",
    "OOS报警片数",
    "Total报警片数",
]


def _period_windows(end_date: pd.Timestamp) -> list[tuple[str, pd.Timestamp, pd.Timestamp]]:
    end = pd.Timestamp(end_date).normalize()
    exclusive_end = end + pd.Timedelta(days=1)
    windows: list[tuple[str, pd.Timestamp, pd.Timestamp]] = [
        (f"Y{end.year % 100:02d}", pd.Timestamp(end.year, 1, 1), exclusive_end)
    ]
    current_quarter = (end.month - 1) // 3 + 1
    for quarter in range(1, current_quarter + 1):
        start = pd.Timestamp(end.year, (quarter - 1) * 3 + 1, 1)
        quarter_end = start + pd.DateOffset(months=3)
        windows.append((f"Q{quarter}", start, min(quarter_end, exclusive_end)))
    month_start = end.replace(day=1)
    for offset in (2, 1, 0):
        start = month_start - pd.DateOffset(months=offset)
        month_end = start + pd.DateOffset(months=1)
        windows.append((f"M{start.month}", start, min(month_end, exclusive_end)))
    week_start = end - pd.Timedelta(days=end.weekday())
    windows.append((f"W{end.isocalendar().week}", week_start, exclusive_end))
    return windows


def build_period_summary(
    alerts_df: pd.DataFrame,
    throughput_df: pd.DataFrame,
    *,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """Build overlapping year/quarter/month/week totals through ``end_date``."""
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

    result: dict[str, list[int | str]] = {"报警类型": SUMMARY_ROW_ORDER.copy()}
    for label, start, end in _period_windows(pd.Timestamp(end_date)):
        period_alerts = (
            alerts[(alerts["event_time"] >= start) & (alerts["event_time"] < end)]
            if not alerts.empty
            else alerts
        )
        period_throughput = (
            throughput[
                (throughput["event_date"] >= start)
                & (throughput["event_date"] < end)
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
        oos = alarm_sheet_count("OOS")
        soos = 0
        total = (
            len(period_alerts.drop_duplicates(["factory", "prod_code", "item_id"]))
            if not period_alerts.empty
            else 0
        )
        result[label] = [len(period_throughput), ooc, soos, oos, total]
    return pd.DataFrame(result)


__all__ = ["SUMMARY_ROW_ORDER", "build_period_summary"]
