"""Two calendar months and three Monday-based weeks for each Printer."""

from datetime import date, timedelta

import pandas as pd

from src.indicator_domain.core.ijp.printer_summary import (
    PRINTER_CODES, PRINTER_DIMENSIONS, SUMMARY_COLUMNS, summarize_printers,
)

CODE_DESCRIPTIONS = {
    "C3DM0": "Ink未到DAM1",
    "C3DM1": "Ink到DAM1",
    "C3DM2": "Ink溢出DAM1未到DAM2",
    "C3DM3": "Ink到DAM2",
    "C3DM4": "Ink溢出DAM2",
    "C3DM5": "拍图异常（例如模糊/偏移等无法判定的情况）",
}


def reporting_periods(today: date) -> list[tuple[str, date, date]]:
    month = today.replace(day=1)
    previous_end = month - timedelta(days=1)
    window_start = previous_end.replace(day=1)
    periods = [
        (f"{window_start:%Y-%m}（月）", window_start, previous_end),
        (f"{month:%Y-%m}（月）", month, today),
    ]
    monday = today - timedelta(days=today.weekday())
    for offset in (2, 1, 0):
        start = monday - timedelta(weeks=offset)
        end = min(start + timedelta(days=6), today)
        periods.append((f"{start:%m/%d}–{end:%m/%d}（周）", max(start, window_start), end))
    return periods


def summarize_periods(
    daily_counts: pd.DataFrame, today: date, *, decorate: bool = True,
    minimum: float = 0.9, maximum: float = 0.92,
) -> pd.DataFrame:
    columns = [*SUMMARY_COLUMNS, "period", "period_start", "period_end", "has_data"]
    if daily_counts.empty:
        return pd.DataFrame(columns=columns)
    frame = daily_counts.copy()
    frame["day"] = pd.to_datetime(frame["day"], errors="raise").dt.date
    periods = reporting_periods(today)
    frame = frame[frame.day.between(periods[0][1], today)]
    frame = frame[frame.rs_code.isin(PRINTER_CODES)]
    groups = frame[PRINTER_DIMENSIONS].drop_duplicates()
    outputs = []
    for label, start, end in periods:
        selected = frame[frame.day.between(start, end)]
        summary = summarize_printers(
            selected, decorate=decorate, minimum=minimum, maximum=maximum,
        )
        grid = groups.merge(pd.DataFrame({"rs_code": PRINTER_CODES}), how="cross")
        result = grid.merge(summary, on=[*PRINTER_DIMENSIONS, "rs_code"], how="left")
        result["has_data"] = result.raw_ratio.notna()
        result[["code_num", "raw_ratio", "display_ratio"]] = result[
            ["code_num", "raw_ratio", "display_ratio"]
        ].astype(float).fillna(0)
        result["period"] = label
        result["period_start"] = str(start)
        result["period_end"] = str(end)
        outputs.append(result)
    return pd.concat(outputs, ignore_index=True).reindex(columns=columns)
