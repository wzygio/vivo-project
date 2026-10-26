"""Previous ISO week alerts evaluated from the current report projection."""

from datetime import date, timedelta

import pandas as pd

ALERT_COLUMNS = [
    'region', 'productcode', 'line', 'printer', 'rs_code',
    'period_start', 'period_end', 'ratio', 'minimum',
]
ALERT_CODES = {'AA区': 'C3DM1', '孔区': 'C3RA1'}
ALERT_MINIMUM = 0.9


def previous_week_range(today: date) -> tuple[date, date]:
    end = today - timedelta(days=today.weekday() + 1)
    return end - timedelta(days=6), end


def build_weekly_alerts(frame: pd.DataFrame, region: str, today: date) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=ALERT_COLUMNS)
    start, end = previous_week_range(today)
    ratio_column = 'display_ratio' if region == 'AA区' else 'ratio'
    selected = frame.loc[
        frame.rs_code.eq(ALERT_CODES[region]) & frame.has_data.eq(True)
        & frame.period_start.eq(str(start)) & frame.period_end.eq(str(end))
        & frame[ratio_column].lt(ALERT_MINIMUM)
    ].copy()
    selected['region'] = region
    selected['ratio'] = selected[ratio_column]
    selected['minimum'] = ALERT_MINIMUM
    return selected.reindex(columns=ALERT_COLUMNS).reset_index(drop=True)
