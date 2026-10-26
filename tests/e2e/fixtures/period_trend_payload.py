"""Bounded synthetic annual periods for standalone Streamlit fixtures."""
from datetime import datetime

import pandas as pd

from src.yield_domain.core.mwd_trend.period_trend import aggregate_periods, RATE_COLUMNS


def period_payload(mode='normal'):
    now = datetime.now()
    months = [f'{now.year}-{m:02}' for m in range(1, now.month+1)]
    rows = [[month, level, group, code, code, rate]
            for month in months
            for group, rate in [('Array_Line', .01), ('Array_Mura', .004), ('OLED_Mura', .011)]
            for level, code in [('Group', ''), ('Code', group+'-C')]]
    monthly = pd.DataFrame(rows, columns=RATE_COLUMNS)
    if mode == 'empty':
        monthly = monthly.iloc[:0]
    if mode == 'stale':
        monthly['defect_rate'] = float('nan')
    inputs = pd.DataFrame({'month': months, 'total_panels': 1000})
    rates, volumes = aggregate_periods(monthly, inputs, now.year, now.month)
    return {'rates': rates, 'volumes': volumes}
