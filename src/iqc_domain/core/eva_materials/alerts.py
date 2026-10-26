"""Select failed inspections from the current public report results."""

from datetime import date, timedelta

import pandas as pd

from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS


def previous_week_window(today: date) -> tuple[date, date]:
    end = today - timedelta(days=today.weekday() + 1)
    return end - timedelta(days=6), end


def build_evaporation_alerts(report: pd.DataFrame) -> pd.DataFrame:
    if report.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)
    if not {'IQC结果', 'COA结果'} <= set(report):
        raise ValueError('IQC_ALERT_SCHEMA_INVALID')
    failed = report['IQC结果'].eq('NG') | report['COA结果'].eq('NG')
    return report.loc[failed].reindex(columns=REPORT_COLUMNS).copy().reset_index(drop=True)
