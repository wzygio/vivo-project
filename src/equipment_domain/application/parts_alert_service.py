"""Publish current critical-parts alerts through a caller-owned store."""

import pandas as pd

from src.equipment_domain.application.ports import PartsAlertStore
from src.equipment_domain.core.parts_alerts import build_parts_alerts


def publish_parts_alerts(
    report_df: pd.DataFrame, *, store: PartsAlertStore,
    as_of_date: str, last_update: str,
) -> pd.DataFrame:
    alerts = build_parts_alerts(report_df)
    store.save(alerts, as_of_date=as_of_date, last_update=last_update)
    return alerts
