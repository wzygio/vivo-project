"""Publish failed IQC/COA inspections via an application-owned store port."""

from datetime import date
from typing import Protocol

import pandas as pd

from src.iqc_domain.application.eva_materials.evaporation import validate_dates
from src.iqc_domain.core.eva_materials.alerts import build_evaporation_alerts


class EvaporationAlertStore(Protocol):
    def save(
        self, alerts: pd.DataFrame, *, start: date, end: date, products: tuple[str, ...],
    ) -> None: ...


def publish_evaporation_alerts(
    report: pd.DataFrame, *, store: EvaporationAlertStore,
    start: date, end: date, products: tuple[str, ...],
) -> pd.DataFrame:
    validate_dates(start, end)
    alerts = build_evaporation_alerts(report)
    store.save(alerts, start=start, end=end, products=products)
    return alerts
