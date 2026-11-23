"""Publish lifetime failures for the complete enabled product scope."""

from collections.abc import Mapping
from datetime import date
from typing import Protocol

import pandas as pd

from src.iqc_domain.core.lifetime.alerts import evaluate_lifetime_specs, lifetime_spec_alerts


class LifetimeAlertStore(Protocol):
    def save(
        self, alerts: pd.DataFrame, *, reference_date: date, products: tuple[str, ...],
    ) -> None: ...


def publish_lifetime_alerts(
    report: pd.DataFrame, specs: Mapping[tuple[str, str], float], *,
    store: LifetimeAlertStore, reference_date: date, products: tuple[str, ...],
) -> pd.DataFrame:
    scoped = report.loc[report['产品型号'].isin(products)]
    alerts = lifetime_spec_alerts(evaluate_lifetime_specs(scoped, specs))
    store.save(alerts, reference_date=reference_date, products=products)
    return alerts
