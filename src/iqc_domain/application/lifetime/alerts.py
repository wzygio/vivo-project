"""Publish lifetime failures for the complete enabled product scope."""

from collections.abc import Mapping
from datetime import date
from typing import Protocol

import pandas as pd

from src.iqc_domain.core.lifetime.alerts import evaluate_lifetime_specs


class LifetimeAlertStore(Protocol):
    def save(
        self, evaluations: pd.DataFrame, *, reference_date: date, products: tuple[str, ...],
    ) -> pd.DataFrame:
        """Reconcile current measurements with saved decisions and return released alerts."""
        ...


def publish_lifetime_alerts(
    report: pd.DataFrame, specs: Mapping[tuple[str, str], float], *,
    store: LifetimeAlertStore, reference_date: date, products: tuple[str, ...],
) -> pd.DataFrame:
    scoped = report.loc[report['产品型号'].isin(products)]
    evaluations = evaluate_lifetime_specs(scoped, specs)
    return store.save(evaluations, reference_date=reference_date, products=products)
