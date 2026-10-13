"""Read-only evaporation report use case and outbound contract."""

from collections.abc import Sequence
from datetime import date, datetime
from typing import Protocol

import pandas as pd

from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS, project_report


class IqcSourceUnavailable(RuntimeError):
    """The complete material report could not be read."""


class EvaporationDataPort(Protocol):
    def read_report(self, start: date, end: date, products: tuple[str, ...]) -> pd.DataFrame:
        """Read material facts eligible through any selected product's cycle."""
        ...


def validate_dates(start: date, end: date) -> None:
    if (
        not isinstance(start, date) or isinstance(start, datetime)
        or not isinstance(end, date) or isinstance(end, datetime) or start > end
    ):
        raise ValueError("IQC_DATE_WINDOW_INVALID")


class EvaporationReportService:
    def __init__(self, source: EvaporationDataPort, *, enabled_products: Sequence[str]) -> None:
        self._source = source
        self._enabled_products = tuple(dict.fromkeys(enabled_products))

    def get_report(
        self, start: date, end: date, products: Sequence[str] = (),
    ) -> pd.DataFrame:
        validate_dates(start, end)
        if isinstance(products, str):
            raise ValueError("IQC_PRODUCT_SCOPE_INVALID")
        requested = set(products) if products else set(self._enabled_products)
        effective = tuple(code for code in self._enabled_products if code in requested)
        if not effective:
            return pd.DataFrame(columns=REPORT_COLUMNS)
        return project_report(self._source.read_report(start, end, effective))
