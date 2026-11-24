"""Read-only lifetime use case and consumer-owned source port."""

from typing import Protocol
from collections.abc import Callable

import pandas as pd

from src.iqc_domain.core.lifetime.lifetime import project_report


class LifetimeSourceUnavailable(RuntimeError):
    """The source could not supply a complete report."""


class LifetimeDataPort(Protocol):
    def read_report(self) -> pd.DataFrame:
        """Return complete source measurements for stable per-group numbering."""
        ...


class LifetimeReportService:
    def __init__(
        self, source: LifetimeDataPort, *, enabled_products: tuple[str, ...] | None = None,
        sample_numberer: Callable[[pd.DataFrame], pd.Series] | None = None,
    ) -> None:
        self._source = source
        self._enabled_products = enabled_products
        self._sample_numberer = sample_numberer

    def get_report(self, *, sample_numberer: Callable[[pd.DataFrame], pd.Series] | None = None) -> pd.DataFrame:
        return project_report(self._source.read_report(), enabled_products=self._enabled_products,
                              sample_numberer=sample_numberer or self._sample_numberer)
