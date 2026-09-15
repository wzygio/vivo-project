"""Read-only lifetime use case and consumer-owned source port."""

from typing import Protocol

import pandas as pd

from src.iqc_domain.core.lifetime.lifetime import project_report


class LifetimeSourceUnavailable(RuntimeError):
    """The source could not supply a complete report."""


class LifetimeDataPort(Protocol):
    def read_report(self) -> pd.DataFrame:
        """Return complete source measurements for stable per-group numbering."""
        ...


class LifetimeReportService:
    def __init__(self, source: LifetimeDataPort) -> None:
        self._source = source

    def get_report(self) -> pd.DataFrame:
        return project_report(self._source.read_report())
