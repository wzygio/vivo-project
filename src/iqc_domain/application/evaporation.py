"""Read-only evaporation report use case and outbound contract."""

from datetime import date, datetime
from typing import Protocol

import pandas as pd

from src.iqc_domain.core.evaporation import project_report


class IqcSourceUnavailable(RuntimeError):
    """The complete material report could not be read."""


class EvaporationDataPort(Protocol):
    def read_report(self, start: date, end: date) -> pd.DataFrame:
        """Read all report facts for an inclusive display-date window."""
        ...


def validate_dates(start: date, end: date) -> None:
    if (
        not isinstance(start, date) or isinstance(start, datetime)
        or not isinstance(end, date) or isinstance(end, datetime) or start > end
    ):
        raise ValueError("IQC_DATE_WINDOW_INVALID")


class EvaporationReportService:
    def __init__(self, source: EvaporationDataPort) -> None:
        self._source = source

    def get_report(self, start: date, end: date) -> pd.DataFrame:
        validate_dates(start, end)
        return project_report(self._source.read_report(start, end))
