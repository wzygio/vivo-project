"""Read maintained capability alerts without calculating or publishing reports."""
from __future__ import annotations

from datetime import date
from typing import Protocol

import pandas as pd

from src.inline_domain.core.monitor.cpk_latest import (
    capability_week_window, select_capability_alert_records,
)
from src.inline_domain.core.monitor.cpk_summary import capability_summary_columns
from src.inline_domain.core.shared.date_exclusion import InlineDateExclusion
from src.inline_domain.core.spc.cpk_decoration import CAPABILITY_ALERT_MIN_SPAN_HOURS


class CapabilityReader(Protocol):
    def read_product(self, product: str) -> pd.DataFrame | None: ...


class CapabilityAlertQuery:
    """Share saved-result rules while preserving explicit source availability."""

    def __init__(self, reader: CapabilityReader, *, metric: str = "cpk",
                 date_exclusion: InlineDateExclusion | None = None,
                 min_span_hours: float = CAPABILITY_ALERT_MIN_SPAN_HOURS) -> None:
        capability_summary_columns(metric)
        self._reader = reader
        self._metric = metric
        self._date_exclusion = date_exclusion
        self._min_span_hours = min_span_hours

    def read(self, product: str, reference_date: date, *, start_date: str | date | None = None,
             end_date: str | date | None = None) -> dict:
        window = capability_week_window(reference_date, start_date=start_date, end_date=end_date)
        if window is None:
            return self._result(pd.DataFrame(columns=["flag"]), availability="ready")
        detail = self._reader.read_product(product)
        if detail is None:
            return self._result(pd.DataFrame(columns=["flag"]), availability="missing")
        # A readable, validated product ledger can have no rows in the selected
        # weeks. That is a successful empty selection, not a missing source.
        records = select_capability_alert_records(
            detail, reference_date, metric=self._metric, date_exclusion=self._date_exclusion,
            min_span_hours=self._min_span_hours, start_date=start_date, end_date=end_date,
        )
        return self._result(records, availability="ready")

    @staticmethod
    def _result(records: pd.DataFrame, *, availability: str) -> dict:
        return {"state": "no_data" if availability == "missing" else "alert" if len(records) else "ok",
                "records": records.copy(), "metadata": {"availability": availability}}
