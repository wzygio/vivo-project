"""Q-Time report use cases."""

from __future__ import annotations

from typing import TypedDict

import pandas as pd

from src.qtime_domain.application.dtos import QTimeQuery, Shop
from src.qtime_domain.application.ports import QTimeDataPort


class QTimeFilterOptions(TypedDict):
    step_descriptions: tuple[str, ...]


class QTimeReportService:
    def __init__(self, data_port: QTimeDataPort) -> None:
        self._data_port = data_port

    def get_filter_options(self, shop: Shop) -> QTimeFilterOptions:
        return {
            "step_descriptions": self._data_port.list_step_descriptions(shop),
        }

    def get_report(self, query: QTimeQuery) -> pd.DataFrame:
        return self._data_port.fetch_details(query)
