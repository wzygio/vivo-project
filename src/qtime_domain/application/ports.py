"""Outbound data contract owned by the Q-Time application layer."""

from typing import Protocol

import pandas as pd

from src.qtime_domain.application.dtos import QTimeQuery, Shop


class QTimeDataPort(Protocol):
    def list_products(self) -> tuple[str, ...]: ...

    def list_step_descriptions(self, shop: Shop) -> tuple[str, ...]: ...

    def fetch_details(self, query: QTimeQuery) -> pd.DataFrame: ...
