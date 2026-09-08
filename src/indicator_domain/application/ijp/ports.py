"""Outbound data contract owned by the IJP overflow application layer."""

from datetime import datetime
from typing import Protocol

import pandas as pd

from src.indicator_domain.application.ijp.dtos import IjpQuery


class IjpDataPort(Protocol):
    def list_product_codes(self) -> tuple[str, ...]: ...

    def list_picis(
        self,
        start_time: datetime,
        end_time: datetime,
        product_codes: tuple[str, ...],
    ) -> tuple[str, ...]: ...

    def fetch_glass_ratios(self, query: IjpQuery) -> pd.DataFrame: ...

    def fetch_daily_counts(self, query: IjpQuery) -> pd.DataFrame: ...

    def fetch_details(self, query: IjpQuery) -> pd.DataFrame: ...
