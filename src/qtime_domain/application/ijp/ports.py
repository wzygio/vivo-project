"""Outbound data contract owned by the IJP overflow application layer."""

from datetime import datetime
from typing import Protocol

import pandas as pd

from src.qtime_domain.application.ijp.dtos import IjpQuery


class IjpDataPort(Protocol):
    def list_product_codes(self) -> tuple[str, ...]: ...

    def list_product_names(self, product_codes: tuple[str, ...]) -> tuple[str, ...]: ...

    def list_sub_prod_types(self) -> tuple[str, ...]: ...

    def list_picis(
        self,
        start_time: datetime,
        end_time: datetime,
        product_codes: tuple[str, ...],
    ) -> tuple[str, ...]: ...

    def list_cycles(
        self,
        start_time: datetime,
        end_time: datetime,
        product_codes: tuple[str, ...],
        picis: tuple[str, ...],
    ) -> tuple[str, ...]: ...

    def fetch_daily_ratios(self, query: IjpQuery) -> pd.DataFrame: ...

    def fetch_details(self, query: IjpQuery) -> pd.DataFrame: ...
