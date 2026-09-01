"""IJP overflow report use cases."""

from __future__ import annotations

from datetime import datetime
from typing import TypedDict

import pandas as pd

from src.qtime_domain.application.ijp.dtos import IjpQuery
from src.qtime_domain.application.ijp.ports import IjpDataPort
from src.qtime_domain.core.ijp_overflow import (
    IJP_EQUIPMENTS,
    IJP_LINES,
    IJP_RS_CODES,
    PANEL_LOCATIONS,
)


class IjpFilterOptions(TypedDict):
    product_codes: tuple[str, ...]
    product_names: tuple[str, ...]
    sub_prod_types: tuple[str, ...]
    picis: tuple[str, ...]
    cycles: tuple[str, ...]
    lines: tuple[str, ...]
    equipments: tuple[str, ...]
    codes: tuple[str, ...]
    panel_locations: tuple[str, ...]


class IjpReportService:
    def __init__(self, data_port: IjpDataPort) -> None:
        self._data_port = data_port

    def get_filter_options(
        self,
        start_time: datetime,
        end_time: datetime,
        product_codes: tuple[str, ...] = (),
        picis: tuple[str, ...] = (),
    ) -> IjpFilterOptions:
        return {
            "product_codes": self._data_port.list_product_codes(),
            "product_names": self._data_port.list_product_names(product_codes),
            "sub_prod_types": self._data_port.list_sub_prod_types(),
            "picis": self._data_port.list_picis(start_time, end_time, product_codes),
            "cycles": self._data_port.list_cycles(
                start_time, end_time, product_codes, picis
            ),
            "lines": IJP_LINES,
            "equipments": IJP_EQUIPMENTS,
            "codes": IJP_RS_CODES,
            "panel_locations": PANEL_LOCATIONS,
        }

    def get_daily_ratios(self, query: IjpQuery) -> pd.DataFrame:
        return self._data_port.fetch_daily_ratios(query)

    def get_details(self, query: IjpQuery) -> pd.DataFrame:
        return self._data_port.fetch_details(query)
