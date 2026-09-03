"""IJP overflow report use cases."""

from __future__ import annotations

from datetime import datetime
from typing import TypedDict

import pandas as pd

from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.ports import IjpDataPort
from src.indicator_domain.core.ijp.overflow import (
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
    def __init__(
        self,
        data_port: IjpDataPort,
        enabled_product_codes: tuple[str, ...] = (),
    ) -> None:
        self._data_port = data_port
        self._enabled_product_codes = tuple(dict.fromkeys(enabled_product_codes))

    def get_filter_options(
        self,
        start_time: datetime,
        end_time: datetime,
        product_codes: tuple[str, ...] = (),
        picis: tuple[str, ...] = (),
    ) -> IjpFilterOptions:
        available_product_codes = self._data_port.list_product_codes()
        if self._enabled_product_codes:
            enabled = set(self._enabled_product_codes)
            available_product_codes = tuple(
                code for code in available_product_codes if code in enabled
            )
        scoped_product_codes = self._scope_product_codes(product_codes)
        return {
            "product_codes": available_product_codes,
            "product_names": self._data_port.list_product_names(
                scoped_product_codes
            ),
            "sub_prod_types": self._data_port.list_sub_prod_types(),
            "picis": self._data_port.list_picis(
                start_time,
                end_time,
                scoped_product_codes,
            ),
            "cycles": self._data_port.list_cycles(
                start_time,
                end_time,
                scoped_product_codes,
                picis,
            ),
            "lines": IJP_LINES,
            "equipments": IJP_EQUIPMENTS,
            "codes": IJP_RS_CODES,
            "panel_locations": PANEL_LOCATIONS,
        }

    def get_daily_ratios(self, query: IjpQuery) -> pd.DataFrame:
        return self._data_port.fetch_daily_ratios(self._scope_query(query))

    def get_details(self, query: IjpQuery) -> pd.DataFrame:
        return self._data_port.fetch_details(self._scope_query(query))

    def _scope_product_codes(
        self,
        product_codes: tuple[str, ...],
    ) -> tuple[str, ...]:
        if not self._enabled_product_codes:
            return product_codes
        if not product_codes:
            return self._enabled_product_codes
        enabled = set(self._enabled_product_codes)
        return tuple(code for code in product_codes if code in enabled)

    def _scope_query(self, query: IjpQuery) -> IjpQuery:
        if not self._enabled_product_codes:
            return query
        return query.model_copy(
            update={"product_codes": self._scope_product_codes(query.product_codes)}
        )
