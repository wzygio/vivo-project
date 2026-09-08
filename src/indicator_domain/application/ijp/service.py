"""IJP overflow report use cases."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from typing import TypedDict

import pandas as pd

from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.ports import IjpDataPort
from src.indicator_domain.core.ijp.overflow import (
    IJP_LINES,
    IJP_RS_CODES,
)


class IjpFilterOptions(TypedDict):
    product_codes: tuple[str, ...]
    picis: tuple[str, ...]
    lines: tuple[str, ...]
    codes: tuple[str, ...]


class IjpReportService:
    def __init__(
        self,
        data_port: IjpDataPort,
        enabled_product_codes: tuple[str, ...] = (),
        work_order_types: tuple[str, ...] = (),
        today_provider: Callable[[], date] | None = None,
    ) -> None:
        self._data_port = data_port
        self._enabled_product_codes = tuple(dict.fromkeys(enabled_product_codes))
        self._work_order_types = tuple(dict.fromkeys(work_order_types))
        self._today_provider = today_provider or date.today

    def get_reporting_window(self) -> tuple[datetime, datetime]:
        return build_reporting_window(self._today_provider())

    def get_filter_options(
        self,
        product_codes: tuple[str, ...] = (),
    ) -> IjpFilterOptions:
        start_time, end_time = self.get_reporting_window()
        available_product_codes = self._data_port.list_product_codes()
        if self._enabled_product_codes:
            enabled = set(self._enabled_product_codes)
            available_product_codes = tuple(
                code for code in available_product_codes if code in enabled
            )
        scoped_product_codes = self._scope_product_codes(product_codes)
        return {
            "product_codes": available_product_codes,
            "picis": self._data_port.list_picis(
                start_time,
                end_time,
                scoped_product_codes,
            ),
            "lines": IJP_LINES,
            "codes": IJP_RS_CODES,
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
        start_time, end_time = self.get_reporting_window()
        return query.model_copy(
            update={
                "start_time": start_time,
                "end_time": end_time,
                "product_codes": self._scope_product_codes(query.product_codes),
                "work_order_types": self._work_order_types,
            }
        )


def build_reporting_window(today: date) -> tuple[datetime, datetime]:
    """Return the inclusive window from last month's first day through today."""
    current_month_start = today.replace(day=1)
    previous_month_last = current_month_start - timedelta(days=1)
    previous_month_start = previous_month_last.replace(day=1)
    return (
        datetime.combine(previous_month_start, time.min),
        datetime.combine(today, time.max),
    )
