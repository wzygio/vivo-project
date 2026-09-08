"""IJP overflow report use cases."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from typing import TypedDict

import pandas as pd

from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.ports import IjpDataPort
from src.indicator_domain.application.ijp.settings import IjpSettings
from src.indicator_domain.core.ijp.printer_summary import summarize_printers
from src.indicator_domain.core.ijp.overflow import (
    IJP_LINES,
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
        settings: IjpSettings | None = None,
    ) -> None:
        self._data_port = data_port
        self._enabled_product_codes = tuple(dict.fromkeys(enabled_product_codes))
        self._work_order_types = tuple(dict.fromkeys(work_order_types))
        self._today_provider = today_provider or date.today
        self.settings = settings or IjpSettings()

    def get_reporting_window(self) -> tuple[datetime, datetime]:
        return build_reporting_window(self._today_provider())

    def get_filter_options(
        self,
        product_codes: tuple[str, ...] = (),
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> IjpFilterOptions:
        default_start, default_end = self.get_reporting_window()
        start_time = start_time if start_time is not None else default_start
        end_time = end_time if end_time is not None else default_end
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
            "codes": self.settings.codes,
        }

    def get_glass_ratios(self, query: IjpQuery) -> pd.DataFrame:
        return self._data_port.fetch_glass_ratios(self._scope_query(query))

    def get_details(self, query: IjpQuery) -> pd.DataFrame:
        return self._data_port.fetch_details(self._scope_query(query))

    def get_printer_ratios(self, query: IjpQuery) -> pd.DataFrame:
        scoped = self._scope_query(query)
        return summarize_printers(
            self._data_port.fetch_glass_ratios(scoped),
            decorate="C3DM1" in scoped.codes,
            minimum=self.settings.c3dm1_minimum,
            maximum=self.settings.c3dm1_maximum,
        )

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
        if set(query.codes) - set(self.settings.codes):
            raise ValueError("所选 CODE 不在 IJP 配置允许范围内")
        return query.model_copy(
            update={
                "codes": query.codes or self.settings.codes,
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
