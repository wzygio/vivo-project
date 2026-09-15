"""Scope products before every outbound read and keep hole ratios independent."""

from collections.abc import Callable
from datetime import date, datetime

import pandas as pd

from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.service import IjpFilterOptions, build_reporting_window
from src.indicator_domain.application.ijp_hole.ports import IjpHoleDataPort
from src.indicator_domain.core.ijp_hole.summary import HOLE_CODES, summarize_holes


class IjpHoleReportService:
    def __init__(
        self, data_port: IjpHoleDataPort, enabled_products: tuple[str, ...],
        work_order_types: tuple[str, ...] = (),
        today_provider: Callable[[], date] = date.today,
    ) -> None:
        self._data_port = data_port
        self.enabled_products = tuple(dict.fromkeys(enabled_products))
        self.work_order_types = work_order_types
        self._today_provider = today_provider

    def get_reporting_window(self) -> tuple[datetime, datetime]:
        return build_reporting_window(self._today_provider())

    def get_filter_options(self, products: tuple[str, ...] = ()) -> IjpFilterOptions:
        start, end = self.get_reporting_window()
        query = self._scope(IjpQuery(start_time=start, end_time=end, product_codes=products))
        return {
            'product_codes': self.enabled_products,
            'lines': ('3CEE01', '3CEE02'), 'codes': HOLE_CODES,
            'picis': self._data_port.list_picis(query) if query.product_codes else (),
        }

    def get_report(self, query: IjpQuery) -> dict[str, pd.DataFrame]:
        scoped = self._scope(query)
        counts = self._data_port.fetch_counts(scoped) if scoped.product_codes else pd.DataFrame()
        # Defensive public boundary also protects injected adapters.
        if not counts.empty:
            counts = counts[counts.productcode.isin(scoped.product_codes)]
        return summarize_holes(counts, scoped.codes)

    def _scope(self, query: IjpQuery) -> IjpQuery:
        if set(query.codes) - set(HOLE_CODES):
            raise ValueError('孔区 CODE 仅支持 ' + '、'.join(HOLE_CODES))
        products = tuple(p for p in self.enabled_products
                         if not query.product_codes or p in query.product_codes)
        start, end = self.get_reporting_window()
        return query.model_copy(update={
            'start_time': start, 'end_time': end, 'product_codes': products,
            'work_order_types': self.work_order_types,
        })
