"""Read maintained CPK period results without analysing raw measurements."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

import pandas as pd

from src.inline_domain.application.monitor.cpk_monitor_service import CpkMonitorViewModel
from src.inline_domain.application.monitor.summary_workbook_service import ALL_FACTORIES
from src.inline_domain.core.monitor.cpk_summary import (
    CPK_DETAIL_COLUMNS, build_cpk_summary_from_records, capability_summary_columns,
)
from src.inline_domain.core.shared.date_exclusion import InlineDateExclusion, exclude_inline_period_records
from src.shared_kernel.config import ConfigLoader


class CpkSummaryReader(Protocol):
    def read(self) -> pd.DataFrame: ...

    def refresh_latest(
        self, detail: pd.DataFrame, *, products: Iterable[str],
        factories: Iterable[str], factory_key: str, as_of: pd.Timestamp,
        date_exclusion: InlineDateExclusion | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]: ...


class CpkLatestReader(Protocol):
    def read_product(self, product: str) -> pd.DataFrame | None: ...


class CpkWorkbookMonitorService:
    """Maintain the all-factory latest week and independently filter project details."""

    def __init__(self, store: CpkSummaryReader, *, latest_reader: CpkLatestReader | None = None,
                 metric: str = "cpk") -> None:
        capability_summary_columns(metric)
        self._metric = metric
        self._store = store
        self._latest_reader = latest_reader

    def build_dashboard(
        self, *, products: Iterable[str], factories: Iterable[str],
        end_date: object | None = None,
    ) -> CpkMonitorViewModel:
        products = tuple(sorted(set(products)))
        factories = tuple(sorted(set(factories)))
        as_of = pd.Timestamp(end_date if end_date is not None else pd.Timestamp.today()).normalize()
        detail = pd.DataFrame(columns=[
            *(column.replace("cpk", self._metric) for column in CPK_DETAIL_COLUMNS), "status",
        ])
        status = pd.DataFrame()
        date_exclusion = ConfigLoader.get_inline_data_exclusion()
        if self._latest_reader is not None:
            frames = [self._latest_reader.read_product(product) for product in products]
            frames = [frame for frame in frames if frame is not None and not frame.empty]
            if frames:
                detail = pd.concat(frames, ignore_index=True)
                previous = as_of - pd.Timedelta(days=as_of.weekday() + 7)
                iso = previous.isocalendar()
                latest = (detail["period_type"].eq("week")
                          & detail["period_label"].eq(f"{iso.year}-W{iso.week:02d}"))
                latest |= (detail["period_type"].eq("month")
                           & detail["period_label"].eq(f"{as_of.year}-{as_of.month:02d}"))
                detail = detail.loc[latest].copy()
            records, status = self._store.refresh_latest(
                detail, products=products, factories=sorted(ALL_FACTORIES),
                factory_key="ALL", as_of=as_of, date_exclusion=date_exclusion,
            )
            updated_at = getattr(self._latest_reader, "source_updated_at", None)
            if updated_at is not None:
                status["来源文件更新时间"] = updated_at()
        else:
            records = self._store.read()
        # Detail exclusions and widget factories do not change the all-factory baseline.
        detail = exclude_inline_period_records(detail, date_exclusion, as_of=as_of)
        detail = detail.loc[detail["factory"].isin(factories)].copy()
        selected = records.loc[
            records["产品"].isin(products)
            & records["监控类型"].eq("SPC")
            & records["厂别"].eq("ALL")
        ]
        return CpkMonitorViewModel(
            build_cpk_summary_from_records(
                selected, end_date=as_of, expected_products=products, week_count=4, metric=self._metric,
            ),
            detail,
            refresh_status_df=status,
        )
