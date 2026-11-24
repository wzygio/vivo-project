"""Read maintained CPK period results without analysing raw measurements."""

from __future__ import annotations

from collections.abc import Iterable
from numbers import Real
from typing import Protocol

import pandas as pd

from src.inline_domain.application.monitor.cpk_monitor_service import CpkMonitorViewModel
from src.inline_domain.application.monitor.summary_workbook_service import ALL_FACTORIES
from src.inline_domain.core.monitor.cpk_summary import (
    CPK_DETAIL_COLUMNS, build_cpk_summary_from_records, capability_summary_columns,
)
from src.inline_domain.core.shared.date_exclusion import InlineDateExclusion, exclude_inline_period_records
from src.inline_domain.core.monitor.capability_trend import (
    normalize_capability_history, week_owner_month,
)
from src.inline_domain.core.spc.cpk_decoration import CPK_KEY_COLUMNS
from src.inline_domain.core.monitor.cpk_latest import public_capability_history, released_capability_mask
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


class CapabilityWeeklySource(Protocol):
    def read_weekly_totals(
        self, product: str, weeks: Iterable[str], *, metric: str, as_of: pd.Timestamp,
    ) -> pd.DataFrame: ...


class CpkWorkbookMonitorService:
    """Maintain the all-factory latest week and independently filter project details."""

    def __init__(self, store: CpkSummaryReader, *, latest_reader: CpkLatestReader | None = None,
                 metric: str = "cpk", weekly_source: CapabilityWeeklySource | None = None) -> None:
        capability_summary_columns(metric)
        self._metric = metric
        self._store = store
        self._latest_reader = latest_reader
        self._weekly_source = weekly_source

    def build_dashboard(
        self, *, products: Iterable[str], factories: Iterable[str],
        end_date: object | None = None,
    ) -> CpkMonitorViewModel:
        products = tuple(sorted(set(products)))
        factories = tuple(sorted(set(factories)))
        as_of = pd.Timestamp(end_date if end_date is not None else pd.Timestamp.today()).normalize()
        if self._weekly_source is not None:
            return self._build_trend_dashboard(products, factories, as_of)
        detail = pd.DataFrame(columns=[
            *(column.replace("cpk", self._metric) for column in CPK_DETAIL_COLUMNS), "status",
        ])
        all_detail = detail
        status = pd.DataFrame()
        date_exclusion = ConfigLoader.get_inline_data_exclusion()
        if self._latest_reader is not None:
            frames = [self._latest_reader.read_product(product) for product in products]
            frames = [frame for frame in frames if frame is not None and not frame.empty]
            if frames:
                all_detail = detail = pd.concat(frames, ignore_index=True)
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
        all_detail = exclude_inline_period_records(all_detail, date_exclusion, as_of=as_of)
        selected = public_capability_history(selected, all_detail, self._metric)
        if "flag" in detail:
            detail = detail.loc[~detail["flag"].astype(str).str.strip().str.lower().eq("delete")]
        return CpkMonitorViewModel(
            build_cpk_summary_from_records(
                selected, end_date=as_of, expected_products=products, week_count=4, metric=self._metric,
            ),
            detail,
            refresh_status_df=status,
            product_summary_dfs=self._product_summaries(selected, products, as_of),
        )

    def _product_summaries(
        self, records: pd.DataFrame, products: tuple[str, ...], as_of: pd.Timestamp,
    ) -> dict[str, pd.DataFrame]:
        return {
            product: build_cpk_summary_from_records(
                records.loc[records["产品"].eq(product)], end_date=as_of,
                expected_products=[product], week_count=4, metric=self._metric,
            )
            for product in products
        }

    @staticmethod
    def _owned_completed_weeks(month: str, as_of: pd.Timestamp) -> list[str]:
        first = pd.Timestamp(month + "-01")
        sunday = first + pd.Timedelta(days=(6 - first.weekday()) % 7)
        current_monday = as_of - pd.Timedelta(days=as_of.weekday())
        weeks = []
        while sunday.month == first.month and sunday < current_monday:
            iso = sunday.isocalendar()
            weeks.append(f"{iso.year}-W{iso.week:02d}")
            sunday += pd.Timedelta(weeks=1)
        return weeks

    def _weekly_rows(
        self, product: str, weeks: list[str], ledger: pd.DataFrame | None, as_of: pd.Timestamp,
    ) -> pd.DataFrame:
        totals = self._weekly_source.read_weekly_totals(product, weeks, metric=self._metric, as_of=as_of)
        required = {"prod_code", "period_label", "total_projects"}
        if not isinstance(totals, pd.DataFrame) or not required.issubset(totals.columns):
            raise ValueError("Weekly capability totals unavailable")
        if not totals["prod_code"].eq(product).all() or totals["period_label"].duplicated().any():
            raise ValueError("Weekly capability totals contain invalid project identities")
        totals = totals.loc[totals["period_label"].isin(weeks)].copy()
        if set(totals["period_label"]) != set(weeks):
            raise ValueError(f"Incomplete weekly capability totals for {product}")
        warnings = pd.Series(dtype="int64")
        if ledger is not None and not ledger.empty:
            if missing := {*CPK_KEY_COLUMNS, "flag"} - set(ledger.columns):
                raise ValueError(f"Capability alert history missing columns: {sorted(missing)}")
            rows = ledger.loc[ledger["prod_code"].eq(product) & ledger["period_type"].eq("week")
                              & ledger["period_label"].isin(weeks) & released_capability_mask(ledger, self._metric)]
            warnings = rows.drop_duplicates(CPK_KEY_COLUMNS).groupby("period_label").size()
        total_column, rate_column = capability_summary_columns(self._metric)[6:8]
        rows = []
        for item in totals.itertuples(index=False):
            total = item.total_projects
            warning = int(warnings.get(item.period_label, 0))
            if pd.isna(total) or not isinstance(total, Real) or isinstance(total, bool) or not float(total).is_integer() or total < 0:
                raise ValueError(f"Invalid weekly project total for {product}/{item.period_label}")
            total = int(total)
            rows.append({"产品": product, "监控类型": "SPC", "厂别": "ALL", "周期类型": "周度",
                         "时间标签": item.period_label, total_column: total, "预警项目数": warning,
                         "达标项目数": total - warning, rate_column: (total - warning) / total if total else 1.0})
        return normalize_capability_history(pd.DataFrame(rows), metric=self._metric)

    def _build_trend_dashboard(
        self, products: tuple[str, ...], factories: tuple[str, ...], as_of: pd.Timestamp,
    ) -> CpkMonitorViewModel:
        """Refresh closed weekly counts from local report inputs and saved flags.

        Alert history already owns qualification. Do not reapply corrected-value,
        span or threshold decisions while counting its explicit false flags.
        Missing weekly contribution history requests the complete owning month
        before rebuilding it, preventing partial-month totals from wiping history.
        """
        current = self._store.read()
        previous = as_of - pd.Timedelta(days=as_of.weekday() + 1)
        iso = previous.isocalendar()
        label = f"{iso.year}-W{iso.week:02d}"
        month = week_owner_month(label)
        updates, complete_months, statuses, details = [], [], [], []
        if products and self._latest_reader is None:
            raise ValueError("Capability alert workbook reader unavailable")
        for product in products:
            baseline = current.loc[current["产品"].eq(product) & current["监控类型"].eq("SPC")
                                   & current["厂别"].eq("ALL")]
            has_week = (baseline["周期类型"].eq("周度") & baseline["时间标签"].eq(label)).any()
            has_month = (baseline["周期类型"].eq("月度") & baseline["时间标签"].eq(month)).any()
            weeks = [label] if has_week and has_month else self._owned_completed_weeks(month, as_of)
            if not (has_week and has_month):
                complete_months.append((product, "SPC", "ALL", month))
            elif set(self._owned_completed_weeks(month, as_of)).issubset(
                    set(baseline.loc[baseline["周期类型"].eq("周度"), "时间标签"])):
                complete_months.append((product, "SPC", "ALL", month))
            ledger = self._latest_reader.read_product(product)
            updates.append(self._weekly_rows(product, weeks, ledger, as_of))
            if ledger is not None and not ledger.empty:
                details.append(ledger)
            statuses.append({"product": product, "period": label, "source": "snapshot", "message": ""})
        if updates:
            records = self._store.refresh_weeks(pd.concat(updates, ignore_index=True),
                                               complete_months=complete_months, as_of=as_of)
        else:
            records = current
        selected = records.loc[records["产品"].isin(products) & records["监控类型"].eq("SPC")
                               & records["厂别"].eq("ALL")]
        detail = pd.concat(details, ignore_index=True) if details else pd.DataFrame(columns=CPK_DETAIL_COLUMNS)
        active_detail = exclude_inline_period_records(detail, ConfigLoader.get_inline_data_exclusion(), as_of=as_of)
        selected = public_capability_history(selected, active_detail, self._metric)
        if not detail.empty:
            detail = detail.loc[detail["period_type"].eq("week") & detail["period_label"].eq(label)
                                & detail["factory"].isin(factories)
                                & ~detail["flag"].astype(str).str.strip().str.lower().eq("delete")].copy()
        return CpkMonitorViewModel(
            build_cpk_summary_from_records(selected, end_date=as_of, expected_products=products,
                                           week_count=4, metric=self._metric),
            detail, refresh_status_df=pd.DataFrame(statuses),
            product_summary_dfs=self._product_summaries(selected, products, as_of),
        )
