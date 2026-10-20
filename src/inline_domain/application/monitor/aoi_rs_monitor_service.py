"""AOI_RS period-history orchestration; all persistence is behind one store port."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

import pandas as pd

from src.inline_domain.core.monitor.aoi_rs_periods import build_monitor_period_axis
from src.inline_domain.core.aoi_rs.aoi_rs_special_decoration import build_decorated_period_trend_df

HISTORY_COLUMNS = [
    "prod_code", "factory", "step_id", "rs_code", "code_desc", "period_type",
    "period_label", "period_start", "period_end", "rs_qty", "sheet_qty", "value",
    "record_kind", "policy_signature", "covered_through", "refreshed_at", "dependency_month",
]
PUBLIC_KEYS = {"prod_code": "产品", "factory": "厂别", "step_id": "站点",
               "rs_code": "RSCode", "code_desc": "缺陷名称"}


class AoiRsMonitorSource(Protocol):
    def policy_signature(self, product: str) -> str: ...
    def history_signature(self, product: str) -> str: ...
    def read_projected(self, product: str, factory: str, start: str, end: str) -> dict: ...


class AoiRsHistoryStore(Protocol):
    def read(self) -> pd.DataFrame: ...
    def signature(self) -> str: ...
    def update(self, updater: Callable[[pd.DataFrame], pd.DataFrame]) -> pd.DataFrame: ...


@dataclass(frozen=True)
class AoiRsMonitorQuery:
    products: tuple[str, ...] = ()
    factory: str = "ARRAY"
    halfyears: tuple[int, ...] = ()
    quarters: tuple[int, ...] = ()
    months: tuple[int, ...] = ()
    week_count: int = 5
    steps: tuple[str, ...] = ()
    codes: tuple[str, ...] = ()


def _month_axis(axis: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    """Weeks need their endpoint month's full available density for the existing cap."""
    extra = []
    labels = set(axis["period_label"])
    for row in axis.loc[axis["period_type"].eq("week")].itertuples():
        start = pd.Timestamp(row.period_end).replace(day=1)
        label = f"{start.year}-M{start.month:02d}"
        if label not in labels:
            extra.append(dict(period_type="month", period_label=label, period_sort=9000 + len(extra),
                              period_start=start, period_end=min(start + pd.offsets.MonthEnd(), as_of)))
            labels.add(label)
    return pd.concat([axis, pd.DataFrame(extra)], ignore_index=True) if extra else axis.copy()


def _target_end(period_end: object, as_of: pd.Timestamp, cutoff: str, dependency: str = "") -> pd.Timestamp:
    end = pd.Timestamp(period_end).normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)
    if dependency:
        end = max(end, pd.Period(dependency, freq="M").end_time.floor("s"))
    return min(end, pd.Timestamp(f"{as_of.date()} {cutoff}"))


class AoiRsMonitorService:
    def __init__(self, source: AoiRsMonitorSource, store: AoiRsHistoryStore, *,
                 enabled_products: Sequence[str], ttl_seconds: int = 43200,
                 cutoff: str = "12:00:00") -> None:
        self.source, self.store = source, store
        self.enabled_products = tuple(enabled_products)
        self.ttl_seconds, self.cutoff = ttl_seconds, cutoff

    def presentation_signature(self, products: Sequence[str]) -> tuple:
        return (self.store.signature(), self.cutoff,
                tuple((product, self.source.policy_signature(product)) for product in products))

    def read_catalog(self, products: Sequence[str], factory: str) -> pd.DataFrame:
        history = self.store.read()
        return history.loc[
            history["record_kind"].eq("density") & history["prod_code"].isin(products)
            & history["factory"].eq(factory), ["step_id", "rs_code", "code_desc"],
        ].drop_duplicates().reset_index(drop=True)

    def build_dashboard(self, query: AoiRsMonitorQuery, *, as_of: object | None = None,
                        refresh: bool = False) -> dict:
        day = pd.Timestamp(as_of if as_of is not None else pd.Timestamp.today()).normalize()
        products = tuple(product for product in self.enabled_products if product in (query.products or self.enabled_products))
        if set(query.products) - set(self.enabled_products) or query.factory not in {"ARRAY", "OLED", "TP"}:
            raise ValueError("Invalid AOI_RS monitor scope")
        axis = build_monitor_period_axis(day, halfyears=query.halfyears, quarters=query.quarters,
                                         months=query.months, week_count=query.week_count)
        now = pd.Timestamp.now().floor("s")

        def update(history: pd.DataFrame) -> pd.DataFrame:
            result = history.copy()
            for product in products:
                signature = self.source.history_signature(product)
                scope = result.loc[result["prod_code"].eq(product) & result["factory"].eq(query.factory)]
                needed = []
                for row in axis.itertuples():
                    coverage = scope.loc[scope["record_kind"].eq("coverage") & scope["period_label"].eq(row.period_label)
                                         & scope["policy_signature"].eq(signature)]
                    if coverage.empty:
                        needed.append(row.period_label)
                        continue
                    last = coverage.iloc[-1]
                    dependency = str(last["dependency_month"]) if pd.notna(last["dependency_month"]) else ""
                    target = _target_end(row.period_end, day, self.cutoff, dependency)
                    mutable = target >= pd.Timestamp(f"{day.date()} {self.cutoff}")
                    age = (now - pd.Timestamp(last["refreshed_at"])).total_seconds()
                    if pd.Timestamp(last["covered_through"]) < target or (mutable and (refresh or age >= self.ttl_seconds)):
                        needed.append(row.period_label)
                if not needed:
                    continue
                selected = axis.loc[axis["period_label"].isin(needed)]
                compute_axis = _month_axis(selected, day)
                start, end = compute_axis["period_start"].min(), compute_axis["period_end"].max()
                inputs = self.source.read_projected(product, query.factory, str(start.date()), str(end.date()))
                indicators = inputs["indicators_df"].copy()
                # Both the universe and numerator are scoped before frontend/export projection.
                indicators = indicators.loc[indicators["prod_code"].eq(product) & indicators["factory"].eq(query.factory)]
                density = build_decorated_period_trend_df(
                    inputs["rs_details_df"], inputs["pass_through_df"], day.date(),
                    period_axis=compute_axis, indicators=indicators,
                    special_factories=inputs.get("special_factories", ()),
                    decoration_window=inputs.get("decoration_window"),
                )
                descriptions = indicators[["factory", "step_id", "rs_code", "code_desc"]].drop_duplicates(
                    ["factory", "step_id", "rs_code"])
                density = density.drop(columns=["code_desc"], errors="ignore").merge(
                    descriptions, on=["factory", "step_id", "rs_code"], how="left")
                incoming = []
                special = query.factory in inputs.get("special_factories", ())
                window = inputs.get("decoration_window")
                for row in selected.itertuples():
                    capped_week = special and row.period_type == "week" and (
                        window is None or (
                            pd.Timestamp(window[0]) <= pd.Timestamp(row.period_start)
                            and pd.Timestamp(row.period_end) <= pd.Timestamp(window[1])
                        )
                    )
                    dependency = pd.Timestamp(row.period_end).strftime("%Y-%m") if capped_week else ""
                    metadata = dict(prod_code=product, factory=query.factory, period_start=row.period_start,
                                    period_end=row.period_end, policy_signature=signature,
                                    covered_through=_target_end(row.period_end, day, self.cutoff, dependency).isoformat(),
                                    refreshed_at=now.isoformat(), dependency_month=dependency)
                    records = density.loc[density["period_label"].eq(row.period_label)].drop(columns=["period_sort"])
                    records = records.assign(**metadata, record_kind="density")
                    incoming.append(records)
                    incoming.append(pd.DataFrame([dict(metadata, record_kind="coverage", period_type=row.period_type,
                                                       period_label=row.period_label, step_id="", rs_code="", code_desc="")]))
                replace = result["prod_code"].eq(product) & result["factory"].eq(query.factory) & result["period_label"].isin(needed)
                frames = [frame.astype(object) for frame in [result.loc[~replace], *incoming] if not frame.empty]
                result = pd.concat(frames, ignore_index=True).reindex(columns=HISTORY_COLUMNS)
            return result

        history = self.store.update(update)
        tables = {}
        for product in products:
            frame = history.loc[history["prod_code"].eq(product) & history["factory"].eq(query.factory)
                                & history["record_kind"].eq("density") & history["period_label"].isin(axis["period_label"])].copy()
            if query.steps:
                frame = frame.loc[frame["step_id"].isin(query.steps)]
            if query.codes:
                frame = frame.loc[frame["rs_code"].isin(query.codes)]
            keys = list(PUBLIC_KEYS)
            frame["code_desc"] = frame["code_desc"].fillna("")
            if frame.empty:
                table = pd.DataFrame(columns=[*PUBLIC_KEYS.values(), *axis["period_label"]])
            else:
                table = frame.set_index([*keys, "period_label"])["value"].unstack("period_label")
                table = table.reindex(columns=axis["period_label"].tolist()).reset_index().rename(columns=PUBLIC_KEYS)
                table = table.sort_values(["站点", "RSCode"], kind="stable").reset_index(drop=True)
                table.columns.name = None
            tables[product] = table
        return {"tables": tables}
