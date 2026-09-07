"""Read-only OOS fact aggregation for the automatic-warning dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

import pandas as pd

from src.inline_domain.application.shared.oos_history_service import OosHistoryService


class OosProductReader(Protocol):
    def read_product(self, scope: str, prod_code: str): ...


@dataclass(frozen=True)
class OosMonitorViewModel:
    detail_df: pd.DataFrame
    summary_df: pd.DataFrame
    trend_df: pd.DataFrame
    station_df: pd.DataFrame
    refresh_status_df: pd.DataFrame


class OosMonitorService:
    """Aggregate already-computed OOS facts without touching measurements/specs."""

    def __init__(self, reader: OosProductReader) -> None:
        self._reader = reader

    def build_dashboard(
        self,
        *,
        products: Iterable[str],
        scopes: Iterable[str],
        factories: Iterable[str],
        start_date: str,
        end_date: str,
    ) -> OosMonitorViewModel:
        selected_products = tuple(dict.fromkeys(str(value) for value in products))
        selected_scopes = tuple(dict.fromkeys(str(value).lower() for value in scopes))
        selected_factories = {str(value).upper() for value in factories}
        start, end = OosHistoryService.inclusive_date_window(start_date, end_date)

        frames: list[pd.DataFrame] = []
        statuses: list[dict[str, object]] = []
        for prod_code in selected_products:
            for scope in selected_scopes:
                result = self._reader.read_product(scope, prod_code)
                statuses.append(
                    {
                        "prod_code": prod_code,
                        "scope": scope,
                        "source": result.source,
                        "refreshed_at": result.refreshed_at,
                    }
                )
                if not result.alerts_df.empty:
                    frames.append(result.alerts_df)

        detail = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not detail.empty:
            times = pd.to_datetime(detail["event_time"], errors="coerce")
            mask = (times >= start) & (times < end)
            if selected_factories:
                mask &= detail["factory"].fillna("").astype(str).str.upper().isin(
                    selected_factories
                )
            detail = detail.loc[mask].copy()
            detail["event_time"] = times.loc[mask]
            detail = detail.sort_values("event_time", ascending=False).reset_index(drop=True)

        if detail.empty:
            summary = pd.DataFrame(columns=["prod_code", "scope", "oos_count"])
            trend = pd.DataFrame(columns=["event_date", "scope", "oos_count"])
            station = pd.DataFrame(columns=["step_id", "scope", "oos_count"])
        else:
            summary = (
                detail.groupby(["prod_code", "scope"], dropna=False)
                .size()
                .rename("oos_count")
                .reset_index()
            )
            trend = (
                detail.assign(event_date=detail["event_time"].dt.date)
                .groupby(["event_date", "scope"], dropna=False)
                .size()
                .rename("oos_count")
                .reset_index()
            )
            station = (
                detail.groupby(["step_id", "scope"], dropna=False)
                .size()
                .rename("oos_count")
                .reset_index()
                .sort_values("oos_count", ascending=False)
                .head(10)
                .reset_index(drop=True)
            )

        status = pd.DataFrame(
            statuses, columns=["prod_code", "scope", "source", "refreshed_at"]
        )
        return OosMonitorViewModel(detail, summary, trend, station, status)
