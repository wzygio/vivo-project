"""Read-only OOS fact aggregation for the automatic-warning dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

import pandas as pd

from src.inline_domain.application.shared.oos_history_service import OosHistoryService
from src.inline_domain.core.monitor.period_summary import build_period_summary


class OosProductReader(Protocol):
    def read_product(self, scope: str, prod_code: str): ...


class ThroughputProductReader(Protocol):
    def read_product(self, scope: str, prod_code: str) -> pd.DataFrame: ...


class MonitorSummaryWriter(Protocol):
    def refresh_summary(
        self,
        *,
        products: Iterable[str],
        scopes: Iterable[str],
        factories: Iterable[str],
        alerts_df: pd.DataFrame,
        throughput_df: pd.DataFrame,
        end_date: pd.Timestamp,
    ) -> pd.DataFrame: ...


@dataclass(frozen=True)
class OosMonitorViewModel:
    detail_df: pd.DataFrame
    summary_df: pd.DataFrame
    trend_df: pd.DataFrame
    station_df: pd.DataFrame
    refresh_status_df: pd.DataFrame
    period_summary_df: pd.DataFrame


class OosMonitorService:
    """Aggregate already-computed OOS facts without touching measurements/specs."""

    def __init__(
        self,
        reader: OosProductReader,
        *,
        ooc_reader: OosProductReader | None = None,
        throughput_reader: ThroughputProductReader | None = None,
        summary_workbook: MonitorSummaryWriter | None = None,
    ) -> None:
        self._reader = reader
        self._readers = (("OOS", reader),) if ooc_reader is None else (
            ("OOS", reader),
            ("OOC", ooc_reader),
        )
        self._throughput_reader = throughput_reader
        self._summary_workbook = summary_workbook

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
        selected_factories_tuple = tuple(
            dict.fromkeys(str(value).upper() for value in factories)
        )
        selected_factories = set(selected_factories_tuple)
        start, end = OosHistoryService.inclusive_date_window(start_date, end_date)

        frames: list[pd.DataFrame] = []
        throughput_frames: list[pd.DataFrame] = []
        statuses: list[dict[str, object]] = []
        for prod_code in selected_products:
            for scope in selected_scopes:
                if self._throughput_reader is not None:
                    throughput = self._throughput_reader.read_product(scope, prod_code)
                    if not throughput.empty:
                        throughput_frames.append(throughput)
                for alarm_type, reader in self._readers:
                    result = reader.read_product(scope, prod_code)
                    statuses.append(
                        {
                            "prod_code": prod_code,
                            "scope": scope,
                            "alarm_type": alarm_type,
                            "source": result.source,
                            "refreshed_at": result.refreshed_at,
                        }
                    )
                    if not result.alerts_df.empty:
                        alerts = result.alerts_df.copy()
                        alerts["alarm_type"] = alarm_type
                        frames.append(alerts)

        detail = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        throughput = (
            pd.concat(throughput_frames, ignore_index=True)
            if throughput_frames
            else pd.DataFrame()
        )
        if not throughput.empty:
            throughput_dates = pd.to_datetime(throughput["event_date"], errors="coerce")
            throughput_mask = (throughput_dates >= start) & (throughput_dates < end)
            if selected_factories:
                throughput_mask &= throughput["factory"].fillna("").astype(str).str.upper().isin(
                    selected_factories
                )
            throughput = throughput.loc[throughput_mask].copy()
            throughput["event_date"] = throughput_dates.loc[throughput_mask]
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
            summary = pd.DataFrame(columns=["prod_code", "scope", "alarm_type", "oos_count"])
            trend = pd.DataFrame(columns=["event_date", "scope", "alarm_type", "oos_count"])
            station = pd.DataFrame(columns=["step_id", "scope", "alarm_type", "oos_count"])
        else:
            summary = (
                detail.groupby(["prod_code", "scope", "alarm_type"], dropna=False)
                .size()
                .rename("oos_count")
                .reset_index()
            )
            trend = (
                detail.assign(event_date=detail["event_time"].dt.date)
                .groupby(["event_date", "scope", "alarm_type"], dropna=False)
                .size()
                .rename("oos_count")
                .reset_index()
            )
            station = (
                detail.groupby(["step_id", "scope", "alarm_type"], dropna=False)
                .size()
                .rename("oos_count")
                .reset_index()
                .sort_values("oos_count", ascending=False)
                .head(10)
                .reset_index(drop=True)
            )

        status = pd.DataFrame(
            statuses, columns=["prod_code", "scope", "alarm_type", "source", "refreshed_at"]
        )
        period_summary = (
            self._summary_workbook.refresh_summary(
                products=selected_products,
                scopes=selected_scopes,
                factories=selected_factories_tuple,
                alerts_df=detail,
                throughput_df=throughput,
                end_date=pd.Timestamp(end_date),
            )
            if self._summary_workbook is not None
            else build_period_summary(
                detail,
                throughput,
                end_date=pd.Timestamp(end_date),
            )
        )
        return OosMonitorViewModel(detail, summary, trend, station, status, period_summary)
