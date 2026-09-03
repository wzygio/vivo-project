"""Q-Time report use cases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import TypedDict

import pandas as pd

from src.indicator_domain.application.qtime.decoration_service import (
    QTimeDecisionUploadResult,
    parse_qtime_decision_upload,
)
from src.indicator_domain.application.qtime.dtos import (
    QTimeQuery,
    QTimeStepOption,
    Shop,
)
from src.indicator_domain.application.qtime.ports import (
    QTimeDataPort,
    QTimeDecorationPort,
)
from src.indicator_domain.core.qtime.alerts import build_qtime_alerts
from src.indicator_domain.core.qtime.decoration import apply_qtime_decoration


class QTimeFilterOptions(TypedDict):
    step_options: tuple[QTimeStepOption, ...]


@dataclass(frozen=True)
class QTimeMonitoringResult:
    details: pd.DataFrame
    alerts: pd.DataFrame
    decoration: pd.DataFrame
    decisions: pd.DataFrame
    decoration_path: Path | None


class QTimeReportService:
    def __init__(
        self,
        data_port: QTimeDataPort,
        decoration_port: QTimeDecorationPort | None = None,
    ) -> None:
        self._data_port = data_port
        self._decoration_port = decoration_port

    @property
    def decoration_path(self) -> Path | None:
        """决策台账工作簿路径（只读）；未配置 decoration_port 时返回 None。"""
        if self._decoration_port is None:
            return None
        return self._decoration_port.decoration_path

    def get_filter_options(self, shop: Shop) -> QTimeFilterOptions:
        return {
            "step_options": self._data_port.list_step_options(shop),
        }

    def get_report(self, query: QTimeQuery) -> pd.DataFrame:
        return self._data_port.fetch_details(query)

    def get_current_report(
        self,
        *,
        shop: Shop,
        step_descriptions: tuple[str, ...],
        products: tuple[str, ...] = (),
        as_of: date | None = None,
    ) -> pd.DataFrame:
        """Return data from the previous month's first day through `as_of`."""
        report_date = as_of or date.today()
        current_month_start = report_date.replace(day=1)
        previous_month_start = (current_month_start - timedelta(days=1)).replace(day=1)
        query = QTimeQuery(
            start_time=datetime.combine(previous_month_start, time.min),
            end_time=datetime.combine(report_date + timedelta(days=1), time.min),
            shop=shop,
            step_descriptions=step_descriptions,
            products=products,
        )
        return self.get_report(query)

    def get_current_monitoring(
        self,
        *,
        shop: Shop,
        step_descriptions: tuple[str, ...],
        products: tuple[str, ...] = (),
        as_of: date | None = None,
    ) -> QTimeMonitoringResult:
        raw_details = self.get_current_report(
            shop=shop,
            step_descriptions=step_descriptions,
            products=products,
            as_of=as_of,
        )
        decisions = (
            self._decoration_port.load_decisions()
            if self._decoration_port is not None
            else pd.DataFrame()
        )
        decorated = apply_qtime_decoration(raw_details, decisions)
        return QTimeMonitoringResult(
            details=decorated.details,
            alerts=build_qtime_alerts(decorated.decoration),
            decoration=decorated.decoration,
            decisions=decisions,
            decoration_path=(
                self._decoration_port.decoration_path
                if self._decoration_port is not None
                else None
            ),
        )

    def update_decisions(self, file_bytes: bytes) -> QTimeDecisionUploadResult:
        if self._decoration_port is None:
            return QTimeDecisionUploadResult("error", "Q-Time 修饰存储未配置。")
        outcome = parse_qtime_decision_upload(file_bytes)
        if outcome.status == "error" or outcome.decisions is None:
            return outcome
        self._decoration_port.save_decisions(outcome.decisions)
        return QTimeDecisionUploadResult(
            "success",
            "Q-Time 修饰决策已更新。",
            outcome.decisions,
        )
