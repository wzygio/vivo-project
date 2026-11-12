"""Orchestrate the default query and prepared deliveries through IO ports."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from api.notification.core import NotificationError, NotificationPlan, NotificationSettings, plan_notifications


class AlertSource(Protocol):
    def fetch(self) -> dict: ...


class MailDelivery(Protocol):
    def send(self, plan: NotificationPlan) -> None: ...


@dataclass(frozen=True)
class RunReport:
    record_count: int
    notification_count: int
    sent_count: int
    complete: bool
    failed_source_count: int
    dry_run: bool


class DeliveryFailure(NotificationError):
    """Preserve accepted-delivery counts so retries can be assessed safely."""

    def __init__(self, report: RunReport, message: str):
        self.report = report
        super().__init__(message)


def run_notification(source: AlertSource, delivery: MailDelivery | None,
                     settings: NotificationSettings, *, test_mode: bool = False) -> RunReport:
    result = source.fetch()
    plans = plan_notifications(result, settings, test_mode=test_mode)
    sent = 0
    for plan in plans:
        if delivery is not None:
            try:
                delivery.send(plan)
            except NotificationError as exc:
                report = RunReport(len(result['data']), len(plans), sent, result['meta']['complete'],
                                   len(result['meta']['failed_sources']), False)
                raise DeliveryFailure(report, str(exc)) from None
            sent += 1
    return RunReport(len(result['data']), len(plans), sent, result['meta']['complete'],
                     len(result['meta']['failed_sources']), delivery is None)
