from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from src.yield_domain.application.alert_results import RESULT_COLUMNS, YieldAlertResults
from src.yield_domain.application.weekly_report import ReportWeek, load_weekly_conclusions


PUBLISHED_AT = datetime(2026, 1, 5, tzinfo=timezone.utc)


class AlertStore:
    def __init__(self, trend=(), lot=(), *, decisions=None, covered=True):
        self.records = {"trend": trend, "lot": lot}
        self.decisions = decisions
        self.covered = covered

    def source_signature(self, kind):
        return kind

    def is_product_absent(self, product, kind):
        return product == "Z622"

    def read_product(self, product, kind):
        if product == "Z622":
            return None
        frame = pd.DataFrame(self.records[kind], columns=[*RESULT_COLUMNS[kind], "flag"])
        return {
            "records": frame,
            "metadata": {
                "schema_version": 1, "kind": kind, "product": product, "status": "ready",
                "source_status": "fresh", "row_count": len(frame),
                "reference_date": "2026-01-05", "has_data": True, "fingerprint": "fixture",
                "generated_at": PUBLISHED_AT.isoformat(), "source_refreshed_at": PUBLISHED_AT.isoformat(),
                "policy_signature": "fixture",
                "coverage_start": "2025-12-01",
                "coverage_end": "2026-01-05" if self.covered else "2025-12-31",
            },
            "decisions": self.decisions if kind == "trend" else None,
        }


def _reader(store, hours_later=1):
    return YieldAlertResults(store, policy_signature=lambda _: "fixture", ttl_seconds=43200,
                             clock=lambda: PUBLISHED_AT + timedelta(hours=hours_later))


def test_week_uses_iso_year_and_ends_before_current_monday():
    assert ReportWeek.previous(date(2021, 1, 4)) == ReportWeek(date(2020, 12, 28))
    week = ReportWeek.previous(date(2026, 1, 5))
    assert (week.label, week.start, week.end) == (
        "2026-W01", date(2025, 12, 29), date(2026, 1, 4),
    )


def test_weekly_conclusions_use_target_period_and_explicit_flags():
    def trend(code, flag, period="2026-W01", scope="weekly"):
        return ("code", "GR", code, period, 1, 0, '["规则1", "规则2"]', scope, flag)

    def lot(identifier, day, flag):
        return (identifier, "LC", 0, 1, 1, day, "", flag)

    store = AlertStore(
        trend=[
            trend("开放", False), trend("开放", False), trend("屏蔽", True),
            trend("删除", "Delete"), trend("未知", None),
            trend("跨周", False, "2026-W02"), trend("月度", False, "2025-12", "monthly"),
            ("group", "缺陷组", None, "2026-W01", 1, 0, "组级规则", "weekly", False),
        ],
        lot=[
            lot("前一周", "2025/12/28", False), lot("首日", "2025/12/29", "FALSE"),
            lot("末日", "2026/01/04", False), lot("本周", "2026/01/05", False),
            lot("不公开", "2026/01/04", "True"),
        ],
    )
    conclusions = load_weekly_conclusions(_reader(store), ("M678", "Z622"), ReportWeek(date(2025, 12, 29)))
    assert conclusions == {
        "M678": "开放-规则1、规则2；缺陷组-组级规则；首日-LC超规；末日-LC超规",
        "Z622": "趋势预警记录暂无；Lot 超规记录暂无",
    }


def test_valid_empty_publications_produce_the_no_anomaly_conclusion():
    week = ReportWeek(date(2025, 12, 29))
    assert load_weekly_conclusions(_reader(AlertStore()), ("M678",), week) == {
        "M678": "无异常波动，无新增异常，无新增超规lot",
    }


def test_weekly_conclusions_use_authoritative_decisions_without_defaulting_missing_keys():
    week = ReportWeek(date(2025, 12, 29))
    decisions = pd.DataFrame(
        [("code", "GR", "A", week.label, "weekly", True),
         ("code", "GR", "B", week.label, "weekly", False)],
        columns=["level", "defect_group", "defect_desc", "time_period", "period_scope", "flag"],
    )
    store = AlertStore(
        trend=[("code", "GR", code, week.label, 1, 0, "规则", "weekly", flag)
               for code, flag in (("A", False), ("B", True), ("C", False))],
        decisions=decisions,
    )
    assert load_weekly_conclusions(_reader(store), ("M678",), week) == {"M678": "B-规则"}


def test_insufficient_coverage_is_not_reported_as_normal():
    with pytest.raises(ValueError, match="coverage"):
        load_weekly_conclusions(_reader(AlertStore(covered=False)), ("M678",), ReportWeek(date(2025, 12, 29)))


def test_stale_publication_is_not_reported_as_normal():
    with pytest.raises(ValueError, match="Unavailable"):
        load_weekly_conclusions(
            _reader(AlertStore(), hours_later=13), ("M678",), ReportWeek(date(2025, 12, 29)),
        )
