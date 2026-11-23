"""Historical repairs select closed weeks and reuse exact ledger release flags."""

import pandas as pd
import pytest

from src.inline_domain.application.monitor.capability_history_rebuild import (
    closed_week_labels,
    compute_weekly_records,
)


def test_latest_five_weeks_are_complete_and_exclude_the_open_week():
    assert closed_week_labels("2026-11-17", count=5) == [
        "2026-W42", "2026-W43", "2026-W44", "2026-W45", "2026-W46",
    ]
    assert closed_week_labels("2026-11-15", count=1) == ["2026-W45"]


def test_month_window_includes_cross_month_week_but_no_open_week():
    labels = closed_week_labels("2026-11-17", from_month="2026-09-01")
    assert labels[0] == "2026-W36" and labels[-1] == "2026-W46"
    assert len(labels) == 11
    assert closed_week_labels("2026-11-17", from_month="2026-11-01")[0] == "2026-W44"


def test_complete_week_selection_handles_iso_year_boundary():
    assert closed_week_labels("2027-01-04", count=2) == ["2026-W52", "2026-W53"]


class Source:
    def __init__(self, total=10, *, incomplete=False):
        self.total = total
        self.incomplete = incomplete
        self.calls = []

    def read_weekly_totals(self, product, weeks, *, metric, as_of):
        self.calls.append((product, list(weeks), metric, as_of))
        selected = weeks[1:] if self.incomplete else weeks
        return pd.DataFrame([{"prod_code": product, "period_label": label, "total_projects": self.total}
                             for label in selected], columns=["prod_code", "period_label", "total_projects"])


class Ledger:
    def __init__(self, frame):
        self.frame = frame

    def read_product(self, product):
        return self.frame


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_rebuild_counts_released_projects_without_threshold_or_span_reinterpretation(metric):
    frame = pd.DataFrame([
        {"prod_code": "M626", "factory": "ARRAY", "step_id": "1", "param_name": name,
         "period_type": "week", "period_label": "2026-W46", "flag": flag,
         f"{metric}_corrected": 999, "period_start": "2026-11-15", "period_end": "2026-11-15"}
        for name, flag in [("released", False), ("released", False), ("hidden", True)]
    ])
    other_product = frame.iloc[[0]].assign(prod_code="M678", param_name="another-product")
    frame = pd.concat([frame, other_product], ignore_index=True)
    source = Source()
    result = compute_weekly_records(source, Ledger(frame), products=["M626"],
                                    weeks=["2026-W46", "2026-W46"], metric=metric, as_of="2026-11-17")
    assert source.calls == [("M626", ["2026-W46"], metric, "2026-11-17")]
    assert result[f"{metric.upper()}总项目数"].tolist() == [10]
    assert result["预警项目数"].tolist() == [1]
    assert result["达标项目数"].tolist() == [9]
    assert result[f"{metric.capitalize()}≥1.33达标率"].tolist() == [.9]


def test_missing_product_sheet_in_readable_ledger_has_no_warnings():
    result = compute_weekly_records(Source(), Ledger(None), products=["M626"], weeks=["2026-W46"],
                                    metric="cpk", as_of="2026-11-17")
    assert result[["达标项目数", "预警项目数", "Cpk≥1.33达标率"]].iloc[0].tolist() == [10, 0, 1.0]


def test_zero_monitored_projects_have_full_compliance_without_a_ledger_sheet():
    result = compute_weekly_records(Source(0), Ledger(None), products=["Z622"], weeks=["2026-W46"],
                                    metric="cpk", as_of="2026-11-17")
    assert result["Cpk≥1.33达标率"].tolist() == [1.0]


def test_missing_total_weeks_fail_instead_of_publishing_partial_repairs():
    with pytest.raises(ValueError, match="Incomplete weekly"):
        compute_weekly_records(Source(incomplete=True), Ledger(pd.DataFrame()), products=["M626"],
                               weeks=["2026-W45", "2026-W46"], metric="cpk", as_of="2026-11-17")
