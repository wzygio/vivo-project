"""Unit tests for shared sheet-OOS alert filtering (previous ISO week, flag=FALSE)."""

from __future__ import annotations

from datetime import date

import pandas as pd

from inline_domain.core.shared.sheet_oos_alerts import (
    build_sheet_oos_alerts,
    previous_iso_week_range,
)


class TestPreviousIsoWeekRange:
    def test_returns_half_open_previous_monday_to_this_monday(self):
        start, end = previous_iso_week_range(date(2026, 8, 25))  # Tuesday
        assert start == pd.Timestamp("2026-08-17")  # last Monday
        assert end == pd.Timestamp("2026-08-24")  # this Monday, exclusive

    def test_reference_on_monday_targets_immediately_previous_week(self):
        start, end = previous_iso_week_range(date(2026, 8, 24))  # Monday
        assert start == pd.Timestamp("2026-08-17")
        assert end == pd.Timestamp("2026-08-24")

    def test_reference_on_sunday_still_targets_previous_week(self):
        start, end = previous_iso_week_range(date(2026, 8, 30))  # Sunday
        assert start == pd.Timestamp("2026-08-17")
        assert end == pd.Timestamp("2026-08-24")

    def test_cross_year_boundary(self):
        start, end = previous_iso_week_range(date(2026, 1, 1))  # Thursday
        assert start == pd.Timestamp("2025-12-22")
        assert end == pd.Timestamp("2025-12-29")

    def test_result_is_normalized_midnight(self):
        start, end = previous_iso_week_range(pd.Timestamp("2026-08-25 15:40:29"))
        assert start == start.normalize()
        assert end == end.normalize()


def _detail_row(sheet_id, flag, when, **extra):
    row = {
        "factory": "F1",
        "prod_code": "M678",
        "step_id": "S100",
        "param_name": "PPA_B_X",
        "sheet_id": sheet_id,
        "sheet_start_time": when,
        "flag": flag,
    }
    row.update(extra)
    return row


# 参考日 2026-08-25（周二）→ 上一 ISO 周为 [2026-08-17, 2026-08-24)
REF = date(2026, 8, 25)


class TestBuildSheetOosAlerts:
    def test_keeps_flag_false_rows_within_previous_week(self):
        df = pd.DataFrame([
            _detail_row("S1", False, "2026-08-20 10:00:00"),
            _detail_row("S2", True, "2026-08-20 11:00:00"),
        ])
        result = build_sheet_oos_alerts(df, time_column="sheet_start_time", reference_date=REF)
        assert list(result["sheet_id"]) == ["S1"]

    def test_excludes_delete_true_and_empty_flags(self):
        df = pd.DataFrame([
            _detail_row("S1", "Delete", "2026-08-20 10:00:00"),
            _detail_row("S2", True, "2026-08-20 10:00:00"),
            _detail_row("S3", None, "2026-08-20 10:00:00"),
            _detail_row("S4", "FALSE", "2026-08-20 10:00:00"),
        ])
        result = build_sheet_oos_alerts(df, time_column="sheet_start_time", reference_date=REF)
        assert list(result["sheet_id"]) == ["S4"]

    def test_excludes_outside_window_and_end_boundary(self):
        df = pd.DataFrame([
            _detail_row("THIS_WEEK", False, "2026-08-24 00:00:00"),   # end 边界，排除
            _detail_row("TWO_WEEKS", False, "2026-08-16 23:59:59"),   # start 之前，排除
            _detail_row("START_EDGE", False, "2026-08-17 00:00:00"),  # start 边界，包含
            _detail_row("END_EDGE", False, "2026-08-23 23:59:59"),    # 窗口内，包含
        ])
        result = build_sheet_oos_alerts(df, time_column="sheet_start_time", reference_date=REF)
        assert set(result["sheet_id"]) == {"START_EDGE", "END_EDGE"}

    def test_unparseable_or_missing_time_excluded(self):
        df = pd.DataFrame([
            _detail_row("BAD", False, "not-a-time"),
            _detail_row("NAT", False, None),
            _detail_row("OK", False, "2026-08-19 08:00:00"),
        ])
        result = build_sheet_oos_alerts(df, time_column="sheet_start_time", reference_date=REF)
        assert list(result["sheet_id"]) == ["OK"]

    def test_empty_and_missing_columns_return_empty(self):
        empty = pd.DataFrame(columns=["flag", "sheet_start_time"])
        assert build_sheet_oos_alerts(empty, time_column="sheet_start_time", reference_date=REF).empty
        no_flag = pd.DataFrame([{"sheet_id": "S1", "sheet_start_time": "2026-08-20"}])
        assert build_sheet_oos_alerts(no_flag, time_column="sheet_start_time", reference_date=REF).empty
        assert build_sheet_oos_alerts(pd.DataFrame(), time_column="sheet_start_time", reference_date=REF).empty

    def test_result_sorted_by_time_descending(self):
        df = pd.DataFrame([
            _detail_row("OLD", False, "2026-08-18 09:00:00"),
            _detail_row("NEW", False, "2026-08-21 09:00:00"),
            _detail_row("MID", False, "2026-08-20 09:00:00"),
        ])
        result = build_sheet_oos_alerts(df, time_column="sheet_start_time", reference_date=REF)
        assert list(result["sheet_id"]) == ["NEW", "MID", "OLD"]

    def test_supports_alternative_time_column_name(self):
        # aoi_tt 明细的时间列名为 start_time
        df = pd.DataFrame([
            _detail_row("T1", False, "2026-08-20 10:00:00"),
        ]).rename(columns={"sheet_start_time": "start_time"})
        result = build_sheet_oos_alerts(df, time_column="start_time", reference_date=REF)
        assert list(result["sheet_id"]) == ["T1"]
