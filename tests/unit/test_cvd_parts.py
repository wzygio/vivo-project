from datetime import datetime

import pandas as pd
import pytest

from src.equipment_domain.config import get_equipment_runtime_config
from src.equipment_domain.core.cvd_parts import build_cvd_report


def cvd_source() -> pd.DataFrame:
    return pd.DataFrame([
        {"厂别": "OLED", "备件类型": "Diffuser", "设备类型": "CVD",
         "膜层": "SIN", "制程": "SIN DEPO", "寿命规格": "2年",
         "站点": 21200, "机台号": "3CEE01-CV1-CHA",
         "测量值": datetime(2024, 9, 21), "进度": None, "测量时间": None},
        {"厂别": "OLED", "备件类型": "Mask", "设备类型": "CVD",
         "膜层": "SIN", "制程": "SIN DEPO", "寿命规格": 6500,
         "站点": 21200, "机台号": "MASK001",
         "测量值": 2477, "进度": 352, "测量时间": None},
    ])


def report(source=None, day="2026-09-22"):
    return build_cvd_report(
        cvd_source() if source is None else source,
        as_of_date=day, baseline_date="2026-09-21",
        policy=get_equipment_runtime_config().alert_policy,
    )


def test_dates_preserved_and_monthly_increment_is_linear_and_repeatable():
    source = cvd_source()
    original = source.copy(deep=True)
    result = report(source)
    assert result.loc[0, "测量值"] == "2026-09-21"
    assert result.loc[0, "寿命规格"] == "2年"
    assert result.loc[0, "预警状态"] == "正常"
    assert result.loc[1, "测量值"] == pytest.approx(2477 + 352 / 30)
    assert report(day="2026-10-21").loc[1, "测量值"] == 2829
    assert report(day="2026-09-20").loc[1, "测量值"] == 2477
    assert "进度" not in result.columns
    assert result["站点"].tolist() == ["21200", "21200"]
    pd.testing.assert_frame_equal(result, report(source))
    pd.testing.assert_frame_equal(source, original)


def test_excel_serial_replacement_date_and_calendar_leap_year():
    source = cvd_source().iloc[:1].copy()
    source.loc[0, "测量值"] = 45351  # 2024-02-29
    result = report(source, day="2025-02-28")
    assert result.loc[0, "测量值"] == "2024-02-29"
    assert result.loc[0, "使用进度"] == pytest.approx(365 / 730 * 100)


@pytest.mark.parametrize("replacement,day,cycle_start,expiry", [
    ("2025-04-12", "2026-10-13", "2026-10-12", "2028-04-12"),
    ("2024-02-29", "2025-08-29", "2025-08-29", "2027-02-28"),
    ("2025-08-31", "2026-02-28", "2025-08-31", "2027-02-28"),
    ("2026-10-12", "2026-10-11", "2026-10-12", "2028-04-12"),
])
def test_fractional_year_lifespan_uses_eighteen_calendar_months(replacement, day, cycle_start, expiry):
    source = cvd_source().iloc[:1].copy()
    source["厂别"] = "ARRAY"
    source["寿命规格"] = "1.5年"
    source["测量值"] = replacement
    source["测量时间"] = "2026-10-12"
    result = report(source, day=day).iloc[0]
    elapsed = max((pd.Timestamp(day) - pd.Timestamp(cycle_start)).days, 0)
    duration = (pd.Timestamp(expiry) - pd.Timestamp(cycle_start)).days
    assert result["测量值"] == cycle_start
    assert result["寿命规格"] == "1.5年"
    assert result["使用进度"] == pytest.approx(elapsed / duration * 100)
    assert result["厂别"] == "ARRAY"


def test_fractional_year_excel_date_serial_and_missing_increment_are_supported():
    source = cvd_source().iloc[:1].copy()
    source["寿命规格"] = "1.5年"
    source["测量值"] = 45351  # 2024-02-29
    source["进度"] = None
    result = report(source, day="2025-08-29").iloc[0]
    assert result["测量值"] == "2025-08-29"
    assert result["使用进度"] == 0
    assert result["预警状态"] == "正常"
    next_day = report(source, day="2025-08-30").iloc[0]
    assert next_day["使用进度"] == pytest.approx(
        100 / (pd.Timestamp("2027-02-28") - pd.Timestamp("2025-08-29")).days,
    )
    assert next_day["预警状态"] == "正常"


def test_fractional_year_without_whole_month_duration_fails_explicitly():
    source = cvd_source().iloc[:1].copy()
    source["寿命规格"] = "1.01年"
    with pytest.raises(ValueError, match="整数月"):
        report(source)


@pytest.mark.parametrize("sheet,day,expected", [
    (" OLED-1012 ", "2026-10-13", "2026-10-12"),
    ("ARRAY-1231", "2027-01-01", "2026-12-31"),
    ("OLED-1013", "2026-10-13", "2026-10-13"),
    ("OLED-0229", "2026-10-13", "2024-02-29"),
])
def test_sheet_update_date_is_most_recent_occurrence(sheet, day, expected):
    from src.equipment_domain.core.cvd_parts import parse_cvd_sheet_name

    factory, updated_at = parse_cvd_sheet_name(sheet, as_of_date=day)
    assert factory == sheet.strip().split("-")[0]
    assert updated_at == pd.Timestamp(expected)


def test_new_version_replacement_date_is_used_without_numeric_growth():
    source = cvd_source().iloc[:1].copy()
    source["测量值"] = datetime(2026, 10, 12)
    source["测量时间"] = "2026-10-12"
    result = report(source, day="2026-10-13")
    assert result.iloc[0]["测量值"] == "2026-10-12"
    assert result.iloc[0]["使用进度"] == pytest.approx(1 / 731 * 100)


def test_row_measurement_date_overrides_default_simulation_origin():
    source = cvd_source().iloc[1:].copy()
    source["测量时间"] = "2026-09-20"
    assert report(source).iloc[0]["测量值"] == pytest.approx(2477 + 2 * 352 / 30)


@pytest.mark.parametrize("value,expected,status", [
    (97, 97, "预警"), (98, 98, "预警"),
    (98.01, 0, "已更换"), (99, 0, "已更换"),
    (100, 0, "正常"), (105, 5, "正常"), (250, 50, "正常"),
])
def test_numeric_cvd_wraps_at_full_lifetime_before_report_replacement(value, expected, status):
    source = cvd_source().iloc[1:].copy().reset_index(drop=True)
    source["寿命规格"] = 100
    source["测量值"] = value
    source["进度"] = 0
    row = report(source, day="2026-09-21").iloc[0]
    if status == "已更换":
        assert row["测量值"] == "2026-09-21"
    else:
        assert row["测量值"] == pytest.approx(expected)
    assert row["使用进度"] == pytest.approx(expected)
    assert row["预警状态"] == status


def test_cvd_after_wrap_keeps_its_monthly_increment_and_does_not_accumulate_on_refresh():
    source = cvd_source().iloc[1:].copy().reset_index(drop=True)
    source["寿命规格"] = 100
    source["测量值"] = 97
    source["进度"] = 30  # 每日 +1，与常规备件约15%的TTL增量独立。
    original = source.copy(deep=True)
    expected = [
        ("2026-09-22", 98, "预警"),
        ("2026-09-23", 0, "已更换"),
        ("2026-09-24", 0, "正常"),
        ("2026-09-25", 1, "正常"),
        ("2027-01-03", 1, "正常"),  # 104天，跨越两个周期。
    ]
    for day, value, status in expected:
        first = report(source, day=day)
        pd.testing.assert_frame_equal(first, report(source, day=day))
        if status == "已更换":
            assert first.loc[0, "测量值"] == day
        else:
            assert first.loc[0, "测量值"] == pytest.approx(value)
        assert first.loc[0, "使用进度"] == pytest.approx(value)
        assert first.loc[0, "预警状态"] == status
    pd.testing.assert_frame_equal(source, original)


@pytest.mark.parametrize("day,replacement,progress,status", [
    ("2025-12-24", "2025-01-01", 357 / 365 * 100, "预警"),
    ("2025-12-25", "2025-12-25", 0, "已更换"),
    ("2026-01-01", "2026-01-01", 0, "正常"),
    ("2026-01-02", "2026-01-01", 1 / 365 * 100, "正常"),
    ("2030-01-02", "2030-01-01", 1 / 365 * 100, "正常"),
])
def test_year_lifetime_replacement_and_multiple_calendar_cycles(day, replacement, progress, status):
    source = cvd_source().iloc[:1].copy()
    source["寿命规格"] = "1年"
    source["测量值"] = "2025-01-01"
    original = source.copy(deep=True)
    row = report(source, day=day).iloc[0]
    assert row["测量值"] == replacement
    assert row["使用进度"] == pytest.approx(progress)
    assert row["预警状态"] == status
    pd.testing.assert_frame_equal(source, original)


def test_calendar_cycle_keeps_source_anniversary_anchor_across_leap_years():
    source = cvd_source().iloc[:1].copy()
    source["寿命规格"] = "1年"
    source["测量值"] = "2024-02-29"
    row = report(source, day="2028-02-29").iloc[0]
    assert row["测量值"] == "2028-02-29"
    assert row["使用进度"] == 0
    assert row["预警状态"] == "正常"


def test_replaced_cvd_year_rows_display_report_replacement_date_instead_of_old_date():
    source = pd.concat([cvd_source().iloc[:1]] * 3, ignore_index=True)
    source["寿命规格"] = "1.5年"
    source["测量值"] = ["2025-04-26", "2025-04-24", "2025-04-22"]
    original = source.copy(deep=True)
    result = report(source, day="2026-10-21")
    assert result["预警状态"].tolist() == ["已更换"] * 3
    assert result["使用进度"].eq(0).all()
    assert result["测量值"].tolist() == ["2026-10-21"] * 3
    pd.testing.assert_frame_equal(source, original)


def test_numeric_cvd_arithmetic_overflow_fails_instead_of_producing_nan():
    source = cvd_source().iloc[1:].copy()
    source["测量值"] = 1e308
    source["进度"] = 1e308
    with pytest.raises(ValueError, match="有限"):
        report(source, day="2030-01-01")


def test_invalid_numeric_progress_fails_instead_of_silent_normal_status():
    source = cvd_source()
    source.loc[1, "进度"] = -1
    with pytest.raises(ValueError, match="进度"):
        report(source)


def test_empty_sheet_has_stable_report_columns():
    result = report(cvd_source().iloc[:0])
    assert result.empty
    assert {"测量值", "使用进度", "预警状态", "机台号-腔室"} <= set(result.columns)


def test_service_merges_cvd_after_existing_numeric_calculations():
    from types import SimpleNamespace
    from src.equipment_domain.application.parts_service import PartsReportService
    from app.sections.equipment_domain.parts_filters import apply_parts_filters

    spec = pd.DataFrame([{
        "厂别": "Array", "备件类型": "Target", "设备类型": "PVD",
        "膜层": "MO", "制程": "DEPO", "寿命规格": 41000,
        "站点": "1K200", "机台号-腔室": "PVD1", "参数名称": "%LIFE%",
    }])
    facts = pd.DataFrame({
        "step_id": ["1K200"], "sub_equip_id": ["PVD1"],
        "param_name": ["LIFE"], "value": [1000.0],
        "glass_start_time": pd.to_datetime(["2026-09-21"]),
    })
    def load_cvd(_, *, as_of_date):
        assert as_of_date == "2026-09-22"
        return cvd_source()

    port = SimpleNamespace(
        load_baseline=lambda _: spec,
        load_snapshots=lambda *args, **kwargs: (facts, pd.DataFrame()),
        load_cvd=load_cvd,
    )
    result = PartsReportService.get_report_data(
        None, "unused.csv", "cvd-test", as_of_date="2026-09-22",
        cvd_path="unused.xlsx", _data_port=port,
    )
    assert result.total_count == 3
    assert result.over_count == 0
    assert result.normal_count == 3
    assert result.last_update == "2026-09-22"
    assert result.report_df.iloc[0]["测量值"] == 1000
    assert "进度" not in result.report_df
    filtered = apply_parts_filters(
        result.report_df, selected_factories=["OLED"],
        selected_equipment_types=["CVD"], selected_part_types=["Mask"],
    )
    assert len(filtered) == 1
    assert filtered.iloc[0]["测量值"] == pytest.approx(2477 + 352 / 30)


def test_cvd_workbook_changes_invalidate_report_cache_context(tmp_path):
    from src.equipment_domain.application.parts_service import build_parts_report_cache_context

    path = tmp_path / "cvd.xlsx"
    path.write_bytes(b"version1")
    first = build_parts_report_cache_context(tmp_path / "baseline.csv", path)
    path.write_bytes(b"version-two")
    second = build_parts_report_cache_context(tmp_path / "baseline.csv", path)
    assert first["cvd_signature"] != second["cvd_signature"]
    assert first["baseline_signature"] == second["baseline_signature"]


def test_service_recomputes_replacement_across_cvd_cycles_and_keeps_public_projection():
    from io import StringIO
    from types import SimpleNamespace
    from app.sections.equipment_domain.parts_dashboard import _prepare_parts_table
    from src.equipment_domain.application.parts_service import PartsReportService

    spec = pd.DataFrame([{
        "厂别": "ARRAY", "备件类型": "Target", "设备类型": "PVD",
        "膜层": "MO", "制程": "DEPO", "寿命规格": 100,
        "站点": "S1", "机台号-腔室": "EQ1", "参数名称": "%LIFE%",
    }])
    facts = pd.DataFrame({
        "step_id": ["S1"], "sub_equip_id": ["EQ1"], "param_name": ["LIFE"],
        "value": [99.0], "glass_start_time": pd.to_datetime(["2025-12-31"]),
    })
    cvd = cvd_source()
    cvd["寿命规格"] = ["1年", 100]
    cvd["测量值"] = ["2025-01-01", 99]
    cvd["进度"] = [None, 30]
    cvd["测量时间"] = "2025-12-31"
    original = cvd.copy(deep=True)
    port = SimpleNamespace(
        load_baseline=lambda _: spec,
        load_snapshots=lambda *args, **kwargs: (facts, pd.DataFrame()),
        load_cvd=lambda *args, **kwargs: cvd,
    )
    PartsReportService.fetch_report_payload.clear()
    try:
        before = PartsReportService.get_report_data(
            None, "lifecycle-test.csv", "lifecycle-test", as_of_date="2025-12-31",
            cvd_path="lifecycle-test.xlsx", _data_port=port,
        )
        assert before.report_df["预警状态"].tolist() == ["已更换"] * 3
        assert before.report_df["测量值"].tolist() == ["2025-12-31"] * 3
        after = PartsReportService.get_report_data(
            None, "lifecycle-test.csv", "lifecycle-test", as_of_date="2026-01-02",
            cvd_path="lifecycle-test.xlsx", _data_port=port,
        )
        assert after.report_df["预警状态"].tolist() == ["已更换", "正常", "正常"]
        assert after.report_df["测量值"].tolist() == ["2026-01-02", "2026-01-01", 1]
        assert after.total_count == 3 and after.normal_count == 2
        public = _prepare_parts_table(after.report_df)
        assert public["测量值"].tolist() == ["2026-01-02", "2026-01-01", "1"]
        assert public["使用进度"].iloc[0] == 0
        assert public["预警状态"].tolist() == ["已更换", "正常", "正常"]
        assert not {"原始测量值", "是否超规", "数据修饰", "进度", "匹配参数名"} & set(public)
        exported = pd.read_csv(StringIO(public.to_csv(index=False)), dtype=str)
        assert exported["测量值"].tolist() == ["2026-01-02", "2026-01-01", "1"]
        assert set(exported) == set(public)
        pd.testing.assert_frame_equal(cvd, original)
    finally:
        PartsReportService.fetch_report_payload.clear()
