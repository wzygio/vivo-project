"""Weekly export uses the same period projection as the AOI RS chart."""

from datetime import date

import pandas as pd
import pytest

from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportViewModel
from src.inline_domain.application.aoi_rs.weekly_report import build_weekly_rs_tables
from src.shared_kernel.config import ConfigLoader


def _detail(time="2026-10-12", *, code="A", step="11629", qty=3, sheet="S1", factory="ARRAY"):
    return dict(factory=factory, step_id=step, rs_code=code, code_qty=qty,
                sheet_id=sheet, start_time=pd.Timestamp(time))


def _report(details=(), passes=(), specs=(), indicators=()):
    return AoiRsReportViewModel(
        rs_details_df=pd.DataFrame(details), pass_through_df=pd.DataFrame(passes),
        spec_df=pd.DataFrame(specs), indicators_df=pd.DataFrame(indicators),
        lot_points_df=pd.DataFrame(), sheet_points_df=pd.DataFrame(),
    )


@pytest.fixture
def load_report(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_aoi_rs_special_decoration_factories", lambda: ())
    monkeypatch.setattr(ConfigLoader, "get_aoi_rs_special_decoration_window", lambda: None)
    reports = {}
    calls = []

    def load(product):
        calls.append(product)
        return reports[product]

    load.reports = reports
    load.calls = calls
    return load


def _tables(load_report, products=("M626",), **kwargs):
    return build_weekly_rs_tables(
        products, load_report=load_report, reference_date=date(2026, 10, 19), **kwargs,
    )


def test_weekly_ratio_uses_distinct_station_sheets_and_preserves_product_order(load_report):
    details = [_detail(qty=3), _detail(qty=6, sheet="S2"),
               _detail("2026-10-19", qty=900, sheet="TODAY")]
    passes = details + [_detail(qty=0, sheet="S2"), _detail(qty=0, sheet="S3")]
    load_report.reports.update(M626=_report(details, passes), M678=_report())
    tables = _tables(load_report, ("M678", "M626"))
    array = next(table for table in tables if table.factory == "ARRAY")
    assert load_report.calls == ["M678", "M626"]
    assert array.products == ("M678", "M626")
    assert array.rows[0].values == ("无过货", "3")
    assert array.rows[0].layer == "3layer"
    assert (array.week_start, array.week_end) == (date(2026, 10, 12), date(2026, 10, 19))


def test_month_projection_matches_current_chart_without_cropping_at_previous_sunday(load_report, monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_aoi_rs_special_decoration_factories", lambda: ("ARRAY",))
    details = [_detail(qty=10), _detail("2026-10-19", qty=0, sheet="S2")]
    load_report.reports["M626"] = _report(details, details)
    # Current month's mean is 5, so the current chart caps the previous week at 6.5.
    assert _tables(load_report)[0].rows[0].values == ("6.5",)


def test_specs_indicators_and_details_form_union_and_zero_is_distinct_from_no_throughput(load_report):
    load_report.reports["M626"] = _report(
        [_detail("2026-09-01", code="OLD", qty=99)],
        [_detail(code="IGNORED")],
        [dict(factory="ARRAY", step_id="11629", rs_code="SPEC", code_desc="规格项目"),
         dict(factory="ARRAY", step_id="12629", rs_code="NO_PASS")],
        [dict(factory="ARRAY", step_id="11629", rs_code="IND")],
    )
    rows = {row.code: row for row in _tables(load_report)[0].rows}
    assert set(rows) == {"OLD", "SPEC", "NO_PASS", "IND"}
    assert rows["OLD"].values == rows["SPEC"].values == rows["IND"].values == ("0",)
    assert rows["SPEC"].label == "SPEC（规格项目）"
    assert rows["NO_PASS"].values == ("无过货",)


def test_never_falls_back_to_old_week_without_previous_week_throughput(load_report):
    old = _detail("2026-09-01", qty=99)
    load_report.reports["M626"] = _report([old], [old])
    assert _tables(load_report)[0].rows[0].values == ("无过货",)


def test_array_station_filter_keeps_other_factories_and_unknown_layer(load_report):
    details = [_detail(step="99999"), _detail(step="12629"),
               _detail(factory="OLED", step="OLED_STEP"), _detail(factory="TP", step="TP_STEP")]
    load_report.reports["M626"] = _report(details, details)
    tables = _tables(load_report, steps=("99999",))
    assert [(table.factory, [row.step_id for row in table.rows]) for table in tables] == [
        ("ARRAY", ["99999"]), ("OLED", ["OLED_STEP"]), ("TP", ["TP_STEP"]),
    ]
    assert tables[0].rows[0].layer == "99999"


def test_same_code_at_different_stations_and_products_stays_independent(load_report):
    for product, qty in [("M626", 3), ("M678", 7)]:
        details = [_detail(qty=qty), _detail(step="12629", qty=qty + 1)]
        load_report.reports[product] = _report(details, details)
    rows = _tables(load_report, ("M626", "M678"))[0].rows
    assert [(row.layer, row.values) for row in rows] == [
        ("3layer", ("3", "7")), ("M1", ("4", "8")),
    ]


def test_iso_year_boundary_is_exact_and_end_exclusive(load_report):
    details = [_detail("2025-12-29", qty=4), _detail("2026-01-05", qty=99, sheet="NEW")]
    load_report.reports["M626"] = _report(details, details)
    table = build_weekly_rs_tables(("M626",), load_report=load_report, reference_date=date(2026, 1, 5))[0]
    assert (table.week_start, table.week_end) == (date(2025, 12, 29), date(2026, 1, 5))
    assert table.rows[0].values == ("4",)


@pytest.mark.parametrize(("value", "expected"), [(1.234567, "1.2346"), (1e-8, "1e-08"), (float("nan"), "数据不足"), (float("inf"), "数据不足")])
def test_numeric_format_and_nonfinite_trend(load_report, monkeypatch, value, expected):
    from src.inline_domain.application.aoi_rs import weekly_report

    source = _detail()
    load_report.reports["M626"] = _report([source], [source])
    monkeypatch.setattr(weekly_report, "build_aoi_rs_period_trend", lambda *_args: pd.DataFrame([
        dict(factory="ARRAY", step_id="11629", rs_code="A", period_type="week",
             period_label="2026-W42", value=value),
    ]))
    assert _tables(load_report)[0].rows[0].values == (expected,)


@pytest.mark.parametrize("field", ["rs_details_df", "pass_through_df", "spec_df", "indicators_df"])
def test_nonempty_malformed_input_is_rejected_instead_of_reported_as_no_throughput(load_report, field):
    report = _report()
    setattr(report, field, pd.DataFrame([dict(unexpected="column")]))
    load_report.reports["M626"] = report
    with pytest.raises(ValueError, match=field):
        _tables(load_report)


def test_loader_failure_propagates(load_report):
    def failing(_product):
        raise RuntimeError("report unavailable")

    with pytest.raises(RuntimeError, match="report unavailable"):
        build_weekly_rs_tables(("M626",), load_report=failing, reference_date=date(2026, 10, 19))


def test_report_frames_are_not_mutated(load_report):
    source = pd.DataFrame([_detail()])
    report = _report()
    report.rs_details_df = source
    report.pass_through_df = source
    load_report.reports["M626"] = report
    before = source.copy(deep=True)
    _tables(load_report)
    pd.testing.assert_frame_equal(source, before)


def test_all_known_array_stations_have_layer_labels_in_process_order(load_report):
    details = [_detail(step=step) for step in ("18629", "13629", "11629", "15629", "12629")]
    load_report.reports["M626"] = _report(details, details)
    assert [row.layer for row in _tables(load_report)[0].rows] == ["3layer", "M1", "M2", "M3", "M4"]


@pytest.mark.parametrize(("field", "column", "invalid"), [
    ("rs_details_df", "start_time", "invalid"),
    ("rs_details_df", "code_qty", "invalid"),
    ("rs_details_df", "step_id", None),
    ("pass_through_df", "sheet_id", None),
])
def test_invalid_fact_columns_raise_instead_of_becoming_zero(load_report, field, column, invalid):
    source = _detail()
    source[column] = invalid
    report = _report()
    setattr(report, field, pd.DataFrame([source]))
    load_report.reports["M626"] = report
    with pytest.raises(ValueError, match=field):
        _tables(load_report)


def test_product_scope_removes_disabled_product_facts_before_export(load_report):
    details = [dict(_detail(qty=4), prod_code="M626"),
               dict(_detail(qty=99, sheet="OTHER"), prod_code="DISABLED")]
    load_report.reports["M626"] = _report(details, details)
    assert _tables(load_report)[0].rows[0].values == ("4",)


def test_no_products_does_not_load_any_report(load_report):
    assert _tables(load_report, ()) == []
    assert load_report.calls == []


def test_nonfinite_trend_without_throughput_is_no_throughput(load_report):
    load_report.reports["M626"] = _report([_detail()], [])
    assert _tables(load_report)[0].rows[0].values == ("无过货",)


def test_zero_rs_through_real_service_keeps_throughput_and_spec_projects(load_report):
    from types import SimpleNamespace
    from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportService
    from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig

    passes = pd.DataFrame([_detail()]).drop(columns=["rs_code", "code_qty"])
    specs = pd.DataFrame([dict(factory="ARRAY", step_id="11629", rs_code="ZERO")])
    port = SimpleNamespace(get_rs_details=lambda query: pd.DataFrame(),
                           get_pass_through=lambda query: passes,
                           get_rs_spec_limits=lambda product: specs)
    query = AoiRsQueryConfig(prod_code="M626", start_date="2026-09-01", end_date="2026-10-19")
    AoiRsReportService.fetch_aoi_rs_report_payload.clear()
    try:
        tables = build_weekly_rs_tables(("M626",), reference_date=date(2026, 10, 19),
            load_report=lambda product: AoiRsReportService.get_aoi_rs_report_data(
                _data_port=port, query_config_json=query.model_dump_json(), snapshot_signature="zero-rs-weekly"))
        assert tables[0].rows[0].code == "ZERO"
        assert tables[0].rows[0].values == ("0",)
    finally:
        AoiRsReportService.fetch_aoi_rs_report_payload.clear()


def test_repeated_non_array_code_labels_identify_each_station(load_report):
    details = [_detail(factory="OLED", code="C4BP1", step="21329", qty=2),
               _detail(factory="OLED", code="C4BP1", step="22329", qty=9)]
    load_report.reports["M626"] = _report(details, details)
    oled = _tables(load_report, factories=("OLED",))[0]
    assert oled.rows[0].label != oled.rows[1].label
    assert "21329" in oled.rows[0].label and "22329" in oled.rows[1].label
    assert [row.values for row in oled.rows] == [("2",), ("9",)]
