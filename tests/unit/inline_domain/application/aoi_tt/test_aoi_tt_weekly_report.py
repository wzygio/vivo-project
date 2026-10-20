"""TT weekly summaries reuse the chart's station/particle denominators."""
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportViewModel, AoiTtReportService
from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.application.aoi_tt.weekly_report import build_weekly_tt_tables


def detail(*, step="11620", name="TDSUM", qty=6, sheet="S1", time="2026-10-14", factory="ARRAY", size="Total"):
    return dict(prod_code="M626", factory=factory, step_id=step, tt_name=name,
                tt_qty=qty, sheet_id=sheet, start_time=pd.Timestamp(time), particle_size=size)


def report(rows=(), specs=()):
    return AoiTtReportViewModel(pd.DataFrame(rows), pd.DataFrame(specs), pd.DataFrame())


def tables(reports, **kwargs):
    return build_weekly_tt_tables(tuple(reports), load_report=reports.__getitem__,
                                  reference_date=date(2026, 10, 20), **kwargs)


def test_chart_mean_and_particle_rows_do_not_double_count_total():
    rows = [detail(qty=6), detail(qty=10, sheet="S2"), detail(qty=3, size="S"),
            detail(qty=5, size="S", sheet="S2"), detail(qty=900, time="2026-10-20")]
    result = tables({"M626": report(rows)}, particle_sizes=("Total", "S"))[0]
    assert result.week_start == date(2026, 10, 12)
    assert result.week_end == date(2026, 10, 19)
    assert [row.values for row in result.rows] == [("8",), ("4",)]
    assert "Total" in result.rows[0].label and "S" in result.rows[1].label
    assert result.rows[0].layer == "3layer"


def test_no_throughput_zero_and_missing_particle_are_distinct():
    specs = [dict(step_id="11620", tt_name="TDSUM")]
    result = tables({"M626": report([detail(qty=0)]), "M678": report([], specs)}, particle_sizes=("Total", "S"))[0]
    assert result.products == ("M626", "M678")
    assert [row.values for row in result.rows] == [("0", "无过货"), ("数据不足", "无过货")]


def test_fixed_previous_week_does_not_fall_back_to_historical_data():
    rows = [detail(time="2026-09-07", qty=800)]
    assert tables({"M626": report(rows)})[0].rows[0].values == ("无过货",)


def test_layers_station_filter_and_oled_particle_scope():
    rows = [detail(step=s) for s in ("18620", "13620", "11620", "12620", "15620")]
    rows += [detail(factory="OLED", step="21320", name="DSUM_L", qty=10)]
    all_layers = tables({"M626": report(rows)})[0]
    assert [r.layer for r in all_layers.rows] == ["3layer", "M1", "M2", "M3", "M4"]
    selected = tables({"M626": report(rows)}, steps=("12620",), particle_sizes=("S",))
    assert [r.step_id for r in selected[0].rows] == ["12620"]
    assert selected[1].rows[0].values == ("10",)
    assert "Total" in selected[1].rows[0].label


def test_oled_station_denominator_matches_chart_across_parameters():
    rows = [detail(factory="OLED", step="21320", name="DSUM_L", qty=6),
            detail(factory="OLED", step="21320", name="DSUM_O", qty=10, sheet="S2")]
    oled = tables({"M626": report(rows)})[1]
    assert [r.values for r in oled.rows] == [("3",), ("5",)]


def test_specs_survive_empty_real_service_for_no_throughput_projects():
    specs = pd.DataFrame([dict(step_id="11620", tt_name="TDSUM", prod_code="M626")])
    port = SimpleNamespace(get_tt_details=lambda query: pd.DataFrame(), get_tt_spec_limits=lambda p: specs)
    query = AoiTtQueryConfig(prod_code="M626", start_date="2026-09-01", end_date="2026-10-20")
    AoiTtReportService.fetch_aoi_tt_report_payload.clear()
    try:
        result = build_weekly_tt_tables(("M626",), reference_date=date(2026, 10, 20),
            load_report=lambda p: AoiTtReportService.get_aoi_tt_report_data(
                port, query.model_dump_json(), snapshot_signature="tt-weekly-empty-test"))
        assert result[0].rows[0].values == ("无过货",)
    finally:
        AoiTtReportService.fetch_aoi_tt_report_payload.clear()


def test_loading_failure_propagates_instead_of_fabricating_empty_report():
    with pytest.raises(OSError):
        build_weekly_tt_tables(("M626",), reference_date=date(2026, 10, 20),
                               load_report=lambda _: (_ for _ in ()).throw(OSError("unavailable")))


@pytest.mark.parametrize("column, value", [("tt_qty", "bad"), ("start_time", "bad"), ("sheet_id", None)])
def test_invalid_nonempty_input_is_not_rendered_as_zero(column, value):
    row = detail()
    row[column] = value
    with pytest.raises(ValueError):
        tables({"M626": report([row])})
