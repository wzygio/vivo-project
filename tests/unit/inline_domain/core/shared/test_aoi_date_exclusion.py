from datetime import date

import pandas as pd
import pytest

from src.inline_domain.core.aoi_rs.aoi_rs_decoration import filter_aoi_rs_report_data
from src.inline_domain.core.aoi_tt.aoi_tt_decoration import apply_aoi_tt_decoration
from src.shared_kernel.config import ConfigLoader
from src.inline_domain.core.shared.date_exclusion import (
    exclude_inline_factory_dates,
    exclude_inline_period_records,
)


EXCLUSION = (("OLED",), "2026-10-01", "2026-10-06")


def _details() -> pd.DataFrame:
    return pd.DataFrame({
        "factory": ["OLED", " oled ", "OLED", "OLED", "OLED", "ARRAY", "OLED"],
        "start_time": pd.to_datetime([
            "2026-09-30 23:59:59", "2026-10-01", "2026-10-06 23:59:59",
            "2026-10-07", None, "2026-10-02", "2025-10-02",
        ], format="mixed"),
        "sheet_id": list("ABCDEFG"),
        "tt_qty": [1.] * 7,
    })


def test_rs_filters_details_and_throughput_with_inclusive_calendar_dates():
    source = _details()
    before = source.copy(deep=True)
    details, throughput = filter_aoi_rs_report_data(source, source, EXCLUSION)
    assert details.sheet_id.tolist() == list("ADEFG")
    pd.testing.assert_frame_equal(details, throughput)
    pd.testing.assert_frame_equal(source, before)


def test_tt_exclusion_applies_without_specs_or_decisions():
    source = _details()
    result = apply_aoi_tt_decoration(
        source, pd.DataFrame(), pd.DataFrame(), date_exclusion=EXCLUSION,
    )
    assert result.sheet_id.tolist() == list("ADEFG")
    assert source.sheet_id.tolist() == list("ABCDEFG")


def test_tt_date_exclusion_takes_precedence_over_false_and_exemptions():
    source = _details().assign(prod_code="P", step_id="1", tt_name="PPA")
    decisions = source.assign(flag=False)
    specs = pd.DataFrame([dict(step_id="1", tt_name="PPA", ucl=10)])
    result = apply_aoi_tt_decoration(
        source, specs, decisions, ["PPA"], date_exclusion=EXCLUSION,
    )
    assert result.sheet_id.tolist() == list("ADEFG")


def test_disabled_rule_preserves_inputs_and_empty_frames():
    source = pd.DataFrame({"tt_qty": [3.]})
    details, throughput = filter_aoi_rs_report_data(source, pd.DataFrame(), None)
    pd.testing.assert_frame_equal(details, source)
    assert throughput.empty
    result = apply_aoi_tt_decoration(source, pd.DataFrame(), pd.DataFrame())
    pd.testing.assert_frame_equal(result, source)


def test_active_rule_rejects_missing_time_instead_of_silently_showing_data():
    with pytest.raises(ValueError, match="start_time"):
        filter_aoi_rs_report_data(pd.DataFrame({"factory": ["OLED"]}), pd.DataFrame(), EXCLUSION)


def test_config_resolves_today_and_normalizes_factories(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _domain: {
        "data_exclusion": {
            "factories": [" OLED ", "array", "OLED"],
            "start_date": "2026-10-01", "end_date": "today",
        },
    })
    assert ConfigLoader.get_inline_data_exclusion(today=date(2026, 10, 6)) == (
        ("OLED", "ARRAY"), "2026-10-01", "2026-10-06",
    )


@pytest.mark.parametrize("section", [{}, {"factories": []}])
def test_missing_or_disabled_config_does_not_filter(monkeypatch, section):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _domain: {"data_exclusion": section})
    assert ConfigLoader.get_inline_data_exclusion() is None


@pytest.mark.parametrize("overrides", [
    {"factories": "OLED"}, {"factories": [""]}, {"start_date": "bad-date"},
    {"end_date": "2026-09-30"}, {"end_date": None},
])
def test_invalid_config_is_rejected(monkeypatch, overrides):
    section = {"factories": ["OLED"], "start_date": "2026-10-01", "end_date": "2026-10-06", **overrides}
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _domain: {"data_exclusion": section})
    with pytest.raises(ValueError, match="data_exclusion"):
        ConfigLoader.get_inline_data_exclusion()


@pytest.mark.parametrize("time_column", ["start_time", "sheet_start_time", "event_time", "event_date"])
def test_all_inline_event_columns_share_the_same_exclusion(time_column):
    source = _details().rename(columns={"start_time": time_column})
    result = exclude_inline_factory_dates(source, EXCLUSION, time_column=time_column)
    assert result.sheet_id.tolist() == list("ADEFG")


def test_legacy_config_remains_usable_and_new_config_takes_precedence(monkeypatch):
    rule = {"factories": ["TP"], "start_date": "2026-10-01", "end_date": "2026-10-06"}
    domain = {"aoi_data_exclusion": rule}
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda _domain: domain)
    assert ConfigLoader.get_inline_data_exclusion() == (("TP",), "2026-10-01", "2026-10-06")
    domain["data_exclusion"] = {"factories": []}
    assert ConfigLoader.get_inline_data_exclusion() is None


def test_maintained_aggregates_with_excluded_dates_are_omitted_without_changing_source():
    records = pd.DataFrame({
        "factory": ["OLED", "OLED", "ARRAY", "ALL", "OLED"],
        "period_type": ["month", "week", "month", "month", "day"],
        "period_label": ["2026-09", "2026-W40", "2026-10", "2026-10", "2026-09-30"],
    })
    before = records.copy(deep=True)
    result = exclude_inline_period_records(records, EXCLUSION, as_of=pd.Timestamp("2026-10-06"))
    assert result.index.tolist() == [0, 2, 4]
    pd.testing.assert_frame_equal(records, before)


def test_period_filter_preserves_duplicate_indexes_and_handles_year_quarter():
    records = pd.DataFrame({
        "factory": ["OLED", "ARRAY", "OLED", "OLED"],
        "period_type": ["年度", "年度", "季度", "季度"],
        "period_label": ["2026", "2026", "2026-Q3", "2026-Q4"],
    }, index=[0, 0, 0, 0])
    result = exclude_inline_period_records(records, EXCLUSION, as_of=pd.Timestamp("2026-10-06"))
    assert result.factory.tolist() == ["ARRAY", "OLED"]
    assert result.period_label.tolist() == ["2026", "2026-Q3"]
