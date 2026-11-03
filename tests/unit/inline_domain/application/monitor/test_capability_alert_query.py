"""Saved capability queries keep decisions, calendar weeks and data spans distinct."""
from datetime import date

import pandas as pd
import pytest

from src.inline_domain.application.monitor.capability_alert_query import CapabilityAlertQuery
from src.inline_domain.core.monitor.cpk_latest import (
    latest_capability_alerts, normalize_latest_cpk, select_capability_alert_records,
)
from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore
from src.inline_domain.infrastructure.monitor.cpm_latest_excel_store import CpmLatestExcelStore
from src.shared_kernel.config import ConfigLoader


TODAY = date(2026, 11, 3)


def row(metric="cpk", **changes):
    return {"prod_code": "P1", "factory": "ARRAY", "step_id": "00100", "param_name": "CD",
            "period_type": "week", "period_label": "2026-W44", "flag": False,
            "period_start": "2026-10-28 08:00:00", "period_end": "2026-10-30 09:00:00",
            f"{metric}_corrected": 1.2, **changes}


class MemoryReader:
    def __init__(self, rows, metric="cpk", span=0):
        self.frame = (None if rows is None else
                      normalize_latest_cpk(pd.DataFrame(rows), "P1", metric=metric, min_span_hours=span))
        self.calls = []

    def read_product(self, product):
        self.calls.append(product)
        return None if self.frame is None else self.frame.copy(deep=True)


def query(rows, metric="cpk", *, span=0, exclusion=None):
    reader = MemoryReader(rows, metric, span)
    return CapabilityAlertQuery(reader, metric=metric, date_exclusion=exclusion, min_span_hours=span), reader


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_previous_week_retains_identity_and_true_data_span(metric):
    service, reader = query([row(metric)], metric)
    before = reader.frame.copy(deep=True)
    result = service.read("P1", TODAY)
    assert result["state"] == "alert"
    assert result["metadata"]["availability"] == "ready"
    item = result["records"].iloc[0]
    assert item[metric] == 1.2 and item["prod_code"] == "P1" and item["flag"] == "false"
    assert item["calendar_start"] == date(2026, 10, 26)
    assert item["calendar_end"] == date(2026, 11, 1)
    assert item["period_start"] == "2026-10-28 08:00:00"
    assert item["period_end"] == "2026-10-30 09:00:00"
    pd.testing.assert_frame_equal(reader.frame, before)


def test_existing_release_and_threshold_rules_are_applied_without_sheet_ppa_exclusion():
    rows = [row(param_name="PPA_X"), row(param_name="HIDDEN", flag=True),
            row(param_name="DELETED", flag="Delete"), row(param_name="UNDECIDED", flag=""),
            row(param_name="BOUNDARY", cpk_corrected=1.33), row(param_name="UNKNOWN", cpk_corrected=None)]
    service, _ = query(rows)
    result = service.read("P1", TODAY)
    assert result["records"]["param_name"].tolist() == ["PPA_X"]


@pytest.mark.parametrize("end", [None, "2026-10-27", "2026-10-28 08:00:00"])
def test_invalid_or_insufficient_actual_span_never_uses_calendar_week_to_fill_it(end):
    service, _ = query([row(period_end=end)], span=48)
    result = service.read("P1", TODAY)
    assert result["records"].empty and result["metadata"]["availability"] == "ready"


def test_zero_span_configuration_still_requires_ordered_real_times():
    service, _ = query([row(period_end="2026-10-28 08:00:00")], span=0)
    assert len(service.read("P1", TODAY)["records"]) == 1


def test_iso_year_rollover_uses_label_not_data_start_or_query_end_as_reference():
    service, _ = query([row(period_label="2025-W52", period_start="2025-12-26", period_end="2025-12-28")])
    result = service.read("P1", date(2026, 1, 1))
    assert result["records"]["calendar_start"].tolist() == [date(2025, 12, 22)]
    assert result["records"]["calendar_end"].tolist() == [date(2025, 12, 28)]


def test_explicit_end_sunday_does_not_select_the_week_before_it():
    service, _ = query([row(), row(period_label="2026-W43", param_name="OLDER")])
    result = service.read("P1", TODAY, start_date="2026-10-26", end_date="2026-11-01")
    assert result["records"]["period_label"].tolist() == ["2026-W44"]


def test_explicit_range_only_includes_whole_weeks_and_keeps_all_selected_weeks():
    service, _ = query([row(), row(period_label="2026-W43", param_name="OLDER"),
                        row(period_label="2026-W45", param_name="FUTURE")])
    result = service.read("P1", TODAY, start_date="2026-10-19", end_date="2026-11-08")
    assert set(result["records"]["period_label"]) == {"2026-W43", "2026-W44"}


def test_partial_first_week_and_unclosed_future_week_are_not_returned():
    service, _ = query([row(), row(period_label="2026-W43", param_name="OLDER"),
                        row(period_label="2026-W45", param_name="CURRENT"),
                        row(period_label="2026-W46", param_name="FUTURE")])
    result = service.read("P1", TODAY, start_date="2026-10-20", end_date="2026-11-15")
    assert result["records"]["period_label"].tolist() == ["2026-W44"]


@pytest.mark.parametrize("start,end", [("2026-11-02", "2026-11-08"), ("2026-11-09", "2026-11-15")])
def test_only_current_or_future_weeks_are_ready_empty_without_source_read(start, end):
    service, reader = query(None)
    result = service.read("P1", TODAY, start_date=start, end_date=end)
    assert result["metadata"]["availability"] == "ready" and result["records"].empty
    assert reader.calls == []


def test_date_range_with_no_complete_week_is_ready_empty_without_reading_source():
    service, reader = query(None)
    result = service.read("P1", TODAY, start_date="2026-10-28", end_date="2026-10-30")
    assert result["records"].empty and result["metadata"]["availability"] == "ready"
    assert reader.calls == []


@pytest.mark.parametrize("rows", [None, [], [row(period_label="2026-W43")]])
def test_missing_source_or_requested_week_is_not_reported_as_complete_no_anomalies(rows):
    service, _ = query(rows)
    result = service.read("P1", TODAY)
    assert result["metadata"]["availability"] == "missing"
    assert result["records"].empty


def test_missing_one_of_several_requested_weeks_is_unavailable():
    service, _ = query([row()])
    result = service.read("P1", TODAY, start_date="2026-10-19", end_date="2026-11-01")
    assert result["metadata"]["availability"] == "missing"


def test_other_period_kinds_do_not_substitute_for_missing_week():
    service, _ = query([row(period_type="month", period_label="2026-11")])
    assert service.read("P1", TODAY)["metadata"]["availability"] == "missing"


def test_reversed_date_range_is_rejected_before_source_read():
    service, reader = query(None)
    with pytest.raises(ValueError, match="date range"):
        service.read("P1", TODAY, start_date="2026-11-01", end_date="2026-10-26")
    assert reader.calls == []


def test_existing_week_with_only_hidden_or_qualified_items_is_ready_empty():
    service, _ = query([row(flag=True), row(param_name="QUALIFIED", cpk_corrected=1.8)])
    result = service.read("P1", TODAY)
    assert result["state"] == "ok" and result["metadata"]["availability"] == "ready"


def test_period_date_exclusion_drops_whole_affected_aggregate():
    service, _ = query([row(factory="OLED"), row(factory="ARRAY")],
                       exclusion=(("OLED", "TP"), "2026-10-29", "2026-10-29"))
    assert service.read("P1", TODAY)["records"]["factory"].tolist() == ["ARRAY"]


def test_legacy_five_column_latest_projection_is_preserved():
    normalized = normalize_latest_cpk(pd.DataFrame([row()]), "P1", min_span_hours=0)
    selected = select_capability_alert_records(normalized, TODAY, min_span_hours=0)
    legacy = latest_capability_alerts(normalized, TODAY, min_span_hours=0)
    assert list(legacy.columns) == ["factory", "step_id", "param_name", "period_label", "cpk"]
    pd.testing.assert_frame_equal(legacy, selected[list(legacy.columns)])


@pytest.mark.parametrize("label", ["2026-W99", "2026-W1", "invalid"])
def test_invalid_week_labels_do_not_become_silent_empty_queries(label):
    service, _ = query([row(period_label=label)])
    with pytest.raises(ValueError):
        service.read("P1", TODAY)


@pytest.mark.parametrize("store_type,metric,suffix", [(CpkLatestExcelStore, "cpk", ""),
                                                     (CpmLatestExcelStore, "cpm", "_cpm")])
def test_store_uses_injected_reader_and_correct_product_sheet(tmp_path, monkeypatch, store_type, metric, suffix):
    path = tmp_path/"capability.xlsx"
    path.touch()
    frames = {"P1"+suffix: pd.DataFrame([row(metric)])}
    calls = []
    monkeypatch.setattr(ConfigLoader, "get_spc_capability_alert_min_span_hours", lambda: 0)
    def reader(*args):
        calls.append(args)
        return frames
    store = store_type(path, workbook_reader=reader, strict_empty=True)
    assert store.read_product("P1")[metric].tolist() == [1.2]
    assert calls[0][0] == str(path.resolve())
    assert frames["P1"+suffix]["flag"].tolist() == [False]


def test_strict_empty_store_rejects_missing_headers_but_legacy_stays_compatible(tmp_path):
    path = tmp_path/"capability.xlsx"
    path.touch()
    reader = lambda *_: {"P1": pd.DataFrame(columns=["unrelated"])}
    with pytest.raises(ValueError, match="缺少列"):
        CpkLatestExcelStore(path, workbook_reader=reader, strict_empty=True).read_product("P1")
    assert CpkLatestExcelStore(path, workbook_reader=reader).read_product("P1").empty


def test_missing_file_does_not_call_workbook_reader(tmp_path):
    def reader(*args):
        pytest.fail("a missing file must not invoke the workbook parser")
    assert CpkLatestExcelStore(tmp_path/"missing.xlsx", workbook_reader=reader).read_product("P1") is None


def test_composition_binds_strict_injected_read_only_query(tmp_path, monkeypatch):
    from src.inline_domain.composition import build_capability_alert_query
    path = tmp_path/"relocated.xlsx"
    path.touch()
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda *args: path)
    monkeypatch.setattr(ConfigLoader, "get_spc_capability_alert_min_span_hours", lambda: 0)
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    service = build_capability_alert_query("cpm", workbook_reader=lambda *_: {"P1_cpm": pd.DataFrame([row("cpm")])})
    assert service.read("P1", TODAY)["records"]["cpm"].tolist() == [1.2]
