"""Prepared BySheet membership is published explicitly and read without producers."""
from copy import deepcopy
from datetime import date
import json

import pandas as pd
import pytest

from src.inline_domain.application.spc.sheet_membership import (
    SpcSheetMembershipQuery, SpcSheetMembershipUnavailable, publish_spc_sheet_memberships,
)
from src.inline_domain.infrastructure.spc.sheet_membership_store import SpcSheetMembershipJsonStore


def points():
    base = dict(prod_code="P1", factory="ARRAY", step_id="00100", param_name="CD", param_value=1.2)
    return pd.DataFrame([
        dict(base, sheet_id="0002", sheet_start_time="2026-10-26 00:00:00"),
        dict(base, sheet_id="0001", sheet_start_time="2026-11-01 23:59:59"),
        dict(base, sheet_id="0002", sheet_start_time="2026-10-27 01:00:00"),
        dict(base, sheet_id="older", sheet_start_time="2026-10-25 23:59:59"),
        dict(base, sheet_id="current", sheet_start_time="2026-11-02 00:00:00"),
        dict(base, sheet_id="invalid", sheet_start_time="2026-10-27", param_value=None),
        dict(base, sheet_id="other-param", sheet_start_time="2026-10-27", param_name="OTHER"),
        dict(base, sheet_id="other-factory", sheet_start_time="2026-10-27", factory="OLED"),
    ])


def capability(**changes):
    return pd.DataFrame([{**dict(prod_code="P1", factory="ARRAY", step_id="00100", param_name="CD",
                                period_type="week", period_label="2026-W44", cpk_corrected=1.2,
                                cpm_corrected=1.1, period_start="2026-10-26 00:00:00",
                                period_end="2026-11-01 23:59:59"), **changes}])


def publish(store, frame=None, **kwargs):
    return publish_spc_sheet_memberships(
        points() if frame is None else frame, store=store, product="P1", start_date="2026-10-26",
        end_date="2026-11-01", reference_date=date(2026, 11, 4), policy_signature="fixture-policy",
        capability_records=capability(), **kwargs,
    )


def read(query, **kwargs):
    values = dict(product="P1", factory="ARRAY", step_id="00100", parameter_name="CD",
                  start_date=date(2026, 10, 26), end_date=date(2026, 11, 1))
    values.update(kwargs)
    return query.sheet_ids(**values)


def test_real_json_snapshot_keeps_all_unique_text_ids_and_excludes_other_week_scope(tmp_path):
    store = SpcSheetMembershipJsonStore(tmp_path)
    before = points()
    original = before.copy(deep=True)
    assert publish(store, before) == ("2026-W44",)
    query = SpcSheetMembershipQuery(store, policy_signature="fixture-policy")
    assert read(query, metric="cpk", expected_value=1.2,
                expected_period_start="2026-10-26", expected_period_end="2026-11-01 23:59:59") == ["0001", "0002"]
    assert read(query, metric="cpm", expected_value=1.1) == ["0001", "0002"]
    pd.testing.assert_frame_equal(before, original)


def test_no_row_limit_and_duplicate_points_do_not_duplicate_ids(tmp_path):
    frame = pd.DataFrame([dict(prod_code="P1", factory="ARRAY", step_id="00100", param_name="CD",
                               sheet_id=f"{number:06d}", sheet_start_time="2026-10-28", param_value=number)
                          for number in range(1205)])
    store = SpcSheetMembershipJsonStore(tmp_path)
    publish(store, pd.concat([frame, frame], ignore_index=True))
    assert read(SpcSheetMembershipQuery(store, policy_signature="fixture-policy")) == [f"{n:06d}" for n in range(1205)]


@pytest.mark.parametrize("changes", [dict(factory="TP"), dict(step_id="missing"), dict(parameter_name="missing"),
                                     dict(product="P2"), dict(start_date="2026-10-19", end_date="2026-10-25")])
def test_missing_snapshot_or_business_group_is_explicitly_unavailable(tmp_path, changes):
    store = SpcSheetMembershipJsonStore(tmp_path)
    publish(store)
    with pytest.raises(SpcSheetMembershipUnavailable):
        read(SpcSheetMembershipQuery(store, policy_signature="fixture-policy"), **changes)


def test_policy_change_invalidates_snapshot_without_rewriting_it(tmp_path):
    store = SpcSheetMembershipJsonStore(tmp_path)
    publish(store)
    before = {path: path.read_bytes() for path in tmp_path.rglob("*.json")}
    with pytest.raises(SpcSheetMembershipUnavailable):
        read(SpcSheetMembershipQuery(store, policy_signature=lambda: "changed-policy"))
    assert before == {path: path.read_bytes() for path in before}


def test_capability_value_or_source_span_cannot_silently_use_previous_generation(tmp_path):
    store = SpcSheetMembershipJsonStore(tmp_path)
    publish(store)
    query = SpcSheetMembershipQuery(store, policy_signature="fixture-policy")
    with pytest.raises(SpcSheetMembershipUnavailable):
        read(query, metric="cpk", expected_value=.9)
    with pytest.raises(SpcSheetMembershipUnavailable):
        read(query, expected_period_start="2026-10-25", expected_period_end="2026-11-01")
    with pytest.raises(SpcSheetMembershipUnavailable):
        read(query, expected_source_generation="wrong-generation")


def test_metric_span_can_be_inside_broader_by_sheet_span(tmp_path):
    store = SpcSheetMembershipJsonStore(tmp_path)
    publish_spc_sheet_memberships(points(), store=store, product="P1", start_date="2026-10-26",
        end_date="2026-11-01", reference_date=date(2026,11,4), policy_signature="fixture-policy",
        capability_records=capability(period_start="2026-10-27", period_end="2026-10-31"))
    assert read(SpcSheetMembershipQuery(store, policy_signature="fixture-policy"), metric="cpk", expected_value=1.2,
                expected_period_start="2026-10-27", expected_period_end="2026-10-31") == ["0001", "0002"]


def test_pure_prepared_publication_requires_an_anchor_for_capability_requests(tmp_path):
    store = SpcSheetMembershipJsonStore(tmp_path)
    publish_spc_sheet_memberships(points(), store=store, product="P1", start_date="2026-10-26",
                                 end_date="2026-11-01", reference_date=date(2026,11,4), policy_signature="fixture-policy")
    query = SpcSheetMembershipQuery(store, policy_signature="fixture-policy")
    assert read(query) == ["0001", "0002"]
    with pytest.raises(SpcSheetMembershipUnavailable):
        read(query, metric="cpk", expected_value=1.2)


def test_complete_empty_republication_invalidates_old_members_without_erasing_other_weeks(tmp_path):
    store = SpcSheetMembershipJsonStore(tmp_path)
    publish(store)
    assert publish_spc_sheet_memberships(pd.DataFrame(), store=store, product="P1", start_date="2026-10-26",
        end_date="2026-11-01", reference_date=date(2026,11,4), policy_signature="fixture-policy") == ("2026-W44",)
    assert store.read("P1", date(2026,10,26))["groups"] == []
    with pytest.raises(SpcSheetMembershipUnavailable):
        read(SpcSheetMembershipQuery(store, policy_signature="fixture-policy"), metric="cpk", expected_value=1.2)


def test_new_week_publication_preserves_previous_week_and_generation_is_week_local(tmp_path):
    store = SpcSheetMembershipJsonStore(tmp_path)
    publish(store, source_generation="raw-snapshot-a")
    old = store.read("P1", date(2026,10,26))
    newer = points().assign(sheet_start_time="2026-11-03")
    publish_spc_sheet_memberships(newer, store=store, product="P1", start_date="2026-11-02", end_date="2026-11-08",
                                 reference_date=date(2026,11,9), policy_signature="fixture-policy", source_generation="raw-snapshot-b")
    assert store.read("P1", date(2026,10,26)) == old
    publish(store, source_generation="raw-snapshot-c")
    assert store.read("P1", date(2026,10,26))["source_generation"] == old["source_generation"]


def test_partial_and_unclosed_weeks_are_not_published(tmp_path):
    store = SpcSheetMembershipJsonStore(tmp_path)
    assert publish_spc_sheet_memberships(points(), store=store, product="P1", start_date="2026-10-27",
        end_date="2026-11-04", reference_date=date(2026,11,4)) == ()
    assert not list(tmp_path.rglob("*.json"))


def test_store_write_failure_preserves_previous_snapshot(tmp_path, monkeypatch):
    import src.inline_domain.infrastructure.spc.sheet_membership_store as storage
    store = SpcSheetMembershipJsonStore(tmp_path)
    publish(store)
    before = {path: path.read_bytes() for path in tmp_path.rglob("*.json")}
    def fail(*args):
        raise PermissionError("fixture denied")
    monkeypatch.setattr(storage.os, "replace", fail)
    with pytest.raises(OSError):
        publish(store, points().assign(sheet_id="changed"))
    assert before == {path: path.read_bytes() for path in before}
    assert not list(tmp_path.rglob("*.tmp"))


def test_bad_or_incomplete_snapshot_is_not_an_empty_success(tmp_path):
    store = SpcSheetMembershipJsonStore(tmp_path)
    publish(store)
    snapshot = store.read("P1", date(2026,10,26))
    for change in ({"coverage_end_exclusive": "2026-11-01"}, {"groups": []}):
        corrupt = {**deepcopy(snapshot), **change}
        class FakeStore:
            def read(self, *args):
                return corrupt
        with pytest.raises(SpcSheetMembershipUnavailable):
            read(SpcSheetMembershipQuery(FakeStore(), policy_signature="fixture-policy"))


@pytest.mark.parametrize("product", ["../outside", "P1/other", "", "P1\\other"])
def test_store_rejects_path_like_product_ids(tmp_path, product):
    with pytest.raises(ValueError):
        SpcSheetMembershipJsonStore(tmp_path).read(product, date(2026,10,26))


def _isolated_report(monkeypatch, tmp_path, payload):
    from src.inline_domain.application.spc import spc_service
    from src.inline_domain.application.spc.dtos import SpcQueryConfig
    captured = []
    monkeypatch.setattr(spc_service.ConfigLoader, "get_domain_resource_path", lambda *args: tmp_path/"ledger.xlsx")
    monkeypatch.setattr(spc_service.ConfigLoader, "get_domain_resource_dir", lambda *args: tmp_path)
    monkeypatch.setattr(spc_service, "get_capability_decoration_signature", lambda *args, **kwargs: (0, 0))
    monkeypatch.setattr(spc_service.ConfigLoader, "get_inline_data_exclusion", lambda: None)
    def cached(**kwargs):
        captured.append(kwargs)
        return payload
    monkeypatch.setattr(spc_service.SpcReportService, "fetch_spc_report_payload", cached)
    query = SpcQueryConfig(prod_code="P1", start_date="2026-10-26", end_date="2026-11-04")
    return spc_service, query.model_dump_json(), captured


def test_report_publishes_outside_cached_payload_and_policy_reaches_prepared_cache_key(monkeypatch, tmp_path):
    payload = {"raw_measurements_df": points(), "period_capability_df": capability()}
    service, query_json, calls = _isolated_report(monkeypatch, tmp_path, payload)
    store = SpcSheetMembershipJsonStore(tmp_path/"members")
    kwargs = dict(_data_port=object(), query_config_json=query_json, snapshot_signature="same-source",
                  _sheet_membership_store=store,
                  _sheet_membership_coverage=lambda: (date(2026,10,26), date(2026,11,4)))
    service.SpcReportService.get_spc_report_data(**kwargs, sheet_membership_policy_signature="policy-a")
    service.SpcReportService.get_spc_report_data(**kwargs, sheet_membership_policy_signature="policy-b")
    assert calls[0]["snapshot_signature"] != calls[1]["snapshot_signature"]
    assert "sheet_membership_policy=policy-b" in calls[1]["snapshot_signature"]
    assert "_sheet_membership_store" not in calls[0]
    assert read(SpcSheetMembershipQuery(store, policy_signature="policy-b"), metric="cpk", expected_value=1.2) == ["0001", "0002"]
    assert not any("membership" in key for key in payload)


def test_report_empty_payload_replaces_requested_week_with_explicit_unavailable_generation(monkeypatch, tmp_path):
    store = SpcSheetMembershipJsonStore(tmp_path/"members")
    publish(store)
    service, query_json, _ = _isolated_report(monkeypatch, tmp_path, {"raw_measurements_df": pd.DataFrame()})
    service.SpcReportService.get_spc_report_data(_data_port=object(), query_config_json=query_json,
        _sheet_membership_store=store, sheet_membership_policy_signature="fixture-policy",
        _sheet_membership_coverage=lambda: (date(2026,10,26), date(2026,11,4)))
    assert store.read("P1", date(2026,10,26))["groups"] == []
    with pytest.raises(SpcSheetMembershipUnavailable):
        read(SpcSheetMembershipQuery(store, policy_signature="fixture-policy"))


def test_member_publication_failure_is_logged_and_report_does_not_claim_success(monkeypatch, tmp_path, caplog):
    service, query_json, _ = _isolated_report(monkeypatch, tmp_path,
        {"raw_measurements_df": points(), "period_capability_df": capability()})
    class BrokenStore:
        def publish(self, snapshot):
            raise PermissionError("injected failure")
    with pytest.raises(service.SpcReportBuildError, match="Sheet membership publication failed"):
        service.SpcReportService.get_spc_report_data(_data_port=object(), query_config_json=query_json,
            _sheet_membership_store=BrokenStore(), sheet_membership_policy_signature="fixture-policy",
            _sheet_membership_coverage=lambda: (date(2026,10,26), date(2026,11,4)))
    assert "Sheet membership publication failed" in caplog.text


def test_report_without_injected_membership_store_keeps_legacy_read_path(monkeypatch, tmp_path):
    service, query_json, calls = _isolated_report(monkeypatch, tmp_path, {"raw_measurements_df": points()})
    def forbidden(*args, **kwargs):
        pytest.fail("legacy calls must not publish an uninjected membership store")
    monkeypatch.setattr(service, "publish_spc_sheet_memberships", forbidden)
    service.SpcReportService.get_spc_report_data(_data_port=object(), query_config_json=query_json)
    assert "sheet_membership_policy=" not in calls[0]["snapshot_signature"]


@pytest.mark.parametrize("coverage", [None, lambda: (date(2026,10,26), date(2026,10,30)),
                                     lambda: (date(2026,10,27), date(2026,11,4))])
def test_report_does_not_publish_request_dates_as_unproven_or_partial_source_coverage(monkeypatch, tmp_path, coverage):
    service, query_json, _ = _isolated_report(monkeypatch, tmp_path,
        {"raw_measurements_df": points(), "period_capability_df": capability()})
    store = SpcSheetMembershipJsonStore(tmp_path/"members")
    publish(store)
    before = store.read("P1", date(2026,10,26))
    with pytest.raises(service.SpcReportBuildError, match="coverage"):
        service.SpcReportService.get_spc_report_data(_data_port=object(), query_config_json=query_json,
            _sheet_membership_store=store, _sheet_membership_coverage=coverage,
            sheet_membership_policy_signature="fixture-policy")
    assert store.read("P1", date(2026,10,26)) == before


@pytest.mark.parametrize("native,provider,succeeds", [
    (None, {"start_date":"2026-10-26", "end_date":"2026-11-04", "source_generation":"new"}, False),
    ({"start_date":"2026-10-26", "end_date":"2026-10-30", "source_generation":"same"},
     {"start_date":"2026-10-26", "end_date":"2026-11-04", "source_generation":"same"}, False),
    ({"start_date":"2026-10-26", "end_date":"2026-11-04", "source_generation":"cached-old"},
     {"start_date":"2026-10-26", "end_date":"2026-11-04", "source_generation":"current-new"}, False),
    ({"start_date":"2026-10-26", "end_date":"2026-11-04", "source_generation":"same"},
     {"start_date":"2026-10-26", "end_date":"2026-11-04", "source_generation":"same"}, True),
])
def test_production_reports_require_same_batch_native_coverage(monkeypatch, tmp_path, native, provider, succeeds):
    payload = {"raw_measurements_df": points(), "period_capability_df": capability(), "spc_source_coverage": native}
    service, query_json, _ = _isolated_report(monkeypatch, tmp_path, payload)
    class ProductionPort:
        supports_shared_history_persistence = True
    store = SpcSheetMembershipJsonStore(tmp_path/"members")
    kwargs = dict(_data_port=ProductionPort(), query_config_json=query_json,
                  _sheet_membership_store=store, _sheet_membership_coverage=lambda: provider,
                  sheet_membership_policy_signature="fixture-policy")
    if succeeds:
        service.SpcReportService.get_spc_report_data(**kwargs)
        assert read(SpcSheetMembershipQuery(store, policy_signature="fixture-policy")) == ["0001", "0002"]
    else:
        with pytest.raises(service.SpcReportBuildError, match="coverage"):
            service.SpcReportService.get_spc_report_data(**kwargs)
        assert not list((tmp_path/"members").rglob("*.json"))


def test_partial_native_coverage_is_rejected_before_capability_decisions_are_written(monkeypatch):
    from src.inline_domain.application.spc import spc_service as service
    from src.inline_domain.application.spc.dtos import SpcQueryConfig
    native = {"start_date":"2026-10-26", "end_date":"2026-10-30", "source_generation":"same"}
    monkeypatch.setattr(service, "fetch_decorated_features", lambda **kwargs: {
        "raw_measurements_df": points(), "sheet_features_df": pd.DataFrame(), "spec_empty": False,
        "spc_source_coverage": native})
    def forbidden(*args, **kwargs):
        pytest.fail("partial source coverage must be rejected before capability publication")
    monkeypatch.setattr(service, "prepare_capability_decoration", forbidden)
    service.SpcReportService.fetch_spc_report_payload.clear()
    config = SpcQueryConfig(prod_code="P1", start_date="2026-10-26", end_date="2026-11-04")
    try:
        with pytest.raises(service.SpcReportBuildError):
            service.SpcReportService.fetch_spc_report_payload(_data_port=object(), query_config_json=config.model_dump_json(),
                _sheet_membership_coverage=lambda: native, _require_native_sheet_membership_coverage=True)
    finally:
        service.SpcReportService.fetch_spc_report_payload.clear()


def test_snapshot_upstream_generation_comes_from_native_payload_not_a_new_file_label(monkeypatch, tmp_path):
    native = {"start_date":"2026-10-26", "end_date":"2026-11-04", "source_generation":"bound-native"}
    service, query_json, _ = _isolated_report(monkeypatch, tmp_path,
        {"raw_measurements_df": points(), "period_capability_df": capability(), "spc_source_coverage": native})
    store = SpcSheetMembershipJsonStore(tmp_path/"members")
    service.SpcReportService.get_spc_report_data(_data_port=object(), query_config_json=query_json,
        snapshot_signature="different-cache-label", _sheet_membership_store=store,
        _sheet_membership_coverage=lambda: native, sheet_membership_policy_signature="fixture-policy")
    import hashlib
    expected = hashlib.sha256(json.dumps("bound-native", separators=(",", ":")).encode()).hexdigest()
    assert store.read("P1", date(2026,10,26))["upstream_generation"] == expected
