"""Offline capability totals consume the report's final points and verified dates."""

import json
import os
from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.infrastructure.monitor.capability_snapshot_source import (
    SnapshotCapabilityInputError, SnapshotCapabilitySource,
)
from src.inline_domain.infrastructure.monitor import capability_snapshot_source as source_module
from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import InlineMeasurementSnapshotRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy


@pytest.fixture
def policies(monkeypatch, tmp_path):
    monkeypatch.setattr(ConfigLoader, "get_data_forward_policy", lambda: DataForwardPolicy(True, 4))
    monkeypatch.setattr(ConfigLoader, "get_report_cutoff_policy", lambda: ReportCutoffPolicy())
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    monkeypatch.setattr(ConfigLoader, "get_spc_point_decoration_policy", lambda: None)
    monkeypatch.setattr(ConfigLoader, "get_spc_capability_param_exemptions", lambda: ["PPA"])
    monkeypatch.setattr(ConfigLoader, "get_spc_period_sigma_source", lambda: "point_value")
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda *args, **kwargs: tmp_path / "decisions.xlsx")
    monkeypatch.setattr(pd, "read_sql", lambda *args, **kwargs: pytest.fail("Offline source queried a database"))


def _snapshot(tmp_path, frame, *, start="2026-10-01", end="2026-11-17"):
    path = tmp_path / "inline_measurements_P.parquet"
    frame.to_parquet(path, index=False)
    policy = InlineMeasurementSnapshotRepository.SNAPSHOT_POLICY_VERSION
    path.with_suffix(".policy").write_text(policy, encoding="utf-8")
    stat = path.stat()
    path.with_suffix(".snapshot.json").write_text(json.dumps({
        "covered_from": start, "covered_through": end, "policy_version": policy,
        "refreshed_at": "2026-11-17T00:00:00+00:00",
        "snapshot_mtime_ns": stat.st_mtime_ns, "snapshot_size_bytes": stat.st_size,
    }), encoding="utf-8")
    return path


def _frames():
    # Source Nov1/4 become display Nov5/8: both belong to ISO W45.
    facts = pd.DataFrame([
        dict(prod_code="P", factory="ARRAY", start_time=pd.Timestamp(day), sheet_id=f"S{i}",
             lot_id="L1", step_id="100", param_name=parameter, site_name="1", unit_id="EQ1", param_value=value)
        for i, (day, value) in enumerate([("2026-11-01 09:00", 5), ("2026-11-04 09:00", 6)])
        for parameter in ("CD", "PPA_X", "UNKNOWN_SPEC")
    ])
    catalog = pd.DataFrame({"ref_param_name": ["CD", "PPA_X", "UNKNOWN_SPEC"], "data_type": ["SPC"] * 3})
    specs = pd.DataFrame([
        dict(prod_code="P", step_id="100", param_name=parameter, param_type="SPC", usl=10.,
             lsl=1. if parameter != "UNKNOWN_SPEC" else float("nan"), ucl=9., lcl=2., target=5.5,
             main_step_id="100", main_eqp_type="EQP")
        for parameter in ("CD", "PPA_X", "UNKNOWN_SPEC")
    ])
    return facts, {"catalog": catalog, "specs": specs}


def _source(tmp_path, metadata, **kwargs):
    decisions = SimpleNamespace(load_sheet_oos_decisions=lambda *args: pd.DataFrame())
    return SnapshotCapabilitySource(tmp_path, lambda product: metadata,
                                    decoration_port=decisions, outlier_rules=pd.DataFrame(), **kwargs)


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_weekly_total_uses_shifted_snapshot_final_pipeline_exemptions_and_specs(tmp_path, policies, metric):
    facts, metadata = _frames()
    original = _snapshot(tmp_path, facts)
    before = original.read_bytes()
    result = _source(tmp_path, metadata).read_weekly_totals(
        "P", ["2026-W45", "2026-W46"], metric=metric, as_of="2026-11-17",
    )
    assert result.to_dict("records") == [
        {"prod_code": "P", "period_label": "2026-W45", "total_projects": 1},
        {"prod_code": "P", "period_label": "2026-W46", "total_projects": 0},
    ]
    assert original.read_bytes() == before
    assert not (tmp_path / "decisions.xlsx").exists()
    pd.testing.assert_frame_equal(metadata["catalog"], _frames()[1]["catalog"])


def test_denominator_does_not_use_the_48_hour_alarm_span_gate(tmp_path, policies):
    facts, metadata = _frames()
    facts.loc[facts.sheet_id.eq("S1"), "start_time"] = pd.Timestamp("2026-11-01 10:00")
    _snapshot(tmp_path, facts)
    result = _source(tmp_path, metadata).read_weekly_totals("P", ["2026-W45"], metric="cpk", as_of="2026-11-17")
    assert result.total_projects.tolist() == [1]


def test_missing_coverage_is_an_error_not_a_zero_denominator(tmp_path, policies):
    facts, metadata = _frames()
    _snapshot(tmp_path, facts, start="2026-11-03")
    with pytest.raises(SnapshotCapabilityInputError, match="does not cover"):
        _source(tmp_path, metadata).read_weekly_totals("P", ["2026-W45"], metric="cpk", as_of="2026-11-17")


def test_snapshot_identity_mismatch_is_rejected(tmp_path, policies):
    facts, metadata = _frames()
    path = _snapshot(tmp_path, facts)
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(SnapshotCapabilityInputError, match="does not cover"):
        _source(tmp_path, metadata).read_weekly_totals("P", ["2026-W45"], metric="cpk", as_of="2026-11-17")


@pytest.mark.parametrize("missing", ["catalog", "specs"])
def test_missing_metadata_does_not_become_an_authoritative_zero(tmp_path, policies, missing):
    facts, metadata = _frames()
    _snapshot(tmp_path, facts)
    metadata[missing] = pd.DataFrame()
    with pytest.raises(SnapshotCapabilityInputError, match="metadata requires"):
        _source(tmp_path, metadata).read_weekly_totals("P", ["2026-W45"], metric="cpk", as_of="2026-11-17")


def test_z622_uses_normal_snapshot_and_capability_path(tmp_path, policies):
    facts, metadata = _frames()
    facts["prod_code"] = "Z622"
    metadata["specs"]["prod_code"] = "Z622"
    _snapshot(tmp_path, facts).rename(tmp_path / "inline_measurements_Z622.parquet")
    (tmp_path / "inline_measurements_P.policy").rename(tmp_path / "inline_measurements_Z622.policy")
    (tmp_path / "inline_measurements_P.snapshot.json").rename(tmp_path / "inline_measurements_Z622.snapshot.json")
    source = _source(tmp_path, metadata)
    with pytest.raises(SnapshotCapabilityInputError, match="completed"):
        source.read_weekly_totals("Z622", ["2026-W47"], metric="cpk", as_of="2026-11-17")
    result = source.read_weekly_totals("Z622", ["2026-W45"], metric="cpm", as_of="2026-11-17")
    assert result.total_projects.tolist() == [1]


def test_date_exclusion_is_applied_before_features(tmp_path, policies, monkeypatch):
    facts, metadata = _frames()
    _snapshot(tmp_path, facts)
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: (("ARRAY",), "2026-11-01", "2026-11-10"))
    result = _source(tmp_path, metadata).read_weekly_totals("P", ["2026-W45"], metric="cpk", as_of="2026-11-17")
    assert result.total_projects.tolist() == [0]


def _cached_source(tmp_path, monkeypatch, metadata):
    """Exercise production caching while isolating filesystem decision inputs."""
    class Provider:
        signature = "metadata-v1"
        calls = 0

        def source_signature(self, products):
            assert products == ["P"]
            return self.signature

        def __call__(self, product):
            self.calls += 1
            return metadata

    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(source_module._Preparation, "_apply_outlier_filters", lambda self, frame, product: frame.copy())
    from src.inline_domain.application.shared import sheet_oos_decoration_service
    monkeypatch.setattr(sheet_oos_decoration_service, "load_sheet_oos_decisions", lambda *args, **kwargs: pd.DataFrame())
    source_module._cached_totals.clear()
    provider = Provider()
    return SnapshotCapabilitySource(tmp_path, provider), provider


@pytest.mark.parametrize("sidecar", [".policy", ".snapshot.json"])
def test_warm_cache_rechecks_invalidated_snapshot_sidecars(tmp_path, policies, monkeypatch, sidecar):
    facts, metadata = _frames()
    path = _snapshot(tmp_path, facts)
    source, provider = _cached_source(tmp_path, monkeypatch, metadata)
    kwargs = dict(metric="cpk", as_of="2026-11-17")
    first = source.read_weekly_totals("P", ["2026-W45"], **kwargs)
    assert first.total_projects.tolist() == [1]
    first.loc[0, "total_projects"] = 999
    # The cached native frame is copied and the expensive pipeline is reused.
    assert source.read_weekly_totals("P", ["2026-W45"], **kwargs).total_projects.tolist() == [1]
    assert provider.calls == 1
    changed = path.with_suffix(sidecar)
    old_stat = changed.stat()
    if sidecar == ".policy":
        changed.write_text("unknown-snapshot-policy", encoding="utf-8")
    else:
        payload = json.loads(changed.read_text(encoding="utf-8"))
        payload["covered_from"] = "2026-11-03"
        changed.write_text(json.dumps(payload), encoding="utf-8")
    os.utime(changed, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 1_000_000))
    with pytest.raises(SnapshotCapabilityInputError, match="does not cover"):
        source.read_weekly_totals("P", ["2026-W45"], **kwargs)


def test_warm_cache_is_invalidated_by_metadata_generation(tmp_path, policies, monkeypatch):
    facts, metadata = _frames()
    _snapshot(tmp_path, facts)
    source, provider = _cached_source(tmp_path, monkeypatch, metadata)
    kwargs = dict(metric="cpk", as_of="2026-11-17")
    assert source.read_weekly_totals("P", ["2026-W45"], **kwargs).total_projects.tolist() == [1]
    assert source.read_weekly_totals("P", ["2026-W45"], **kwargs).total_projects.tolist() == [1]
    assert provider.calls == 1
    metadata["specs"]["lsl"] = float("nan")
    provider.signature = "metadata-v2"
    assert source.read_weekly_totals("P", ["2026-W45"], **kwargs).total_projects.tolist() == [0]
    assert provider.calls == 2


@pytest.mark.parametrize("injected", ["decoration_port", "outlier_rules"])
def test_injected_decisions_or_outlier_rules_bypass_production_cache(tmp_path, policies, monkeypatch, injected):
    facts, metadata = _frames()
    _snapshot(tmp_path, facts)
    source, provider = _cached_source(tmp_path, monkeypatch, metadata)
    # Even a signed provider must not cache caller-owned injected policy inputs.
    if injected == "decoration_port":
        source.decoration_port = SimpleNamespace(load_sheet_oos_decisions=lambda *args: pd.DataFrame())
    else:
        source.outlier_rules = pd.DataFrame()
    kwargs = dict(metric="cpk", as_of="2026-11-17")
    assert source.read_weekly_totals("P", ["2026-W45"], **kwargs).total_projects.tolist() == [1]
    metadata["specs"]["lsl"] = float("nan")
    assert source.read_weekly_totals("P", ["2026-W45"], **kwargs).total_projects.tolist() == [0]
    assert provider.calls == 2
