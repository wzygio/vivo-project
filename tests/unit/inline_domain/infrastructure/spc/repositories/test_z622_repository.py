"""Z622 source contracts, isolated from production databases and workbooks."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.infrastructure.shared.measurement_data_loader import (
    RAW_MEASUREMENT_COLUMNS,
)
from src.inline_domain.infrastructure.shared.measurement_metadata_loader import SPEC_COLUMNS
from src.inline_domain.infrastructure.shared.measurement_preparation import (
    InlineMeasurementPreparationRepository,
)
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import (
    InlineMeasurementSnapshotRepository,
    measurement_snapshot_coverage,
)
from src.inline_domain.infrastructure.shared import rolling_snapshot
from src.inline_domain.infrastructure.spc import spc_repository as module
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy


class _Connection:
    def __init__(self):
        self.options = {}
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execution_options(self, **kwargs):
        self.options.update(kwargs)
        return self

    def begin(self):
        return self

    def exec_driver_sql(self, statement, *args, **kwargs):
        self.statements.append(statement)
        return None


def _database():
    connection = _Connection()
    return SimpleNamespace(engine=SimpleNamespace(connect=lambda: connection), connection=connection)


def _monitoring() -> pd.DataFrame:
    return pd.DataFrame([
        {"factory": "ARRAY", "step_id": "11140", "param_name": "SE_L2T", "data_type": "SPC"},
        {"factory": "ARRAY", "step_id": "12200", "param_name": "CD1", "data_type": "SPC"},
    ])


@pytest.fixture
def monitoring(tmp_path, monkeypatch):
    path = tmp_path / "z622_monitoring.xlsx"
    _monitoring().to_excel(path, sheet_name="monitoring", index=False)

    def resource(domain, key, *args, **kwargs):
        assert domain == "inline_domain"
        return {"spc_z622_monitoring": path,
                "spc_outlier_filters": tmp_path / "spc_outlier_filters.xlsx"}[key]

    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", resource)
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {
        "spc": {"z622_pds": {"lot_ids": ["L3ZB6BE03", "L3ZB6BE04"]}},
    })
    monkeypatch.setattr(ConfigLoader, "get_data_forward_policy", lambda: DataForwardPolicy())
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    monkeypatch.setattr(ConfigLoader, "get_snapshot_ttl_hours", lambda: 12)
    monkeypatch.setattr(ConfigLoader, "get_incremental_refresh_days", lambda: 7)
    return path


def _facts(time="2026-11-11 10:00:00") -> pd.DataFrame:
    return pd.DataFrame([
        {"factory": "ARRAY", "prod_code": "Z622", "start_time": pd.Timestamp(time),
         "sheet_id": "L3ZB6BE0301", "lot_id": "L3ZB6BE03", "step_id": "11140",
         "param_name": "SE_L2T", "site_name": "88", "unit_id": "ARRAY-EQ1",
         "param_value": 3.0},
    ], columns=RAW_MEASUREMENT_COLUMNS)


def _record_queries(monkeypatch, frames=None):
    queries = []

    def read_sql(query, connection, *, params, chunksize=None, **kwargs):
        queries.append((str(query), dict(params), chunksize))
        if "dwd_imp_dv_param_spec" in str(query):
            return _monitoring()[["step_id", "param_name"]].assign(
                prod_code="Z622", param_type="SPC", usl=10., lsl=1., ucl=9., lcl=2.,
                target=5.5, main_step_id=None, main_eqp_type=None,
            ).reindex(columns=SPEC_COLUMNS)
        result = [frame.copy() for frame in (frames or [pd.DataFrame(columns=RAW_MEASUREMENT_COLUMNS)])]
        return iter(result) if chunksize else pd.concat(result, ignore_index=True)

    monkeypatch.setattr(pd, "read_sql", read_sql)
    return queries


def test_pds_query_is_partition_bounded_lot_scoped_and_streamed(monitoring, monkeypatch):
    calls = _record_queries(monkeypatch)
    database = _database()

    result = module.load_z622_measurements(
        database, "2026-10-01", "2026-10-03", "Z622",
    )

    assert list(result.columns) == RAW_MEASUREMENT_COLUMNS
    assert result.empty
    assert len(calls) == 3
    assert database.connection.options["postgresql_readonly"] is True
    assert any("statement_timeout" in sql.lower() for sql in database.connection.statements)
    intervals = []
    for sql, params, chunksize in calls:
        assert "array_pds_result_t" in sql.lower()
        assert re.search(r"glass_start_time\s*>=\s*:\w+", sql, re.I)
        assert re.search(r"glass_start_time\s*<\s*:\w+", sql, re.I)
        prefixes = re.findall(r"glass_id\s+LIKE\s*:(\w+)", sql, re.I)
        assert [params[key] for key in prefixes] == ["L3ZB6BE03%", "L3ZB6BE04%"]
        assert "ORDER BY" not in sql.upper()
        assert "LIMIT" not in sql.upper()
        assert chunksize == 50000
        lower = re.search(r"glass_start_time\s*>=\s*:(\w+)", sql, re.I).group(1)
        upper = re.search(r"glass_start_time\s*<\s*:(\w+)", sql, re.I).group(1)
        start, end = pd.Timestamp(params[lower]), pd.Timestamp(params[upper])
        assert pd.Timedelta(0) < end - start <= pd.Timedelta(days=1)
        intervals.append((start, end))
    assert intervals[0][0] == pd.Timestamp("2026-10-01")
    assert intervals[-1][1] == pd.Timestamp("2026-10-04")
    assert all(left[1] == right[0] for left, right in zip(intervals, intervals[1:]))


def test_pds_predicates_keep_step_parameter_pairs_together(monitoring, monkeypatch):
    calls = _record_queries(monkeypatch)
    module.load_z622_measurements(
        _database(), "2026-11-11", "2026-11-11", "Z622",
    )

    sql, params, _ = calls[0]
    pairs = re.findall(
        r"step_id\s*=\s*:(\w+)\s+AND\s+(?:\w+\.)?param_name\s*=\s*:(\w+)",
        sql, re.I,
    )
    assert {(params[step], params[param]) for step, param in pairs} == {
        ("11140", "SE_L2T"), ("12200", "CD1"),
    }


def test_raw_loader_preserves_retests_for_shared_preparation(monitoring, monkeypatch):
    old = _facts("2026-11-11 09:00:00")
    latest = _facts("2026-11-11 12:00:00").assign(param_value=10.0)
    _record_queries(monkeypatch, [old, latest])

    result = module.load_z622_measurements(
        _database(), "2026-11-11", "2026-11-11", "Z622",
    )

    assert list(result.columns) == RAW_MEASUREMENT_COLUMNS
    assert result.param_value.tolist() == [3.0, 10.0]
    assert result.start_time.tolist() == [pd.Timestamp("2026-11-11 09:00:00"), pd.Timestamp("2026-11-11 12:00:00")]
    assert result.sheet_id.tolist() == ["L3ZB6BE0301", "L3ZB6BE0301"]
    assert result.lot_id.eq("L3ZB6BE03").all()
    assert result.prod_code.eq("Z622").all()
    assert result.factory.eq("ARRAY").all()


def test_z622_loader_rejects_other_products_before_query(monitoring, monkeypatch):
    calls = _record_queries(monkeypatch)
    with pytest.raises(ValueError):
        module.load_z622_measurements(
            _database(), "2026-11-11", "2026-11-11", "M626",
        )
    assert calls == []


def test_pds_result_rejects_crossed_pairs_foreign_lots_and_outside_times(monitoring, monkeypatch):
    valid = _facts()
    second_lot = valid.assign(sheet_id="L3ZB6BE0401")
    result_rows = pd.concat([
        valid,
        second_lot,
        valid.assign(param_name="CD1"),  # Allowed parameter, wrong station.
        valid.assign(step_id="12200"),  # Allowed station, wrong parameter.
        valid.assign(sheet_id="ANOTHERLOT01"),
        valid.assign(start_time=pd.Timestamp("2026-11-10 23:59:59")),
        valid.assign(start_time=pd.Timestamp("2026-11-12 00:00:00")),
    ], ignore_index=True)
    _record_queries(monkeypatch, [result_rows])

    result = module.load_z622_measurements(
        _database(), "2026-11-11", "2026-11-11", "Z622",
    )

    expected = pd.concat([valid, second_lot.assign(lot_id="L3ZB6BE04")], ignore_index=True)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize("lot_ids", [None, [], "L3ZB6BE03", ["L3ZB6BE0_"], ["L3ZB6BE03", "L3ZB6BE03"]])
def test_invalid_configured_lots_fail_before_pds_query(monitoring, monkeypatch, lot_ids):
    monkeypatch.setattr(ConfigLoader, "load_domain_config", lambda *_: {
        "spc": {"z622_pds": {"lot_ids": lot_ids}},
    })
    calls = _record_queries(monkeypatch)

    with pytest.raises(ValueError, match=r"spc\.z622_pds\.lot_ids"):
        module.load_z622_measurements(_database(), "2026-11-11", "2026-11-11", "Z622")
    assert calls == []


def test_shared_preparation_keeps_latest_per_complete_point_identity(monitoring, monkeypatch):
    _record_queries(monkeypatch)
    old = _facts("2026-11-11 09:00:00")
    latest = _facts("2026-11-11 12:00:00").assign(param_value=10.0)
    another_parameter = latest.assign(step_id="12200", param_name="CD1", param_value=20.0)
    raw = pd.concat([old, latest, another_parameter], ignore_index=True)
    monkeypatch.setattr(
        InlineMeasurementPreparationRepository, "_apply_outlier_filters",
        lambda _self, frame, _product: frame,
    )
    preparation = InlineMeasurementPreparationRepository(
        raw_measurements=SimpleNamespace(get_measurements=lambda *_args: raw.copy()),
        metadata=module.build_z622_metadata_repository(_database()),
        main_process_history=SimpleNamespace(get_main_process_history=lambda *_args, **_kwargs: pd.DataFrame()),
    )

    result = module.SpcRepository(preparation).get_spc_measurements(SpcQueryConfig(
        prod_code="Z622", start_date="2026-11-11", end_date="2026-11-11", data_type_filter="SPC",
    ))

    assert len(result) == 2
    assert set(zip(result.step_id, result.param_name, result.param_value)) == {
        ("11140", "SE_L2T", 10.0), ("12200", "CD1", 20.0),
    }
    assert result.sheet_start_time.eq(pd.Timestamp("2026-11-11 12:00:00")).all()
    assert len(raw) == 3


def test_z622_loader_does_not_scan_before_source_start(monitoring, monkeypatch):
    calls = _record_queries(monkeypatch)
    result = module.load_z622_measurements(
        _database(), "2026-09-01", "2026-09-30", "Z622",
    )
    assert result.empty
    assert calls == []


@pytest.mark.parametrize("problem", ["missing_file", "wrong_sheet", "missing_column", "empty", "duplicate", "unknown_factory", "blank_step", "blank_parameter", "unknown_type"])
def test_invalid_monitoring_is_an_error_before_pds_query(monitoring, monkeypatch, problem):
    frame = _monitoring()
    if problem == "missing_file":
        monitoring.unlink()
    else:
        if problem == "missing_column":
            frame = frame.drop(columns="step_id")
        elif problem == "empty":
            frame = frame.iloc[:0]
        elif problem == "duplicate":
            frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
        elif problem == "unknown_factory":
            frame.loc[0, "factory"] = "UNKNOWN"
        elif problem == "blank_step":
            frame.loc[0, "step_id"] = None
        elif problem == "blank_parameter":
            frame.loc[0, "param_name"] = " "
        elif problem == "unknown_type":
            frame.loc[0, "data_type"] = "UNKNOWN"
        frame.to_excel(monitoring, sheet_name="other" if problem == "wrong_sheet" else "monitoring", index=False)
    calls = _record_queries(monkeypatch)

    with pytest.raises((ValueError, FileNotFoundError)):
        module.load_z622_measurements(
            _database(), "2026-11-11", "2026-11-11", "Z622",
        )
    assert calls == []


def test_metadata_uses_monitoring_catalog_and_native_product_specifications(monitoring, monkeypatch):
    calls = _record_queries(monkeypatch)
    metadata = module.build_z622_metadata_repository(_database())

    catalog = metadata.get_parameter_catalog("Z622")
    specs = metadata.get_parameter_specs("Z622")

    assert set(zip(catalog.ref_param_name, catalog.data_type)) == {("SE_L2T", "SPC"), ("CD1", "SPC")}
    assert set(SPEC_COLUMNS).issubset(specs.columns)
    assert set(zip(specs.step_id, specs.param_name)) == {("11140", "SE_L2T"), ("12200", "CD1")}
    assert specs.prod_code.eq("Z622").all()
    assert specs.usl.eq(10.).all() and specs.lsl.eq(1.).all()
    assert specs.main_step_id.tolist() == specs.step_id.tolist()
    assert specs.main_eqp_type.eq("EQP").all()
    assert len(calls) == 1
    assert "interface_time <= :known_before" in calls[0][0]
    assert calls[0][1]["prod_code"] == "Z622"


def test_enabling_analysis_changes_cache_key_without_changing_raw_source(monitoring, monkeypatch, tmp_path):
    old = module.spc_input_signature(tmp_path, "Z622")
    raw_signature = module._z622_source_signature()
    monkeypatch.setattr(module, "Z622_ANALYSIS_POLICY", "future-analysis-version")
    assert module.spc_input_signature(tmp_path, "Z622") != old
    assert module._z622_source_signature() == raw_signature


def test_z622_spec_failure_is_not_replaced_by_empty_limits(monitoring, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("specifications unavailable")
    monkeypatch.setattr(pd, "read_sql", fail)
    with pytest.raises(OSError):
        module.build_z622_metadata_repository(_database()).get_parameter_specs("Z622")


def test_z622_snapshot_reuse_force_refresh_and_failed_refresh_preserve_source_time(monitoring, monkeypatch, tmp_path):
    calls = _record_queries(monkeypatch, [_facts()])
    monkeypatch.setattr(ConfigLoader, "get_data_forward_policy", lambda: DataForwardPolicy(enabled=True, offset_days=4))
    directory = tmp_path / "snapshots"
    repository = module.build_z622_measurement_repository(_database(), snapshot_dir=directory)
    assert isinstance(repository, InlineMeasurementSnapshotRepository)

    first = repository.get_measurements("Z622", "2026-11-11")
    assert not first.empty
    assert first.start_time.eq(pd.Timestamp("2026-11-15 10:00:00")).all()
    path = directory / "inline_measurements_Z622.parquet"
    assert pd.read_parquet(path).start_time.eq(pd.Timestamp("2026-11-11 10:00:00")).all()
    initial_queries = len(calls)
    second = repository.get_measurements("Z622", "2026-11-11")
    assert len(calls) == initial_queries
    pd.testing.assert_frame_equal(first, second)
    proof = measurement_snapshot_coverage(path, ConfigLoader.get_data_forward_policy())
    assert proof == first.attrs["spc_source_coverage"]
    assert proof is not None
    assert json.loads(path.with_suffix(".snapshot.json").read_text())["z622_source_signature"]

    refreshed = repository.refresh_measurements("Z622", "2026-11-11")
    assert refreshed.refreshed_from_db
    assert len(calls) > initial_queries

    def unavailable(*args, **kwargs):
        raise OSError("source unavailable")

    monkeypatch.setattr(pd, "read_sql", unavailable)
    fallback = repository.refresh_measurements("Z622", "2026-11-11")
    assert not fallback.refreshed_from_db
    pd.testing.assert_frame_equal(fallback.measurements, refreshed.measurements)


@pytest.mark.parametrize("new_instance", [False, True])
@pytest.mark.parametrize("change", ["monitoring", "lot_ids"])
def test_z622_changed_configuration_cannot_reuse_or_fallback_to_old_source(
    monitoring, monkeypatch, tmp_path, new_instance, change,
):
    _record_queries(monkeypatch, [_facts()])
    repository = module.build_z622_measurement_repository(_database(), snapshot_dir=tmp_path / "snapshots")
    repository.get_measurements("Z622", "2026-11-11")
    if change == "monitoring":
        updated = _monitoring()
        updated.loc[0, "param_name"] = "DIFFERENT_PARAM"
        updated.to_excel(monitoring, sheet_name="monitoring", index=False)
    else:
        monkeypatch.setattr(ConfigLoader, "get_z622_pds_lot_ids", lambda: ("L3ZB6BE04",))

    def unavailable(*args, **kwargs):
        raise OSError("source unavailable")

    monkeypatch.setattr(pd, "read_sql", unavailable)
    changed = (
        module.build_z622_measurement_repository(_database(), snapshot_dir=tmp_path / "snapshots")
        if new_instance else repository
    )

    with pytest.raises(RuntimeError):
        changed.get_measurements("Z622", "2026-11-11")


def _generation_files(directory):
    path = directory / "inline_measurements_Z622.parquet"
    return [path, path.with_suffix(".policy"), path.with_suffix(".snapshot.json")]


def _generation_state(directory):
    return {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in _generation_files(directory)
    }


def test_failed_source_manifest_publication_restores_entire_old_generation(monitoring, monkeypatch, tmp_path):
    _record_queries(monkeypatch, [_facts()])
    directory = tmp_path / "snapshots"
    repository = module.build_z622_measurement_repository(_database(), snapshot_dir=directory)
    original = repository.get_measurements("Z622", "2026-11-11")
    before = _generation_state(directory)
    manifest = _generation_files(directory)[-1]
    replace = os.replace

    def failed_manifest(source, target):
        if Path(target) == manifest:
            raise OSError("source manifest publication interrupted")
        return replace(source, target)

    _record_queries(monkeypatch, [_facts().assign(param_value=12345.0)])
    with monkeypatch.context() as failure:
        failure.setattr(rolling_snapshot.os, "replace", failed_manifest)
        with pytest.raises(OSError, match="manifest publication interrupted"):
            repository.refresh_measurements("Z622", "2026-11-11")

    assert _generation_state(directory) == before
    assert not list(directory.glob("*.tmp"))
    restored = repository.get_measurements("Z622", "2026-11-11")
    pd.testing.assert_frame_equal(restored, original)
    assert restored.attrs["spc_source_coverage"] == original.attrs["spc_source_coverage"]


def test_configuration_changed_after_read_cannot_relabel_or_replace_old_generation(monitoring, monkeypatch, tmp_path):
    _record_queries(monkeypatch, [_facts()])
    directory = tmp_path / "snapshots"
    repository = module.build_z622_measurement_repository(_database(), snapshot_dir=directory)
    original = repository.get_measurements("Z622", "2026-11-11")
    before = _generation_state(directory)
    original_signature = json.loads(_generation_files(directory)[-1].read_text())["z622_source_signature"]

    def changed_after_load(frame):
        # The loader already completed its final configuration check. The
        # correction seam is the next real step before snapshot publication.
        assert frame.attrs["z622_source_signature"] == original_signature
        updated = _monitoring()
        updated.loc[0, "param_name"] = "DIFFERENT_PARAM"
        updated.to_excel(monitoring, sheet_name="monitoring", index=False)
        return frame.copy()

    repository.measurement_corrector = changed_after_load
    with pytest.raises(ValueError, match="configuration changed"):
        repository.refresh_measurements("Z622", "2026-11-11")

    assert _generation_state(directory) == before
    assert not list(directory.glob("*.tmp"))
    assert json.loads(_generation_files(directory)[-1].read_text())["z622_source_signature"] == original_signature
    _monitoring().to_excel(monitoring, sheet_name="monitoring", index=False)
    restored = repository.get_measurements("Z622", "2026-11-11")
    pd.testing.assert_frame_equal(restored, original)
    assert restored.attrs["spc_source_coverage"] == original.attrs["spc_source_coverage"]


def test_incremental_tail_merge_preserves_bound_signature_and_old_history(monitoring, monkeypatch, tmp_path):
    history = _facts("2026-11-01 10:00:00").assign(sheet_id="L3ZB6BE0300", param_value=1.0)
    recent = _facts("2026-11-11 10:00:00").assign(param_value=2.0)
    _record_queries(monkeypatch, [pd.concat([history, recent], ignore_index=True)])
    directory = tmp_path / "snapshots"
    repository = module.build_z622_measurement_repository(_database(), snapshot_dir=directory)
    repository.get_measurements("Z622", "2026-11-11")
    first_metadata = json.loads(_generation_files(directory)[-1].read_text())
    new = _facts("2026-11-12 10:00:00").assign(sheet_id="L3ZB6BE0302", param_value=3.0)
    calls = _record_queries(monkeypatch, [new])

    result = repository.get_measurements("Z622", "2026-11-12")

    assert calls
    # The removed recent record is inside the refreshed tail; historical points
    # outside that tail survive. Both branches must keep the source binding.
    assert result.param_value.tolist() == [1.0, 3.0]
    assert result.sheet_id.tolist() == ["L3ZB6BE0300", "L3ZB6BE0302"]
    signature = module._z622_source_signature()
    assert result.attrs["z622_source_signature"] == signature
    metadata = json.loads(_generation_files(directory)[-1].read_text())
    assert metadata["z622_source_signature"] == first_metadata["z622_source_signature"] == signature
    assert metadata["covered_from"] == first_metadata["covered_from"]
    assert metadata["covered_through"] == "2026-11-12"
    assert metadata != first_metadata
    assert measurement_snapshot_coverage(_generation_files(directory)[0], DataForwardPolicy()) == result.attrs["spc_source_coverage"]


def test_composition_routes_z622_refresh_and_preserves_legacy_loader(monitoring, monkeypatch, tmp_path):
    from src.inline_domain import composition
    from src.inline_domain.infrastructure.shared.measurement_data_loader import load_raw_measurements

    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    database = _database()
    calls = _record_queries(monkeypatch, [_facts()])
    routed = composition.build_raw_measurement_repository(database, "Z622")
    legacy = composition.build_raw_measurement_repository(database, "M626")
    assert routed.measurement_loader is module.load_z622_measurements
    assert legacy.measurement_loader is load_raw_measurements
    assert type(legacy) is InlineMeasurementSnapshotRepository

    assert composition.refresh_raw_measurements(database, "Z622", "2026-11-11") is True
    assert calls and all("array_pds_result_t" in sql.lower() for sql, _, _ in calls)
    directory = routed.snapshot_dir
    original = _generation_state(directory)

    def unavailable(*args, **kwargs):
        raise OSError("PDS source unavailable")

    monkeypatch.setattr(pd, "read_sql", unavailable)
    assert composition.refresh_raw_measurements(database, "Z622", "2026-11-11") is False
    assert _generation_state(directory) == original


def test_composed_preparation_get_spec_limits_uses_native_z622_limits(monitoring, monkeypatch, tmp_path):
    from src.inline_domain import composition

    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    calls = _record_queries(monkeypatch)
    repository = composition.build_spc_repository(_database(), "Z622")

    specs = repository.get_spc_spec_limits("Z622")

    assert not specs.empty
    assert specs.prod_code.eq("Z622").all()
    assert set(zip(specs.step_id, specs.param_name)) == {("11140", "SE_L2T"), ("12200", "CD1")}
    assert specs.usl.eq(10.).all() and specs.lsl.eq(1.).all()
    assert len(calls) == 1


def test_encrypted_monitoring_reader_cleans_outputs_and_uses_logical_source_identity(monitoring, monkeypatch, tmp_path):
    decrypted_files = []
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(module, "is_encrypted_file", lambda path: True)

    def decrypt(source, *, output_dir, strategy):
        assert Path(source) == monitoring
        assert output_dir == tmp_path / "output" / "decrypted_files"
        assert strategy == "excel-values"
        path = tmp_path / f"decrypted-{len(decrypted_files)}.xlsx"
        # Both source containers represent the same pairs, even if Excel saves
        # a different row order and non-policy provenance on the second open.
        frame = _monitoring()
        if decrypted_files:
            frame = frame.iloc[::-1].assign(extraction_note="updated provenance")
        with pd.ExcelWriter(path) as writer:
            frame.to_excel(writer, sheet_name="monitoring", index=False)
            pd.DataFrame({"revision": [len(decrypted_files)]}).to_excel(writer, sheet_name="provenance", index=False)
        decrypted_files.append(path)
        return SimpleNamespace(output_path=path)

    monkeypatch.setattr(module, "decrypt_file", decrypt)
    before = module._z622_source_signature()
    assert len(decrypted_files) == 1
    assert not decrypted_files[0].exists()
    # Simulate a new enterprise-encrypted container for unchanged logical data.
    monitoring.write_bytes(b"changed-enterprise-encrypted-container-bytes")

    after = module._z622_source_signature()

    assert after == before
    assert len(decrypted_files) == 2
    assert all(not path.exists() for path in decrypted_files)
    catalog = module.build_z622_metadata_repository(_database()).get_parameter_catalog("Z622")
    assert set(catalog.ref_param_name) == {"SE_L2T", "CD1"}
    assert len(decrypted_files) == 2  # Validated parse reused for this file version.


def test_decryption_failure_is_explicit_without_database_or_old_source_fallback(monitoring, monkeypatch, tmp_path):
    _record_queries(monkeypatch, [_facts()])
    repository = module.build_z622_measurement_repository(_database(), snapshot_dir=tmp_path / "snapshots")
    repository.get_measurements("Z622", "2026-11-11")
    original = _generation_state(repository.snapshot_dir)
    calls = _record_queries(monkeypatch)
    monkeypatch.setattr(module, "is_encrypted_file", lambda path: True)

    def decrypt(source, *, output_dir, strategy):
        assert strategy == "excel-values"
        raise PermissionError("monitoring workbook decryption denied")

    monkeypatch.setattr(module, "decrypt_file", decrypt)
    monitoring.write_bytes(b"unreadable-new-encrypted-workbook")

    with pytest.raises(PermissionError, match="decryption denied"):
        repository.get_measurements("Z622", "2026-11-11")
    assert calls == []
    assert _generation_state(repository.snapshot_dir) == original

