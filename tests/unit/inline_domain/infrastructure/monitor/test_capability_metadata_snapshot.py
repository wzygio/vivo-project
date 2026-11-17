"""Metadata reads are bounded, temporal, read-only and committed as one generation."""

from contextlib import nullcontext

import pandas as pd
import pytest

from src.inline_domain.infrastructure.monitor import capability_metadata_snapshot as module


class Connection:
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
        return nullcontext()

    def exec_driver_sql(self, statement):
        self.statements.append(statement)


class Engine:
    def __init__(self):
        self.connection = Connection()

    def connect(self):
        return self.connection


@pytest.fixture
def captures(monkeypatch):
    calls = []

    def read_sql(query, connection, params=None):
        sql = str(query)
        calls.append((sql, params))
        if "information_schema.columns" in sql:
            return pd.DataFrame([
                dict(table_schema=schema, table_name=table, column_name=column, data_type="timestamp without time zone")
                for (schema, table), columns in module.RELATIONS.items() for column in columns
            ])
        if "pg_class" in sql:
            return pd.DataFrame([
                dict(table_schema=schema, table_name=table, relkind="r", estimated_rows=1000., estimated_bytes=100_000)
                for schema, table in module.RELATIONS
            ])
        if "parameter.parmtername" in sql:
            return pd.DataFrame([dict(prod_code="P", ref_param_name="cd", data_type="SPC")])
        return pd.DataFrame([dict(prod_code="P", step_id="100", param_name="CD", param_type="SPC",
                                  usl=10., lsl=1., ucl=9., lcl=2., target=5.5,
                                  main_step_id=None, main_eqp_type=None)])

    monkeypatch.setattr(pd, "read_sql", read_sql)
    return calls


def test_capture_is_guarded_and_temporal_before_publish(tmp_path, captures):
    engine = Engine()
    result = module.capture_capability_metadata(engine, ["P"], directory=tmp_path, as_of="2026-11-17 12:00")
    assert engine.connection.options["postgresql_readonly"] is True
    assert engine.connection.statements == ["SET LOCAL statement_timeout = '30s'"]
    assert "information_schema.columns" in captures[0][0]
    assert "pg_class" in captures[1][0]
    assert len(captures) == 4
    assert result["read_only"] is True
    assert set(result["products"]) == {"P"}
    for sql, params in captures[2:]:
        assert params["products"] == ["P"]
        assert params["as_of"] == pd.Timestamp("2026-11-17 12:00").to_pydatetime()
        assert "< :as_of" in sql
    assert "parameter.update_time" in captures[2][0]
    assert "product.interface_time" in captures[2][0]
    assert "product.checkouttime" in captures[2][0]
    assert "product.createtime" in captures[2][0]
    assert "interface_time < :as_of" in captures[3][0]
    provider = module.CapabilityMetadataSnapshot(tmp_path)
    data = provider("P")
    assert data["catalog"].ref_param_name.tolist() == ["CD"]
    assert data["specs"].main_step_id.tolist() == ["100"]
    assert data["specs"].main_eqp_type.tolist() == ["EQP"]
    data["specs"].loc[0, "usl"] = 99
    assert provider("P")["specs"].usl.tolist() == [10.]


def test_z622_capture_includes_native_specs_and_pds_catalog(tmp_path, captures, monkeypatch):
    from types import SimpleNamespace
    from src.inline_domain.infrastructure.spc import spc_repository

    original = pd.read_sql
    def read_sql(query, connection, params=None):
        frame = original(query, connection, params=params)
        if "prod_code" in frame:
            frame["prod_code"] = "Z622"
        return frame
    monkeypatch.setattr(pd, "read_sql", read_sql)
    monkeypatch.setattr(spc_repository, "build_z622_metadata_repository", lambda: SimpleNamespace(
        get_parameter_catalog=lambda product: pd.DataFrame({"ref_param_name": ["PDS_CD"], "data_type": ["SPC"]})))
    result = module.capture_capability_metadata(Engine(), ["Z622"], directory=tmp_path)
    assert set(result["products"]) == {"Z622"}
    data = module.CapabilityMetadataSnapshot(tmp_path)("Z622")
    assert data["catalog"].ref_param_name.tolist() == ["PDS_CD"]
    assert data["specs"].usl.tolist() == [10.]


def test_new_capture_changes_signature_and_keeps_prior_generation(tmp_path, captures):
    first = module.capture_capability_metadata(Engine(), ["P"], directory=tmp_path, as_of="2026-11-17")
    provider = module.CapabilityMetadataSnapshot(tmp_path)
    old_signature = provider.source_signature(["P"])
    second = module.capture_capability_metadata(Engine(), ["P"], directory=tmp_path, as_of="2026-11-18")
    assert provider.source_signature(["P"]) != old_signature
    assert (tmp_path / first["generation"] / "manifest.json").exists()
    assert second["generation"] != first["generation"]


def test_corrupt_metadata_is_unavailable_not_empty(tmp_path, captures):
    result = module.capture_capability_metadata(Engine(), ["P"], directory=tmp_path)
    entry = result["products"]["P"]["specs"]
    path = tmp_path / result["generation"] / entry["filename"]
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(module.CapabilityMetadataSnapshotError, match="unavailable"):
        module.CapabilityMetadataSnapshot(tmp_path)("P")


def test_guard_failure_never_replaces_prior_generation(tmp_path, captures, monkeypatch):
    module.capture_capability_metadata(Engine(), ["P"], directory=tmp_path)
    old_pointer = (tmp_path / "current.json").read_bytes()
    monkeypatch.setattr(module, "_guard_sources", lambda connection: (_ for _ in ()).throw(module.CapabilityMetadataSnapshotError("bad volume")))
    with pytest.raises(module.CapabilityMetadataSnapshotError, match="bad volume"):
        module.capture_capability_metadata(Engine(), ["P"], directory=tmp_path)
    assert (tmp_path / "current.json").read_bytes() == old_pointer


@pytest.mark.parametrize("kind", ["catalog", "specs"])
@pytest.mark.parametrize("mutation", ["delete", "corrupt"])
def test_signature_checks_referenced_files_even_when_manifest_is_unchanged(tmp_path, captures, kind, mutation):
    result = module.capture_capability_metadata(Engine(), ["P"], directory=tmp_path)
    provider = module.CapabilityMetadataSnapshot(tmp_path)
    provider.source_signature(["P"])
    pointer_before = (tmp_path / "current.json").read_bytes()
    manifest_path = tmp_path / result["generation"] / "manifest.json"
    manifest_before = manifest_path.read_bytes()
    path = tmp_path / result["generation"] / result["products"]["P"][kind]["filename"]
    if mutation == "delete":
        path.unlink()
    else:
        path.write_bytes(path.read_bytes() + b"changed")
    assert (tmp_path / "current.json").read_bytes() == pointer_before
    assert manifest_path.read_bytes() == manifest_before
    with pytest.raises(module.CapabilityMetadataSnapshotError, match="signature unavailable"):
        provider.source_signature(["P"])


def test_signature_rejects_missing_requested_products_instead_of_hashing_null(tmp_path, captures):
    module.capture_capability_metadata(Engine(), ["P"], directory=tmp_path)
    provider = module.CapabilityMetadataSnapshot(tmp_path)
    assert provider.source_signature() == provider.source_signature(["P"])
    with pytest.raises(module.CapabilityMetadataSnapshotError, match="signature unavailable"):
        provider.source_signature(["MISSING"])
    with pytest.raises(module.CapabilityMetadataSnapshotError, match="signature unavailable"):
        provider.source_signature(["P", "MISSING"])


@pytest.mark.parametrize("kind,mutation", [("catalog", "delete"), ("specs", "corrupt")])
def test_warm_totals_cache_cannot_bypass_metadata_file_integrity(
    tmp_path, captures, monkeypatch, kind, mutation,
):
    from src.inline_domain.infrastructure.monitor import capability_snapshot_source as source_module

    directory = tmp_path / "metadata"
    generation = module.capture_capability_metadata(Engine(), ["P"], directory=directory)
    provider = module.CapabilityMetadataSnapshot(directory)
    monkeypatch.setattr(source_module.ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(source_module.ConfigLoader, "get_domain_resource_path",
                        lambda *args, **kwargs: tmp_path / "isolated-input.xlsx")
    loads = []
    def totals(self, product, weeks, *, metric, as_of):
        loads.append(product)
        return pd.DataFrame([{"prod_code": product, "period_label": week, "total_projects": 5}
                             for week in weeks])
    monkeypatch.setattr(source_module.SnapshotCapabilitySource, "_read_weekly_totals", totals)
    source = source_module.SnapshotCapabilitySource(tmp_path / "facts", provider)
    source_module._cached_totals.clear()
    try:
        first = source.read_weekly_totals("P", ["2026-W46"], metric="cpk", as_of="2026-11-17")
        second = source.read_weekly_totals("P", ["2026-W46"], metric="cpk", as_of="2026-11-17")
        pd.testing.assert_frame_equal(first, second)
        assert loads == ["P"]
        entry = generation["products"]["P"][kind]
        path = directory / generation["generation"] / entry["filename"]
        if mutation == "delete":
            path.unlink()
        else:
            path.write_bytes(path.read_bytes() + b"changed")
        with pytest.raises(module.CapabilityMetadataSnapshotError, match="signature unavailable"):
            source.read_weekly_totals("P", ["2026-W46"], metric="cpk", as_of="2026-11-17")
        assert loads == ["P"]
    finally:
        source_module._cached_totals.clear()
