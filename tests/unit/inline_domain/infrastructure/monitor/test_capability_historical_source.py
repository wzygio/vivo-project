"""Authorized older-week replay cannot modify retained facts or broaden its reads."""
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.infrastructure.monitor import capability_historical_source as module
from src.inline_domain.infrastructure.monitor.capability_historical_source import (
    HistoricalCapabilitySource, capture_historical_capability_inputs,
)
from src.inline_domain.infrastructure.monitor.capability_snapshot_source import SnapshotCapabilityInputError
from src.inline_domain.infrastructure.shared.measurement_data_loader import RAW_MEASUREMENT_COLUMNS
from src.inline_domain.infrastructure.shared.rolling_snapshot import publish_snapshots
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy


@pytest.fixture
def policy(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_data_forward_policy", lambda: DataForwardPolicy(True, 4))


def facts(product, dates):
    return pd.DataFrame([dict(prod_code=product, factory="ARRAY", start_time=pd.Timestamp(day),
                              sheet_id=f"{product}-{index}", lot_id="L1", step_id="1", param_name="CD",
                              site_name="1", unit_id="EQ", param_value=1.) for index, day in enumerate(dates)],
                        columns=RAW_MEASUREMENT_COLUMNS)


def snapshot(directory, product, frame, start, end):
    path = directory / f"inline_measurements_{product}.parquet"
    publish_snapshots([(path, frame, module.POLICY, pd.Timestamp(end), True)], window_start=pd.Timestamp(start))
    return path


class Connection:
    def __init__(self):
        self.read_only = False

    def execution_options(self, **options):
        self.read_only = options["postgresql_readonly"]
        assert "stream_results" not in options
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def begin(self):
        return self

    def exec_driver_sql(self, statement):
        assert statement == "SET LOCAL statement_timeout = '30s'"


def test_capture_queries_only_missing_prefix_and_keeps_global_snapshot_bytes(tmp_path, monkeypatch, policy):
    current, output = tmp_path / "current", tmp_path / "historical"
    original = snapshot(current, "P", facts("P", ["2026-08-01", "2026-08-05"]), "2026-08-01", "2026-11-17")
    before = {path.name: path.read_bytes() for path in current.iterdir()}
    connection = Connection()
    monkeypatch.setattr(module, "_guard_sources", lambda _: [{"table": "guarded"}])
    calls = []

    def read(_connection, product, first, last, as_of):
        calls.append((product, str(first.date()), str(last.date())))
        return facts(product, ["2026-07-23", "2026-07-31"])

    monkeypatch.setattr(module, "_read_missing", read)
    result = capture_historical_capability_inputs(SimpleNamespace(connect=lambda: connection),
        snapshot_dir=current, output_dir=output, authorized_weeks={"P": ["2026-W31", "2026-W32"]}, as_of="2026-11-17")
    assert calls == [("P", "2026-07-23", "2026-07-31")]
    assert connection.read_only
    assert result["products"]["P"]["retained_rows"] == 2
    assert result["products"]["P"]["rows"] == 4
    assert {path.name: path.read_bytes() for path in current.iterdir()} == before
    assert pd.read_parquet(output / original.name).start_time.max() == pd.Timestamp("2026-08-05")


class Source:
    def __init__(self, directory):
        self.snapshot_dir = directory
        self.metadata_provider = lambda _: {}
        self.decoration_port = None
        self.outlier_rules = None
        self.calls = []

    def source_signature(self, products):
        return "current-verified"

    def read_weekly_totals(self, product, weeks, *, metric, as_of):
        self.calls.append((product, list(weeks), metric))
        return pd.DataFrame({"prod_code": [product] * len(weeks), "period_label": list(weeks),
                             "total_projects": [3] * len(weeks)})


def test_wrapper_routes_only_approved_missing_weeks_and_preserves_requested_order(tmp_path, policy):
    current, historical = tmp_path / "current", tmp_path / "historical"
    snapshot(current, "P", facts("P", []), "2026-08-01", "2026-11-17")
    path = snapshot(historical, "P", facts("P", []), "2026-07-23", "2026-08-05")
    import hashlib
    (historical / "capture.json").write_text(json.dumps({"version": 1, "read_only": True,
        "products": {"P": {"weeks": ["2026-W31", "2026-W32"], "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                           "base_snapshot": module._coverage(current, "P")}}}))
    base = Source(current)
    source = HistoricalCapabilitySource(base, historical, {"P": ["2026-W31", "2026-W32"]})
    older = Source(historical)
    source.historical = older
    result = source.read_weekly_totals("P", ["2026-W33", "2026-W31", "2026-W32"], metric="cpm", as_of="2026-11-17")
    assert base.calls == [("P", ["2026-W33"], "cpm")]
    assert older.calls == [("P", ["2026-W31", "2026-W32"], "cpm")]
    assert result.period_label.tolist() == ["2026-W33", "2026-W31", "2026-W32"]
    with pytest.raises(SnapshotCapabilityInputError, match="Unapproved"):
        source.read_weekly_totals("P", ["2026-W30"], metric="cpm", as_of="2026-11-17")
    path.write_bytes(path.read_bytes() + b"modified")
    with pytest.raises(SnapshotCapabilityInputError, match="capture unavailable"):
        source.source_signature(["P"])


def test_query_sql_has_time_predicates_for_facts_product_master_and_mqc():
    sql = module._measurement_sql()
    assert "fact.sheet_start_time >= :start_time" in sql
    assert sql.count("fact.glass_start_time >= :start_time") == 3
    assert sql.count("COALESCE(product.interface_time,product.checkouttime,product.createtime) < :as_of") == 3
    assert "update_time < :as_of OR update_time IS NULL" in sql
    assert "SELECT *" not in sql and "COUNT(" not in sql


def test_capture_refuses_open_weeks_before_database_access(tmp_path, policy):
    engine = SimpleNamespace(connect=lambda: pytest.fail("Database accessed before scope validation"))
    with pytest.raises(SnapshotCapabilityInputError, match="completed"):
        capture_historical_capability_inputs(engine, snapshot_dir=tmp_path, output_dir=tmp_path / "historical",
                                             authorized_weeks={"P": ["2026-W47"]}, as_of="2026-11-17")
