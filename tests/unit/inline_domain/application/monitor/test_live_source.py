"""Live monitor cache invalidation and shared reader boundaries."""

from __future__ import annotations

import pandas as pd
import pytest
import streamlit as st

from src.inline_domain.application.monitor.live_history import LiveHistoryStore
from src.inline_domain.application.monitor.live_source import (
    LiveMonitorSource,
    LiveThroughputReader,
)
from src.inline_domain.application.shared.oos_history_service import OosHistoryService
from src.inline_domain.application.shared.ooc_history_service import OocHistoryService


class EmptyPort:
    def __init__(self):
        self.raw_reads = 0

    def _raw(self, _query):
        self.raw_reads += 1
        return pd.DataFrame()

    get_spc_measurements = _raw
    get_tt_details = _raw
    get_rs_details = _raw

    def get_pass_through(self, _query):
        return pd.DataFrame()

    def _specs(self, _product):
        return pd.DataFrame()

    get_spc_spec_limits = _specs
    get_tt_spec_limits = _specs
    get_rs_spec_limits = _specs


class MeasurementPort(EmptyPort):
    values = [("P1", 50.0), ("P2", 100.0)]

    def get_spc_measurements(self, _query):
        self.raw_reads += 1
        return pd.DataFrame([
            {
                "factory": "ARRAY", "prod_code": "M626", "step_id": "100",
                "param_name": "THK", "sheet_id": "S1",
                "sheet_start_time": "2026-09-01", "site_name": site,
                "param_value": value, "data_type": "SPC",
            }
            for site, value in self.values
        ])

    def get_spc_spec_limits(self, _product):
        return pd.DataFrame([{
            "prod_code": "M626", "step_id": "100", "param_name": "THK",
            "usl": 60.0, "lsl": 40.0, "ucl": 54.0, "lcl": 46.0,
            "target": 50.0,
        }])


@pytest.fixture(autouse=True)
def clear_live_cache(monkeypatch):
    def reject_derived_snapshot(*args, **kwargs):
        pytest.fail("live computations must not persist derived Parquet snapshots")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", reject_derived_snapshot)
    st.cache_data.clear()
    yield
    st.cache_data.clear()


def _source(port, tmp_path, signature=None):
    return LiveMonitorSource(
        port_factory=lambda scope, product: port,
        signature_provider=lambda products: signature[0] if signature else "test-v1",
        resource_dir_provider=lambda scope: tmp_path,
    )


@pytest.mark.parametrize("scope", ["spc", "ctq", "aoi_tt", "aoi_rs"])
def test_empty_scope_shares_one_computation_across_all_readers(tmp_path, scope):
    port = EmptyPort()
    source = _source(port, tmp_path)
    oos = LiveHistoryStore(source, "oos").read(scope, "M626")
    ooc = LiveHistoryStore(source, "ooc").read(scope, "M626")
    throughput = LiveThroughputReader(source).read_product(scope, "M626")

    assert oos.frame.empty and ooc.frame.empty and throughput.empty
    assert port.raw_reads == 1
    assert oos.metadata.policy == "inline-computed-cache-v1"
    assert oos.metadata.refreshed_at == ooc.metadata.refreshed_at
    assert not list(tmp_path.rglob("*.parquet"))


def test_configuration_change_and_explicit_clear_recompute(tmp_path):
    port = EmptyPort()
    revision = ["config-v1"]
    source = _source(port, tmp_path, revision)
    source.read_payload("M626", "spc")
    source.read_payload("M626", "spc")
    assert port.raw_reads == 1

    revision[0] = "config-v2"
    source.read_payload("M626", "spc")
    assert port.raw_reads == 2

    st.cache_data.clear()
    source.read_payload("M626", "spc")
    assert port.raw_reads == 3
    assert not list(tmp_path.rglob("*.parquet"))


@pytest.mark.parametrize("alarm_type", ["oos", "ooc"])
def test_existing_history_service_reapplies_manual_flags_without_recomputing(
    tmp_path, alarm_type
):
    port = MeasurementPort()
    if alarm_type == "ooc":
        port.values = [("P1", 55.0), ("P2", 58.0)]
    source = _source(port, tmp_path)

    class Decisions:
        flag = "False"

        def load_decisions(self, file_name, prod_code, key_columns):
            return pd.DataFrame([{
                "prod_code": "M626", "step_id": "100", "param_name": "THK",
                "sheet_id": "S1", "flag": self.flag,
            }])

    decisions = Decisions()
    service_class = OosHistoryService if alarm_type == "oos" else OocHistoryService
    service = service_class(LiveHistoryStore(source, alarm_type), decisions)
    first = service.read_product("spc", "M626")
    assert first.source == "computed_cache"
    assert len(first.alerts_df) == 1

    decisions.flag = "True"
    second = service.read_product("spc", "M626")
    assert second.alerts_df.empty
    assert len(second.decorated_df) == 1
    assert port.raw_reads == 1
    assert not list(tmp_path.rglob("*.parquet"))
