from types import SimpleNamespace

import pandas as pd

from src.inline_domain.infrastructure.monitor import aoi_rs_monitor_source as module
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy


def test_no_l1_source_translates_windows_filters_before_aggregation_and_limits_steps(monkeypatch):
    day = pd.Timestamp.today().normalize()
    source_day = day - pd.Timedelta(days=4)
    common = dict(factory="ARRAY", prod_code="M626", step_id="11629", lot_id="L1", rs_code="A1")
    raw = pd.DataFrame([
        dict(common, sheet_id="S1", start_time=source_day + pd.Timedelta(hours=11), code_qty=2),
        dict(common, sheet_id="S2", start_time=source_day + pd.Timedelta(hours=13), code_qty=9),
    ])
    calls = []

    def rs_loader(db, query, *, strict):
        assert strict
        calls.append(query)
        return raw.copy()

    def pass_loader(db, query, *, strict, step_ids):
        assert strict and step_ids == ("11629",)
        return raw.drop(columns=["rs_code", "code_qty"])

    monkeypatch.setattr(module, "load_rs_details", rs_loader)
    monkeypatch.setattr(module, "load_pass_through", pass_loader)
    monkeypatch.setattr(module, "load_rs_spec_limits", lambda *args, **kwargs: pd.DataFrame([
        dict(common, type_flag="MWD_RATIO", code_desc="Fixture", spec=10),
    ]))
    monkeypatch.setattr(ConfigLoader, "get_data_forward_policy", classmethod(lambda cls: DataForwardPolicy(True, 4)))
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", classmethod(lambda cls: None))
    monkeypatch.setattr(ConfigLoader, "get_aoi_rs_special_decoration_factories", classmethod(lambda cls: []))
    source = module.AoiRsMonitorSource(lambda: SimpleNamespace())
    inputs = source.read_projected("M626", "ARRAY", str(day.date()), str(day.date()))
    assert calls[0].start_date == str(source_day.date())
    assert set(inputs["rs_details_df"]["sheet_id"]) == {"S1"}
    assert inputs["rs_details_df"]["start_time"].iloc[0].date() == day.date()
    assert set(inputs["pass_through_df"]["sheet_id"]) == {"S1"}

    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", classmethod(
        lambda cls: (("ARRAY",), str(day.date()), str(day.date()))))
    inputs = source.read_projected("M626", "ARRAY", str(day.date()), str(day.date()))
    assert inputs["rs_details_df"].empty and inputs["pass_through_df"].empty


def test_each_explicit_query_signature_detects_specification_changes(monkeypatch):
    spec = {"value": 1.0}
    monkeypatch.setattr(module, "load_rs_spec_limits", lambda *args, **kwargs: pd.DataFrame([
        dict(prod_code="M626", factory="ARRAY", step_id="11629", rs_code="A1",
             type_flag="MWD_RATIO", code_desc="Fixture", spec=spec["value"]),
    ]))
    monkeypatch.setattr(module, "get_scope_decision_signature", lambda *args: "fixture")
    source = module.AoiRsMonitorSource(lambda: SimpleNamespace())
    first = source.history_signature("M626")
    spec["value"] = 2.0
    assert source.history_signature("M626") != first
