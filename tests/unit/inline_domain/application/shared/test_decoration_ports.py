from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.application.shared.sheet_oos_decoration_service import prepare_sheet_oos_decoration


@pytest.fixture(autouse=True)
def reject_production_adapter(monkeypatch):
    """A fake-port use case must never initialize production infrastructure."""
    from src.inline_domain.application.shared import decoration_defaults

    def fail():
        raise AssertionError("Injected use case attempted to resolve a production adapter")

    monkeypatch.setattr(decoration_defaults, "default_decoration_port", fail)


class MemoryDecisions:
    def __init__(self):
        self.decisions = pd.DataFrame([dict(prod_code="M", step_id="100", param_name="THK", sheet_id="S1", flag="Delete")])
        self.writes = []
        self.reads = 0

    def load_sheet_oos_decisions(self, *args, **kwargs):
        self.reads += 1
        return self.decisions.copy()

    def persist_sheet_oos_decoration_outcome(self, product_dir, detail, *args, **kwargs):
        from src.inline_domain.core.shared.sheet_oos_decoration import merge_detail_with_decoration_flags
        self.writes.append(detail.copy())
        return SimpleNamespace(
            decoration_df=merge_detail_with_decoration_flags(detail, self.decisions, kwargs.get("key_columns")),
            decisions_df=self.decisions.copy(),
            refresh_decision=SimpleNamespace(reason="memory"),
        )


@pytest.mark.parametrize("persist", [False, True])
def test_sheet_decoration_uses_injected_decisions_without_files(tmp_path, persist):
    key = dict(prod_code="M", step_id="100", param_name="THK", sheet_id="S1")
    raw = pd.DataFrame([{**key, "param_value": 8.0}])
    features = pd.DataFrame([{**key, "factory": "ARRAY", "sheet_start_time": "2026-09-01", "sheet_max": 8.0, "sheet_min": 8.0, "sheet_mean": 8.0, "usl": 6.0, "lsl": 0.0}])
    port = MemoryDecisions()
    result = prepare_sheet_oos_decoration(
        raw, features, tmp_path, persist_files=persist, decoration_port=port,
    )
    assert result.raw_measurements_df.empty
    assert len(port.writes) == int(persist)
    assert port.reads == int(not persist)
    assert result.decoration_df.iloc[0]["flag"] == "Delete"
    assert not list(tmp_path.iterdir())


def test_signature_uses_injected_probe_and_reader_without_shared_cache(tmp_path):
    from src.inline_domain.application.shared.decision_signature import get_scope_decision_signature
    from src.inline_domain.core.shared.sheet_oos_decoration import EMPTY_DECISION_SIGNATURE

    class MemorySignature(MemoryDecisions):
        def scope_resource_dir(self, scope, alarm_type="oos"):
            assert scope == "spc"
            return tmp_path

        def get_decision_file_stat(self, path):
            assert path.name == "spc_sheet_oos_decoration.xlsx"
            return (1, 100)

    port = MemorySignature()
    first = get_scope_decision_signature("spc", "M", signature_port=port, resource_port=port)
    port.decisions["flag"] = False
    second = get_scope_decision_signature("spc", "M", signature_port=port, resource_port=port)
    assert first != EMPTY_DECISION_SIGNATURE
    assert first != second
    assert port.reads == 2
    assert not list(tmp_path.iterdir())


def test_signature_port_error_is_not_an_empty_decision(tmp_path):
    from src.inline_domain.application.shared.decision_signature import get_decision_signature
    from src.inline_domain.application.shared.decoration_ports import SheetOosDecorationReadError

    class UnreadableSignature:
        def get_decision_file_stat(self, path):
            return (1, 100)

        def load_sheet_oos_decisions(self, **kwargs):
            raise SheetOosDecorationReadError("unreadable decisions")

    with pytest.raises(SheetOosDecorationReadError):
        get_decision_signature(tmp_path / "memory.xlsx", "M", signature_port=UnreadableSignature())


@pytest.mark.parametrize("persist", [False, True])
def test_aoi_tt_decoration_reads_and_writes_injected_port(tmp_path, persist):
    from src.inline_domain.application.aoi_tt.decoration_service import prepare_aoi_tt_decoration

    key = dict(prod_code="M", step_id="100", tt_name="TDSUM", sheet_id="S1")
    details = pd.DataFrame([{**key, "tt_qty": 9.0, "start_time": "2026-09-01"}])
    specs = pd.DataFrame([dict(step_id="100", tt_name="TDSUM", usl=5, ucl=3)])
    port = MemoryDecisions()
    port.decisions = pd.DataFrame([{**key, "flag": "Delete"}])
    result = prepare_aoi_tt_decoration(details, specs, tmp_path, "M", persist=persist, decoration_port=port)
    assert result.tt_details_df.empty
    assert len(port.writes) == int(persist)
    assert port.reads == int(not persist)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("persist", [False, True])
def test_capability_decoration_uses_manual_value_from_memory_port(tmp_path, persist):
    from src.inline_domain.application.spc.capability_decoration_service import prepare_capability_decoration

    row = dict(prod_code="M", factory="ARRAY", step_id="100", param_name="THK",
               period_type="week", period_label="2026-W30", period_sort=320,
               period_start="2026-07-20", period_end="2026-07-26", cpk=1.26)

    class MemoryCapability:
        def __init__(self):
            self.reads = 0
            self.writes = []
            self.decisions = pd.DataFrame([{**row, "flag": True, "cpk_corrected": 1.26, "cpk_replacement": 1.372}])

        def load_capability_decoration(self, *args):
            self.reads += 1
            return self.decisions.copy()

        def persist_capability_decoration(self, product_dir, detail, sheet, metric):
            self.writes.append(detail)
            return self.decisions.copy()

        def get_capability_decoration_signature(self, product_dir):
            return (1, 100)

    port = MemoryCapability()
    result = prepare_capability_decoration(pd.DataFrame([row]), tmp_path, persist_files=persist,
                                          reference_date=date(2026, 7, 27), decoration_port=port)
    assert result.period_capability_df["cpk"].tolist() == [1.372]
    assert len(port.writes) == int(persist)
    assert port.reads == int(not persist)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("persist", [False, True])
def test_aoi_rs_port_decision_removes_only_the_selected_chart_point(tmp_path, persist):
    from src.inline_domain.application.aoi_rs.decoration_service import prepare_aoi_rs_decoration

    key = dict(factory="ARRAY", step_id="100", rs_code="A1PPS")
    lots = pd.DataFrame([{**key, "lot_id": "L1", "value": 2.5}])
    sheets = pd.DataFrame([{**key, "sheet_id": "S1", "rs_qty": 3}])
    specs = pd.DataFrame([{**key, "type_flag": kind, "spec": 1.0} for kind in ["LOT_RATIO", "SHEET_ID"]])
    port = MemoryDecisions()
    port.decisions = pd.DataFrame([
        {**key, "prod_code": "M", "chart_kind": "lot", "point_id": "L1", "flag": "Delete"},
        {**key, "prod_code": "M", "chart_kind": "sheet", "point_id": "S1", "flag": False},
    ])
    result = prepare_aoi_rs_decoration(lots, sheets, specs, tmp_path, "M", persist=persist, decoration_port=port)
    assert result.lot_points_df.empty
    assert result.sheet_points_df["rs_qty"].tolist() == [3]
    assert len(port.writes) == int(persist)
    assert port.reads == int(not persist)
    assert not list(tmp_path.iterdir())


def test_ooc_public_entry_persists_through_injected_port(tmp_path):
    from src.inline_domain.application.shared.ooc_decoration_service import persist_ooc_facts

    port = MemoryDecisions()
    result = persist_ooc_facts(
        scope="spc", prod_code="M", detail_df=port.decisions.drop(columns="flag"),
        key_columns=["prod_code", "step_id", "param_name", "sheet_id"],
        product_dir=tmp_path, coverage_start=pd.Timestamp("2026-09-01"),
        coverage_end=pd.Timestamp("2026-09-02"), decoration_port=port,
    )
    assert result["flag"].tolist() == ["Delete"]
    assert len(port.writes) == 1
    assert not list(tmp_path.iterdir())


def test_composition_can_build_decoration_adapter_in_a_fresh_process():
    """Page bootstrap must work without an earlier application import."""
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-c", (
            "from src.inline_domain.composition import build_decoration_port; "
            "port = build_decoration_port(); "
            "assert callable(port.load_sheet_oos_decisions); "
            "assert callable(port.get_decision_file_stat)"
        )],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
