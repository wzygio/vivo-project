import pandas as pd
import pytest

from src.shared_kernel.config import ConfigLoader


@pytest.mark.parametrize("scope", ["spc", "ctq", "aoi_tt", "aoi_rs"])
def test_scope_signature_uses_complete_configured_filename(monkeypatch, tmp_path, scope):
    from src.inline_domain.application.shared import decision_signature as signatures

    configured = tmp_path / "outside" / f"manual-{scope}.xlsx"
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", classmethod(lambda cls, domain, key, default_name=None: configured))
    seen = []
    monkeypatch.setattr(signatures, "get_decision_signature", lambda path, product, **kwargs: seen.append(path) or "signature")
    assert signatures.get_scope_decision_signature(scope, "M") == "signature"
    assert seen == [configured]


def test_capability_adapter_can_read_and_write_renamed_external_workbook(tmp_path):
    from src.inline_domain.infrastructure.spc.capability_decoration_repository import (
        load_capability_decoration, persist_capability_decoration, get_capability_decoration_signature,
    )

    workbook = tmp_path / "outside" / "manual-capability.xlsx"
    detail = pd.DataFrame([dict(prod_code="M", factory="ARRAY", step_id="100", param_name="THK",
                               period_type="week", period_label="2026-W30", period_sort=320,
                               period_start="2026-07-20", period_end="2026-07-26", cpk_corrected=1.26)])
    persist_capability_decoration(tmp_path, detail, "M", workbook_path=workbook)
    loaded = load_capability_decoration(tmp_path, "M", workbook_path=workbook)
    assert loaded["cpk_corrected"].tolist() == [1.26]
    assert get_capability_decoration_signature(tmp_path, workbook_path=workbook)[1] > 0
    assert not (tmp_path / "spc_cpk_cpm_decoration.xlsx").exists()


def test_monitor_signature_tracks_external_configured_workbook(monkeypatch, tmp_path):
    from src.inline_domain.infrastructure.monitor.input_signature import monitor_input_signature

    external = tmp_path / "outside" / "custom-ooc.xlsx"
    external.parent.mkdir()
    external.write_bytes(b"first")
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", classmethod(lambda cls, domain, key, default_name=None: external))
    first = monitor_input_signature(tmp_path / "project", tmp_path / "project" / "resources", ["M"])
    external.write_bytes(b"second version")
    assert monitor_input_signature(tmp_path / "project", tmp_path / "project" / "resources", ["M"]) != first


def test_injected_monitor_directory_does_not_resolve_production_files(monkeypatch, tmp_path):
    from src.inline_domain.infrastructure.monitor.input_signature import monitor_input_signature

    def reject_production(*args, **kwargs):
        raise AssertionError("Injected monitor resolved maintained production files")

    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", reject_production)
    assert monitor_input_signature(tmp_path, tmp_path / "injected", ["M"], include_configured_paths=False)


@pytest.mark.parametrize("scope", ["spc", "ctq"])
def test_preparation_reads_renamed_oos_decisions_and_reports_same_path(monkeypatch, tmp_path, scope):
    from src.inline_domain.application.shared.decorated_data import prepare_decorated_data

    workbook = tmp_path / "outside" / f"manual-{scope}.xlsx"
    workbook.parent.mkdir()
    key = dict(prod_code="M", step_id="100", param_name="THK", sheet_id="S1")
    pd.DataFrame([{**key, "flag": False}]).to_excel(workbook, sheet_name="M__flags", index=False)
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", classmethod(lambda cls, domain, key, default_name=None: workbook))
    raw = pd.DataFrame([{**key, "factory": "ARRAY", "sheet_start_time": "2026-09-01",
                         "site_name": "P1", "unit_id": "U1", "param_value": 8.0, "data_type": scope.upper()}])
    specs = pd.DataFrame([dict(prod_code="M", step_id="100", param_name="THK",
                               usl=6.0, lsl=0.0, ucl=5.0, lcl=1.0, target=3.0)])
    result = prepare_decorated_data(raw, specs, "M", scope, persist=False)
    assert result.raw_measurements_df["param_value"].tolist() == [8.0]
    assert result.sheet_oos_decoration_result.decoration_path == workbook
    assert not (workbook.parent / f"{scope}_sheet_oos_decoration.xlsx").exists()


def test_ooc_write_uses_independent_full_path_and_explicit_directory_stays_isolated(tmp_path):
    from types import SimpleNamespace
    from src.inline_domain.application.shared.ooc_decoration_service import persist_ooc_facts

    calls = []

    class Port:
        def persist_sheet_oos_decoration_outcome(self, directory, detail, **kwargs):
            calls.append(directory / kwargs["file_name"])
            return SimpleNamespace(decoration_df=detail)

    kwargs = dict(scope="spc", prod_code="M", detail_df=pd.DataFrame(), key_columns=[],
                  product_dir=tmp_path / "injected", coverage_start=pd.Timestamp("2026-09-01"),
                  coverage_end=pd.Timestamp("2026-09-02"), decoration_port=Port())
    external = tmp_path / "independent-ooc" / "manual-ooc.xlsx"
    persist_ooc_facts(**kwargs, workbook_path=external)
    persist_ooc_facts(**kwargs)
    assert calls == [external, tmp_path / "injected" / "spc_sheet_ooc_decoration.xlsx"]
    assert not list(tmp_path.iterdir())


def test_aoi_tt_preparation_reads_renamed_decision_file(tmp_path):
    from src.inline_domain.application.aoi_tt.decoration_service import prepare_aoi_tt_decoration

    workbook = tmp_path / "manual-tt.xlsx"
    key = dict(prod_code="M", step_id="100", tt_name="TDSUM", sheet_id="S1")
    pd.DataFrame([{**key, "flag": "Delete"}]).to_excel(workbook, sheet_name="M__flags", index=False)
    details = pd.DataFrame([{**key, "tt_qty": 9.0, "start_time": "2026-09-01"}])
    specs = pd.DataFrame([dict(step_id="100", tt_name="TDSUM", usl=5, ucl=3)])
    result = prepare_aoi_tt_decoration(details, specs, tmp_path / "ignored", "M",
                                       persist=False, workbook_path=workbook)
    assert result.tt_details_df.empty
    assert result.decoration_path == workbook
    assert not (tmp_path / "ignored").exists()


def test_aoi_rs_preparation_reads_renamed_decision_file(tmp_path):
    from src.inline_domain.application.aoi_rs.decoration_service import prepare_aoi_rs_decoration

    workbook = tmp_path / "manual-rs.xlsx"
    key = dict(factory="ARRAY", step_id="100", rs_code="A1PPS")
    decisions = pd.DataFrame([
        {**key, "prod_code": "M", "chart_kind": "lot", "point_id": "L1", "flag": "Delete"},
        {**key, "prod_code": "M", "chart_kind": "sheet", "point_id": "S1", "flag": False},
    ])
    decisions.to_excel(workbook, sheet_name="M__flags", index=False)
    lots = pd.DataFrame([{**key, "lot_id": "L1", "value": 2.5}])
    sheets = pd.DataFrame([{**key, "sheet_id": "S1", "rs_qty": 3}])
    specs = pd.DataFrame([{**key, "type_flag": kind, "spec": 1.0} for kind in ["LOT_RATIO", "SHEET_ID"]])
    result = prepare_aoi_rs_decoration(lots, sheets, specs, tmp_path / "ignored", "M",
                                       persist=False, workbook_path=workbook)
    assert result.lot_points_df.empty
    assert result.sheet_points_df["rs_qty"].tolist() == [3]
    assert result.decoration_path == workbook
    assert not (tmp_path / "ignored").exists()


def test_renamed_spc_oos_persistence_normalizes_legacy_delete_for_alerts(monkeypatch, tmp_path):
    from datetime import date
    from src.inline_domain.application.shared.decorated_data import prepare_decorated_data
    from src.inline_domain.core.shared.sheet_oos_alerts import build_sheet_oos_alerts
    from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import load_sheet_oos_decoration

    workbook = tmp_path / "outside" / "manual-oos.xlsx"
    workbook.parent.mkdir()
    key = dict(prod_code="M", step_id="100", param_name="THK", sheet_id="S1")
    pd.DataFrame([{**key, "flag": "Delete"}]).to_excel(workbook, sheet_name="M__flags", index=False)
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", classmethod(lambda cls, domain, key, default_name=None: workbook))
    raw = pd.DataFrame([{**key, "factory": "ARRAY", "sheet_start_time": "2026-09-01",
                         "site_name": "P1", "unit_id": "U1", "param_value": 8.0, "data_type": "SPC"}])
    specs = pd.DataFrame([dict(prod_code="M", step_id="100", param_name="THK",
                               usl=6.0, lsl=0.0, ucl=5.0, lcl=1.0, target=3.0)])
    result = prepare_decorated_data(raw, specs, "M", "spc", persist=True)
    generated = load_sheet_oos_decoration(workbook.parent, workbook.name, "M")
    assert result.raw_measurements_df["param_value"].tolist() == [8.0]
    assert generated["flag"].tolist() == [False]
    alerts = build_sheet_oos_alerts(generated, time_column="sheet_start_time", reference_date=date(2026, 9, 7))
    assert alerts["sheet_id"].tolist() == ["S1"]
    decisions = pd.read_excel(workbook, sheet_name="M__flags")
    assert decisions["flag"].tolist() == ["Delete"]


@pytest.mark.parametrize("filename", ["manual-ooc.xlsx", "spc_sheet_oos_decoration.xlsx"])
def test_renamed_spc_ooc_persistence_keeps_tristate_delete(tmp_path, filename):
    from src.inline_domain.application.shared.ooc_decoration_service import persist_ooc_facts
    from src.inline_domain.core.shared.sheet_ooc_decoration import OOC_KEY_COLUMNS

    workbook = tmp_path / filename
    key = dict(prod_code="M", step_id="100", param_name="THK", sheet_id="S1")
    pd.DataFrame([{**key, "flag": "Delete"}]).to_excel(workbook, sheet_name="M__flags", index=False)
    detail = pd.DataFrame([{**key, "factory": "ARRAY", "sheet_start_time": "2026-09-01",
                            "sheet_max": 4.5, "sheet_min": 4.5, "sheet_mean": 4.5,
                            "ucl": 4.0, "lcl": 2.0, "ooc_type": "Above UCL"}])
    result = persist_ooc_facts(scope="spc", prod_code="M", detail_df=detail, key_columns=OOC_KEY_COLUMNS,
                               product_dir=tmp_path / "ignored", workbook_path=workbook,
                               coverage_start=pd.Timestamp("2026-09-01"), coverage_end=pd.Timestamp("2026-09-02"))
    assert result["flag"].tolist() == ["Delete"]
    generated = pd.read_excel(workbook, sheet_name="M")
    assert generated["flag"].tolist() == ["Delete"]
