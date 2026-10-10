from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.aoi_tt.decoration_service import prepare_aoi_tt_decoration
from src.inline_domain.application.aoi_rs.decoration_service import prepare_aoi_rs_decoration
from src.inline_domain.application.shared.ooc_decoration_service import persist_ooc_facts
from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    persist_sheet_oos_decoration_outcome,
)
from src.shared_kernel.config import ConfigLoader


@pytest.mark.parametrize("parameter_column", ["tt_name", "rs_code"])
def test_persisted_exempt_flags_preserve_delete_and_manual_decisions(
    tmp_path: Path, parameter_column: str,
) -> None:
    keys = ["prod_code", "step_id", parameter_column, "sheet_id"]
    detail = pd.DataFrame([
        dict(prod_code="M", step_id="100", sheet_id=sheet, **{parameter_column: parameter})
        for sheet, parameter in [("S1", "ppa_X"), ("S2", "PPA_Y"), ("S3", "THK"), ("S4", "A.B"), ("S5", "AxB")]
    ])
    decisions = detail.iloc[:3].assign(flag=[True, "Delete", False])
    path = tmp_path / "custom.xlsx"
    decisions.to_excel(path, sheet_name="M__flags", index=False)
    stored_decisions = pd.read_excel(path, sheet_name="M__flags")
    outcome = persist_sheet_oos_decoration_outcome(
        tmp_path, detail, "custom.xlsx", "M", keys,
        parameter_column=parameter_column, exempt_param_name_contains=[" pPa ", "A.B"],
    )
    actual = pd.read_excel(path, sheet_name="M")
    assert actual["flag"].tolist() == [False, "Delete", False, False, True]
    pd.testing.assert_frame_equal(outcome.decisions_df, decisions[[*keys,"flag"]], check_dtype=False)
    pd.testing.assert_frame_equal(pd.read_excel(path, sheet_name="M__flags"), stored_decisions)
    assert "flag" not in detail


def test_exemption_change_rewrites_detail_inside_ttl_and_can_be_disabled(tmp_path: Path) -> None:
    now = datetime(2026, 10, 9, 12)
    detail = pd.DataFrame([dict(prod_code="M", step_id="100", tt_name="PPA_X", sheet_id="S1")])
    kwargs = dict(scope="aoi_tt", product_revision="R1", decision_signature="manual-sig",
                  parameter_column="tt_name", key_columns=list(detail.columns), sheet_name="M",
                  file_name="custom.xlsx")
    first = persist_sheet_oos_decoration_outcome(tmp_path, detail, now=now, exempt_param_name_contains=[], **kwargs)
    assert first.decoration_df["flag"].tolist() == [True]
    released = persist_sheet_oos_decoration_outcome(tmp_path, detail, now=now+timedelta(minutes=1), exempt_param_name_contains=["PPA"], **kwargs)
    assert released.refresh_decision.should_write
    assert pd.read_excel(tmp_path/"custom.xlsx", sheet_name="M")["flag"].tolist() == [False]
    same = persist_sheet_oos_decoration_outcome(tmp_path, detail, now=now+timedelta(minutes=2), exempt_param_name_contains=["ppa"], **kwargs)
    assert not same.refresh_decision.should_write
    disabled = persist_sheet_oos_decoration_outcome(tmp_path, detail, now=now+timedelta(minutes=3), exempt_param_name_contains=[], **kwargs)
    assert disabled.refresh_decision.should_write
    assert pd.read_excel(tmp_path/"custom.xlsx", sheet_name="M")["flag"].tolist() == [True]


@pytest.mark.parametrize("persist", [False, True])
@pytest.mark.parametrize("as_iterator", [False, True])
def test_aoi_tt_returned_and_saved_exempt_details_are_released(
    tmp_path: Path, persist: bool, as_iterator: bool,
) -> None:
    details = pd.DataFrame([dict(factory="ARRAY", prod_code="M", step_id="100", tt_name="PPA_X",
                                sheet_id="S1", lot_id="L1", start_time=pd.Timestamp("2026-09-01"), tt_qty=9.0)])
    specs = pd.DataFrame([dict(step_id="100", tt_name="PPA_X", usl=6.0, ucl=5.0)])
    exemptions = iter(["PPA"]) if as_iterator else ["PPA"]
    result = prepare_aoi_tt_decoration(
        details, specs, tmp_path, "M", persist=persist,
        exempt_param_name_contains=exemptions,
    )
    assert result.decoration_df["flag"].tolist() == [False]
    assert result.tt_details_df["tt_qty"].tolist() == [9.0]
    if persist:
        assert pd.read_excel(result.decoration_path, sheet_name="M")["flag"].tolist() == [False]

        class Store:
            def read(self, scope: str, product: str) -> dict[str, pd.DataFrame]:
                return {"frame": pd.read_excel(result.decoration_path, sheet_name=product),
                        "refresh_meta": pd.DataFrame()}
        assert len(ExcelAlarmReader(Store()).read_product("aoi_tt", "M").alerts_df) == 1
    else:
        assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("persist", [False, True])
@pytest.mark.parametrize("as_iterator", [False, True])
def test_aoi_rs_lot_and_sheet_exempt_details_are_released(
    tmp_path: Path, persist: bool, as_iterator: bool,
) -> None:
    common = dict(factory="ARRAY", step_id="100", rs_code="PPA_X", first_start_time=pd.Timestamp("2026-09-01"))
    lot = pd.DataFrame([{**common, "lot_id":"L1", "rs_qty":9, "sheet_qty":1, "value":9.0}])
    sheet = pd.DataFrame([{**common, "sheet_id":"S1", "rs_qty":9}])
    specs = pd.DataFrame([{**common, "type_flag":kind, "spec":6.0} for kind in ["LOT_RATIO","SHEET_ID"]])
    exemptions = iter(["PPA"]) if as_iterator else ["PPA"]
    result = prepare_aoi_rs_decoration(
        lot, sheet, specs, tmp_path, "M", persist=persist,
        exempt_param_name_contains=exemptions,
    )
    assert result.decoration_df["flag"].tolist() == [False, False]
    if persist:
        assert pd.read_excel(result.decoration_path,sheet_name="M")["flag"].tolist()==[False,False]


@pytest.mark.parametrize("scope,column,expected", [("aoi_tt","tt_name",False),("aoi_rs","rs_code",False),("spc","param_name",True)])
def test_ooc_persistence_applies_exemptions_only_to_aoi(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scope: str, column: str, expected: bool,
) -> None:
    monkeypatch.setattr(ConfigLoader,"get_auto_decoration_param_exemptions",lambda:["PPA"])
    detail=pd.DataFrame([dict(prod_code="M",step_id="100",sheet_id="S1",**{column:"PPA_X"})])
    result=persist_ooc_facts(scope=scope,prod_code="M",detail_df=detail,key_columns=list(detail.columns),
                             product_dir=tmp_path,coverage_start=pd.Timestamp("2026-09-01"),coverage_end=pd.Timestamp("2026-09-02"))
    assert bool(result.iloc[0]["flag"]) is expected
    assert bool(pd.read_excel(tmp_path/f"{scope}_sheet_ooc_decoration.xlsx",sheet_name="M").iloc[0]["flag"]) is expected


@pytest.mark.parametrize("scope", ["aoi_tt", "aoi_rs"])
def test_exemption_configuration_invalidates_report_cache(
    monkeypatch: pytest.MonkeyPatch, scope: str,
) -> None:
    from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportService
    from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
    from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportService
    from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig

    class EmptyPort:
        calls = 0

        def get_tt_details(self, query: object) -> pd.DataFrame:
            self.calls += 1
            return pd.DataFrame()
        get_rs_details = get_tt_details

    if scope == "aoi_tt":
        service, query_type = AoiTtReportService, AoiTtQueryConfig
    else:
        service, query_type = AoiRsReportService, AoiRsQueryConfig
    cached = getattr(service, f"fetch_{scope}_report_payload")
    get_report = getattr(service, f"get_{scope}_report_data")
    cached.clear()
    query = query_type(prod_code="M", start_date="2026-09-01", end_date="2026-09-02").model_dump_json()
    exemptions: list[str] = []
    monkeypatch.setattr(ConfigLoader, "get_auto_decoration_param_exemptions", lambda: exemptions)
    port = EmptyPort()
    try:
        get_report(port, query, snapshot_signature="exemption-change-test")
        get_report(port, query, snapshot_signature="exemption-change-test")
        assert port.calls == 1
        exemptions.append("PPA")
        get_report(port, query, snapshot_signature="exemption-change-test")
        assert port.calls == 2
    finally:
        cached.clear()
