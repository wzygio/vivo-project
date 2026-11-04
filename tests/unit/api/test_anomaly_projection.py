"""Public projection contracts using local metadata and published fixture rows."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from api.schemas import AnomalyQuery, AnomalyRecord
from api.service import PublishedAnomalyService
from src.shared_kernel.infrastructure.api_alert_metadata import (
    STATION_COLUMNS,
    ApiAlertMetadata,
)

REFERENCE = date(2026, 11, 2)


@pytest.fixture
def metadata_path(tmp_path: Path) -> Path:
    path = tmp_path / "api-metadata.xlsx"
    stations = [
        {"step_id": "00100", "description": "PI2_CURE_THK"},
        {"step_id": "00200", "description": "LAST_FINAL"},
    ]
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(stations, columns=STATION_COLUMNS).to_excel(writer, sheet_name="站点字典", index=False)
    return path


def spc_row(**changes) -> dict:
    return {
        "factory": "ARRAY", "step_id": "00100", "param_name": "THK", "sheet_id": "00000000101",
        "sheet_start_time": "2026-10-28 09:30:00", "oos_type": "USL", "ooc_type": "UCL",
        "flag": False, "internal_policy": "must never be serialized", **changes,
    }


def service_for(metadata_path: Path, rows: list[dict], **result_changes) -> PublishedAnomalyService:
    def source_reader(query, day, factories):
        assert day == REFERENCE
        return {"state": "alert", "records": pd.DataFrame(rows), "metadata": {"availability": "ready"}, **result_changes}
    return PublishedAnomalyService(source_reader=source_reader, hidden=lambda domain, product: False,
                                   metadata=ApiAlertMetadata(metadata_path))


def test_entity_filter_avoids_unrequested_invalid_metadata(metadata_path: Path) -> None:
    service = service_for(metadata_path, [spc_row(), spc_row(sheet_id="UNREQUESTED", step_id="missing")])
    query = AnomalyQuery(event_type="OOS", product="P1", sub_domain="SPC", entity_id="00000000101")
    values = service.read(query, ("ARRAY",), REFERENCE)
    assert [row["对象编号"] for row in values] == ["00000000101"]


@pytest.mark.parametrize("domain,parameter_key,time_key,id_key,kind,parameter", [
    ("SPC", "param_name", "sheet_start_time", "sheet_id", None, "THK"),
    ("CTQ", "param_name", "sheet_start_time", "sheet_id", None, "CD"),
    ("AOI_TT", "tt_name", "start_time", "sheet_id", None, "Total"),
    ("AOI_RS", "rs_code", "sheet_start_time", "point_id", "sheet", "RS001"),
])
@pytest.mark.parametrize("event,boundary", [("OOS", "USL"), ("OOC", "UCL")])
def test_inline_public_fields_follow_each_source_and_event(
    metadata_path: Path, domain: str, parameter_key: str, time_key: str,
    id_key: str, kind: str | None, parameter: str, event: str, boundary: str,
) -> None:
    source = {
        "factory": "ARRAY", "step_id": "00100", parameter_key: parameter,
        time_key: "2026-10-28 09:30:00", id_key: "00000000101", "flag": False,
        "oos_type": "USL", "ooc_type": "UCL", "secret": "not-public",
    }
    if kind:
        source["chart_kind"] = kind
    query = AnomalyQuery(product="P1", sub_domain=domain, event_type=event)
    before = metadata_path.read_bytes()
    result = service_for(metadata_path, [source]).read(query, ("ARRAY",), REFERENCE)
    assert result == [{
        "分类": domain, "发生站点": "00100", "部门": "ARRAY", "层别": "PI2",
        "监控参数": parameter, "异常事件": event, "对象类型": "sheet", "对象编号": "00000000101",
        "产品型号": "P1", "发生时间": "2026-10-28T09:30:00+08:00", "命中边界": boundary,
    }]
    assert metadata_path.read_bytes() == before


def test_rs_keeps_sheet_and_lot_separate_and_filters_by_type(metadata_path: Path) -> None:
    rows = [
        dict(factory="ARRAY", step_id="00100", rs_code="RS001", chart_kind="sheet", point_id="00000000101",
             sheet_start_time="2026-10-28 09:30:00", flag=False),
        dict(factory="ARRAY", step_id="00100", rs_code="RS001", chart_kind="lot", point_id="000000001",
             sheet_start_time="2026-10-28 09:31:00", flag=False),
    ]
    service = service_for(metadata_path, rows)
    values = service.read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="AOI_RS"), ("ARRAY",), REFERENCE)
    assert [(row["对象类型"], row["对象编号"]) for row in values] == [
        ("lot", "000000001"), ("sheet", "00000000101"),
    ]
    filtered = service.read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="AOI_RS", entity_type="lot"), ("ARRAY",), REFERENCE)
    assert [row["对象编号"] for row in filtered] == ["000000001"]


def test_object_type_filter_precedes_unrequested_rs_lot_metadata(metadata_path: Path) -> None:
    rows = [
        dict(factory="ARRAY", step_id="00100", rs_code="RS001", chart_kind="sheet", point_id="00000000101",
             sheet_start_time="2026-10-28 09:30:00", flag=False),
        dict(factory="ARRAY", step_id="missing", rs_code="RS001", chart_kind="lot", point_id="000000001",
             sheet_start_time="2026-10-28 09:31:00", flag=False),
    ]
    result = service_for(metadata_path, rows).read(
        AnomalyQuery(event_type="OOS", product="P1", sub_domain="AOI_RS", entity_type="sheet"), ("ARRAY",), REFERENCE,
    )
    assert [row["对象编号"] for row in result] == ["00000000101"]


def test_qtime_uses_from_step_for_occurrence_station_and_layer(metadata_path: Path) -> None:
    row = dict(shop="OLED", prodcode="P1", f_step="00100", t_step="00200", step_desc="FROM_TO",
               lot_id="000000001", timekey="20261028093000123456", flag=False)
    result = service_for(metadata_path, [row]).read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="Q-Time"), ("OLED",), REFERENCE)
    assert result == [{
        "分类": "Q-Time", "发生站点": "00100", "部门": "OLED", "层别": "PI2", "监控参数": "FROM_TO",
        "异常事件": "OOS", "对象类型": "lot", "对象编号": "000000001", "产品型号": "P1",
        "发生时间": "2026-10-28T09:30:00+08:00", "命中边界": "spec",
    }]


def yield_row() -> dict:
    return {"超规 Lot ID": "000000001", "异常 Code": "LINE", "入库时间": "2026/10/28", "flag": False,
            "defect_group": "Array_Line", "factory": "ARRAY"}


def test_yield_uses_published_factory_and_null_layer_without_metadata(metadata_path: Path) -> None:
    metadata_path.unlink()
    result = service_for(metadata_path, [yield_row()]).read(
        AnomalyQuery(event_type="OOS", product="P1", sub_domain="Yield"), ("ARRAY",), REFERENCE,
    )
    assert result == [{
        "分类": "Yield", "发生站点": None, "部门": "ARRAY", "层别": None, "监控参数": "LINE",
        "异常事件": "OOS", "对象类型": "lot", "对象编号": "000000001", "产品型号": "P1",
        "发生时间": "2026-10-28", "命中边界": "spec",
    }]


def test_yield_missing_factory_is_unavailable_instead_of_dropping_rows(metadata_path: Path) -> None:
    with pytest.raises(ValueError):
        service_for(metadata_path, [{**yield_row(), 'factory': None}]).read(
            AnomalyQuery(event_type="OOS", product="P1", sub_domain="Yield"), ("ARRAY",), REFERENCE)


@pytest.mark.parametrize('domain', ['SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Q-Time'])
def test_null_layer_is_only_valid_for_yield(domain):
    with pytest.raises(ValueError, match='layer'):
        AnomalyRecord(sub_domain=domain, product='P1', event_type='OOS', step_id='001', factory='ARRAY',
                      layer=None, parameter_name='D1', entity_type='sheet', entity_id='S1', occurred_at='2026-10-28')


def test_known_unauthorized_factory_is_skipped_before_missing_station_metadata(metadata_path: Path) -> None:
    service = service_for(metadata_path, [spc_row(), spc_row(factory="OLED", sheet_id="UNAUTHORIZED", step_id="missing")])
    values = service.read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="SPC"), ("ARRAY",), REFERENCE)
    assert [row["对象编号"] for row in values] == ["00000000101"]


def test_unknown_required_metadata_fails_the_source_instead_of_returning_partial_rows(metadata_path: Path) -> None:
    service = service_for(metadata_path, [spc_row(), spc_row(sheet_id="S2", step_id="missing")])
    with pytest.raises(ValueError):
        service.read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="SPC"), ("ARRAY",), REFERENCE)


@pytest.mark.parametrize("changes", [
    {"state": "error"},
    {"metadata": {"availability": "missing"}},
    {"release_evidence": False},
    {"records": None},
])
def test_missing_or_legacy_release_evidence_is_not_a_successful_empty_result(metadata_path: Path, changes: dict) -> None:
    with pytest.raises(ValueError):
        service_for(metadata_path, [], **changes).read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="SPC"), ("ARRAY",), REFERENCE)


def test_valid_empty_source_is_successful_and_does_not_need_station_metadata(metadata_path: Path) -> None:
    empty = pd.DataFrame(columns=list(spc_row()))
    assert service_for(metadata_path, [], records=empty).read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="SPC"), ("ARRAY",), REFERENCE) == []


def test_schema_less_empty_source_is_not_a_successful_publication(metadata_path: Path) -> None:
    with pytest.raises(ValueError):
        service_for(metadata_path, []).read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="SPC"), ("ARRAY",), REFERENCE)


def test_nonempty_source_without_flags_is_unavailable(metadata_path: Path) -> None:
    row = spc_row()
    del row["flag"]
    with pytest.raises(ValueError):
        service_for(metadata_path, [row]).read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="SPC"), ("ARRAY",), REFERENCE)


def test_only_explicit_false_decisions_are_released_and_internal_fields_never_leak(metadata_path: Path) -> None:
    rows = [spc_row(sheet_id=f"S{number:02}", flag=flag) for number, flag in enumerate([
        False, "FALSE", 0, 0.0, "0.0", "no", "n", True, "True", "Delete", None, "", "not-false",
    ])]
    result = service_for(metadata_path, rows).read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="SPC"), ("ARRAY",), REFERENCE)
    assert {row["对象编号"] for row in result} == {"S00", "S01", "S02", "S03", "S04", "S05", "S06"}
    assert all("flag" not in row and "internal_policy" not in row for row in result)


def test_hidden_source_is_not_called_even_when_source_and_metadata_would_fail(tmp_path: Path) -> None:
    calls = []
    def forbidden(query, day, factories):
        calls.append(query)
        raise AssertionError("Hidden source must not be read")
    service = PublishedAnomalyService(
        source_reader=forbidden, hidden=lambda domain, product: (domain, product) == ("SPC", "P1"),
        metadata=ApiAlertMetadata(tmp_path / "missing.xlsx"),
    )
    assert service.read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="SPC"), ("ARRAY",), REFERENCE) == []
    assert calls == []


def test_release_records_are_used_without_leaking_public_table_projections(metadata_path: Path) -> None:
    source = service_for(metadata_path, [{"wrong": "public-table-row"}], release_records=pd.DataFrame([spc_row()]))
    result = source.read(AnomalyQuery(event_type="OOS", product="P1", sub_domain="SPC"), ("ARRAY",), REFERENCE)
    assert [row["对象编号"] for row in result] == ["00000000101"]


@pytest.mark.parametrize('boundary', [None, '', ' ', 'NULL', 'null', 'lower', 'LOWER', 'LoWeR'])
def test_unspecified_or_lower_boundary_defaults_to_spec(metadata_path, boundary):
    result = service_for(metadata_path, [spc_row(oos_type=boundary)]).read(
        AnomalyQuery(event_type="OOS", product='P1', sub_domain='SPC'), ('ARRAY',), REFERENCE)
    assert result[0]['命中边界'] == 'spec'


@pytest.mark.parametrize('boundary', ['USL', 'LSL', 'USL/LSL', 'UCL', 'LCL', 'UCL/LCL'])
def test_explicit_boundary_is_preserved(metadata_path, boundary):
    result = service_for(metadata_path, [spc_row(oos_type=boundary)]).read(
        AnomalyQuery(event_type="OOS", product='P1', sub_domain='SPC'), ('ARRAY',), REFERENCE)
    assert result[0]['命中边界'] == boundary
