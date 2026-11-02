"""Public query contracts use isolated published workbooks, never measurements."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.shared_kernel.config import ConfigLoader


def test_spc_query_reuses_release_week_and_parameter_rules(tmp_path, monkeypatch):
    from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
    from src.inline_domain.application.monitor.sheet_alert_query import SheetAlertQuery
    from src.inline_domain.infrastructure.monitor.excel_alarm_store import ExcelAlarmStore

    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    path = tmp_path / "spc.xlsx"
    rows = pd.DataFrame([
        dict(factory="ARRAY", prod_code="P1", step_id="00100", param_name=parameter,
             sheet_id=sheet, sheet_start_time=when, sheet_max=11., sheet_min=5.,
             usl=10., lsl=2., oos_type="USL", flag=flag)
        for sheet, parameter, when, flag in [
            ("S1", "THK", "2026-10-28 10:00:00", False),
            ("S2", "pPa_metric", "2026-10-28 11:00:00", False),
            ("S3", "THK", "2026-10-28 12:00:00", True),
            ("S4", "THK", "2026-11-02 00:00:00", False),
        ]
    ])
    with pd.ExcelWriter(path) as writer:
        rows.to_excel(writer, sheet_name="P1", index=False)
        pd.DataFrame([dict(scope="spc", prod_code="P1", last_generated_at="2026-11-02T09:00:00")]).to_excel(
            writer, sheet_name="__refresh_meta__", index=False,
        )
    original = path.read_bytes()

    def read_workbook(source: str, mtime_ns: int, size: int):
        return pd.read_excel(Path(source), sheet_name=None, dtype=object)

    query = SheetAlertQuery(
        ExcelAlarmReader(ExcelAlarmStore({"spc": path}, workbook_reader=read_workbook)),
        excluded_param_name_contains=("PPA",),
    )
    value = query.read("spc", "P1", date(2026, 11, 2))

    assert value["state"] == "alert"
    assert value["records"]["sheet_id"].tolist() == ["S1"]
    assert value["records"]["step_id"].tolist() == ["00100"]
    assert value["alerts"]["Sheet ID"].tolist() == ["S1"]
    assert "flag" not in value["alerts"]
    assert value["alert_factories"] == ["ARRAY"]
    assert value["metadata"] == {
        "availability": "ready", "published_at": "2026-11-02T09:00:00",
    }
    assert path.read_bytes() == original


def _detail_rows(scope: str) -> pd.DataFrame:
    rows = []
    for index, (when, flag) in enumerate([
        ("2026-10-26 00:00:00", False),
        ("2026-11-01 23:59:59", "0"),
        ("2026-11-02 00:00:00", False),
        ("2026-10-25 23:59:59", False),
        ("2026-10-28 10:00:00", True),
        ("2026-10-28 11:00:00", "Delete"),
        ("2026-10-28 12:00:00", None),
    ]):
        row = dict(
            factory="ARRAY", prod_code="P1", step_id="00100", flag=flag,
            sheet_id=f"S{index}", sheet_start_time=when, param_name="THK",
            sheet_mean=8., sheet_max=11., sheet_min=2., usl=10., lsl=3.,
            oos_type="USL", ooc_type="UCL", ucl=9., lcl=4.,
        )
        if scope == "aoi_tt":
            row.update(tt_name="TDSUM", start_time=when, tt_qty=12.)
        if scope == "aoi_rs":
            row.update(
                chart_kind="sheet" if index == 0 else "lot", point_id=f"P{index}",
                rs_code="12345", value=12., spec=10.,
            )
        rows.append(row)
    return pd.DataFrame(rows)


@pytest.mark.parametrize("scope", ["spc", "ctq", "aoi_tt", "aoi_rs"])
@pytest.mark.parametrize("event_type", ["OOS", "OOC"])
def test_factory_consumes_each_published_event_with_exact_week_and_release_scope(
    tmp_path, monkeypatch, scope, event_type,
):
    from src.inline_domain.composition import build_sheet_alert_query

    path = tmp_path / f"{scope}-{event_type}.xlsx"
    _detail_rows(scope).to_excel(path, sheet_name="P1", index=False)
    before = path.read_bytes()
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda *a, **k: path)
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    monkeypatch.setattr(ConfigLoader, "get_spc_sheet_oos_alert_param_exclusions", lambda: ("PPA",))
    calls = []

    def read(source: str, mtime_ns: int, size: int):
        calls.append((source, mtime_ns, size))
        return pd.read_excel(source, sheet_name=None, dtype=object)

    result = build_sheet_alert_query(scope, event_type, workbook_reader=read).read(
        scope, "P1", date(2026, 11, 2),
    )

    assert result["state"] == "alert"
    assert result["records"]["sheet_id"].tolist() == ["S1", "S0"]
    assert result["metadata"] == {"availability": "ready", "published_at": None}
    assert calls == [(str(path.resolve()), path.stat().st_mtime_ns, path.stat().st_size)]
    assert "flag" not in result["alerts"]
    if scope == "aoi_rs":
        assert result["records"]["chart_kind"].tolist() == ["lot", "sheet"]
        assert result["alerts"]["图类型"].tolist() == ["By Lot", "By Sheet"]
    elif scope in {"spc", "ctq"}:
        expected_boundary = "USL" if event_type == "OOS" else "UCL"
        assert result["alerts"]["超规类型"].tolist() == [expected_boundary, expected_boundary]
    assert path.read_bytes() == before


def test_missing_product_and_file_differ_from_valid_empty_publication(tmp_path, monkeypatch):
    from src.inline_domain.composition import build_sheet_alert_query

    path = tmp_path / "source.xlsx"
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda *a, **k: path)
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    query = build_sheet_alert_query(
        "spc", workbook_reader=lambda source, *_: pd.read_excel(source, sheet_name=None),
    )
    missing = query.read("spc", "P1", date(2026, 11, 2))
    assert missing["state"] == "no_data"
    assert missing["metadata"]["availability"] == "missing"
    _detail_rows("spc").iloc[:0].to_excel(path, sheet_name="P2", index=False)
    missing_product = query.read("spc", "P1", date(2026, 11, 2))
    assert missing_product["metadata"]["availability"] == "missing"
    empty = query.read("spc", "P2", date(2026, 11, 2))
    assert empty["state"] == "ok"
    assert empty["metadata"]["availability"] == "ready"
    assert empty["alerts"].empty


def test_factory_keeps_tt_classification_and_date_exclusion_in_reader(tmp_path, monkeypatch):
    from src.inline_domain.composition import build_sheet_alert_query

    path = tmp_path / "tt.xlsx"
    rows = _detail_rows("aoi_tt").iloc[:2].copy()
    rows.loc[rows.index[0], "tt_name"] = "PPA_X"
    rows.loc[rows.index[1], "factory"] = "OLED"
    rows.to_excel(path, sheet_name="P1", index=False)
    original = path.read_bytes()
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda *a, **k: path)
    monkeypatch.setattr(
        ConfigLoader, "get_inline_data_exclusion",
        lambda: (("OLED",), date(2026, 11, 1), date(2026, 11, 1)),
    )
    result = build_sheet_alert_query(
        "aoi_tt", workbook_reader=lambda source, *_: pd.read_excel(source, sheet_name=None),
    ).read("aoi_tt", "P1", date(2026, 11, 2))
    assert result["state"] == "ok"
    assert result["records"].empty
    assert path.read_bytes() == original


@pytest.mark.parametrize("corrupt", ["product", "flag", "event_time", "required_column"])
def test_invalid_publication_is_not_converted_into_successful_zero_alerts(tmp_path, monkeypatch, corrupt):
    from src.inline_domain.composition import build_sheet_alert_query
    from src.inline_domain.core.monitor.excel_contract import ExcelAlarmReadError

    path = tmp_path / "spc.xlsx"
    rows = _detail_rows("spc").iloc[:1].copy()
    if corrupt == "product":
        rows["prod_code"] = "ANOTHER_PRODUCT"
    elif corrupt == "flag":
        rows["flag"] = "unverified"
    elif corrupt == "event_time":
        rows["sheet_start_time"] = "invalid time"
    else:
        rows = rows.drop(columns="param_name")
    rows.to_excel(path, sheet_name="P1", index=False)
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda *a, **k: path)
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    query = build_sheet_alert_query(
        "spc", workbook_reader=lambda source, *_: pd.read_excel(source, sheet_name=None),
    )
    with pytest.raises(ExcelAlarmReadError):
        query.read("spc", "P1", date(2026, 11, 2))
