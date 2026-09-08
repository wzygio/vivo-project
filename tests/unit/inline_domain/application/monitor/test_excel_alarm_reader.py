from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
from src.inline_domain.infrastructure.monitor import excel_alarm_store as module
from src.inline_domain.infrastructure.monitor.excel_alarm_store import ExcelAlarmReadError, ExcelAlarmStore, clear_excel_alarm_cache


@pytest.fixture(autouse=True)
def cache_boundary():
    clear_excel_alarm_cache()
    yield
    clear_excel_alarm_cache()


def detail(flag=False):
    return pd.DataFrame([dict(factory="ARRAY", prod_code="M626", step_id="1100", param_name="CD", sheet_id="S1", sheet_start_time="2026-09-08", sheet_max=3, sheet_min=1, sheet_mean=2, usl=2.5, lsl=1.5, ucl=1.9, lcl=1.1, oos_type="LSL", ooc_type="UCL", flag=flag)])


def write_book(path, frame, **extra):
    with pd.ExcelWriter(path) as writer:
        for name, value in {"M626": frame, **extra}.items():
            value.to_excel(writer, sheet_name=name, index=False)


def reader(path, alarm="oos", scope="spc"):
    return ExcelAlarmReader(ExcelAlarmStore({scope: path}), alarm)


@pytest.mark.parametrize("flag,expected", [(False, 1), ("False", 1), (0, 1), (0.0, 1), (True, 0), ("TRUE", 0), (1, 0), ("Delete", 0), (None, 0)])
def test_product_sheet_flags_are_authoritative(tmp_path, flag, expected):
    path = tmp_path / "alarms.xlsx"
    write_book(path, detail(flag).rename(columns={"flag": "Flag"}), M626__flags=pd.DataFrame({"flag": [True]}))
    result = reader(path).read_product("spc", "M626")
    assert result.source == "excel"
    assert len(result.alerts_df) == expected
    assert result.decorated_df.iloc[0]["event_time"] == pd.Timestamp("2026-09-08")
    assert result.decorated_df.iloc[0]["observed_value"] == 1


def test_missing_sheet_and_empty_are_distinct(tmp_path):
    path = tmp_path / "alarms.xlsx"
    service = reader(path)
    assert service.read_product("spc", "M626").source == "missing"
    write_book(path, detail().iloc[:0])
    assert service.read_product("spc", "M626").source == "excel"
    assert service.read_product("spc", "M678").source == "missing"
    assert not list(tmp_path.glob("*.parquet"))


def test_workbook_cache_shared_and_manual_clear_reads_again(tmp_path, monkeypatch):
    path = tmp_path / "alarms.xlsx"
    write_book(path, detail())
    original = module.pd.read_excel
    calls = []
    def counted(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)
    monkeypatch.setattr(module.pd, "read_excel", counted)
    reader(path).read_product("spc", "M626")
    ooc = reader(path, "ooc").read_product("spc", "M626")
    assert ooc.alerts_df.iloc[0]["alarm_type"] == "OOC"
    assert len(calls) == 1
    clear_excel_alarm_cache()
    reader(path).read_product("spc", "M626")
    assert len(calls) == 2


def test_changed_configuration_invalidates_cache(tmp_path):
    path = tmp_path / "alarms.xlsx"
    write_book(path, detail())
    service = reader(path)
    signature = service.source_signature(["M626"], ["spc"])
    assert len(service.read_product("spc", "M626").alerts_df) == 1
    write_book(path, detail(True))
    assert service.source_signature(["M626"], ["spc"]) != signature
    assert service.read_product("spc", "M626").alerts_df.empty


@pytest.mark.parametrize("bad_frame", [detail().drop(columns="sheet_id"), detail("garbage"), detail().assign(sheet_start_time="invalid")])
def test_invalid_schema_is_not_silent_zero(tmp_path, bad_frame):
    path = tmp_path / "alarms.xlsx"
    write_book(path, bad_frame)
    with pytest.raises(ExcelAlarmReadError):
        reader(path).read_product("spc", "M626")


def test_read_errors_are_stable_and_never_create_data(tmp_path, monkeypatch):
    path = tmp_path / "broken.xlsx"
    path.write_bytes(b"not an xlsx")
    def fail(*args):
        raise RuntimeError("COM unavailable")
    monkeypatch.setattr(module.excel_tools, "_read_all_sheets_via_com", fail)
    with pytest.raises(ExcelAlarmReadError, match="Unable to read"):
        reader(path).read_product("spc", "M626")
    assert path.read_bytes() == b"not an xlsx"


def test_aoi_rs_point_projection_and_refresh_metadata(tmp_path):
    path = tmp_path / "alarms.xlsx"
    frame = pd.DataFrame([dict(factory="ARRAY", prod_code="M626", step_id="100", rs_code="P", point_id="LOT1", chart_kind="lot", sheet_start_time="2026-09-08", value=2, spec=1, flag=False)])
    meta = pd.DataFrame([dict(scope="aoi_rs", prod_code="M626", last_generated_at="2026-09-08T07:00:00")])
    write_book(path, frame, __refresh_meta__=meta)
    result = reader(path, scope="aoi_rs").read_product("aoi_rs", "M626")
    assert result.alerts_df.iloc[0]["item_id"] == "LOT1"
    assert result.alerts_df.iloc[0]["chart_kind"] == "lot"
    assert result.refreshed_at == pd.Timestamp("2026-09-08 07:00")
