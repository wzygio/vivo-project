"""真实 XLSX -> 默认数据适配器 -> 缓存服务的版本更新链路；无生产 I/O。"""

from dataclasses import replace

import pandas as pd
import pytest

from src.equipment_domain.application import parts_service
from src.equipment_domain.infrastructure.report_data_adapter import PartsReportDataAdapter


def write_version(path, sheet, value):
    source = pd.DataFrame([{
        "厂别": "old", "备件类型": "Mask", "设备类型": "CVD",
        "膜层": "SIN", "制程": "DEPO", "寿命规格": 1000,
        "站点": "21200", "机台号": "M1", "测量值": value,
        "进度": 30, "测量时间": "2026-09-21",
    }])
    with pd.ExcelWriter(path) as writer:
        source.to_excel(writer, sheet_name=sheet, index=False)


@pytest.fixture
def isolated_port(monkeypatch):
    spec = pd.DataFrame([{
        "厂别": "Array", "备件类型": "Target", "设备类型": "PVD",
        "膜层": "MO", "制程": "DEPO", "寿命规格": 1000,
        "站点": "S1", "机台号-腔室": "PVD1", "参数名称": "%LIFE%",
    }])
    port = PartsReportDataAdapter(None)
    monkeypatch.setattr(port, "load_baseline", lambda _: spec.copy())
    monkeypatch.setattr(port, "load_snapshots", lambda *args, **kwargs: (pd.DataFrame(), pd.DataFrame()))
    runtime = parts_service.get_equipment_runtime_config()
    monkeypatch.setattr(parts_service, "get_equipment_runtime_config", lambda: replace(runtime, cvd_baseline_date=""))
    parts_service.PartsReportService.fetch_report_payload.clear()
    yield port
    parts_service.PartsReportService.fetch_report_payload.clear()


def fetch(path, port, day):
    context = parts_service.build_parts_report_cache_context("unused.csv", path)
    context["as_of_date"] = day
    return parts_service.PartsReportService.get_report_data(
        None, "unused.csv", "cvd-sheet-version-test", _data_port=port,
        cvd_path=str(path), **context,
    ).report_df.query("设备类型 == 'CVD'").reset_index(drop=True)


def test_same_workbook_new_sheet_resets_cached_source_value_and_origin(tmp_path, isolated_port):
    path = tmp_path / "cvd.xlsx"
    write_version(path, "OLED-1001", 960)
    first = fetch(path, isolated_port, "2026-10-13")
    assert first.iloc[0]["测量值"] == 972
    assert first.iloc[0]["预警状态"] == "预警"  # CVD 不执行 >95% 归零。
    write_version(path, "OLED-1012", 200)
    latest = fetch(path, isolated_port, "2026-10-13")
    assert latest.iloc[0]["厂别"] == "OLED"
    assert latest.iloc[0]["测量值"] == 201
    assert latest.iloc[0]["测量时间"] == pd.Timestamp("2026-10-13")
    assert latest.iloc[0]["使用进度"] == pytest.approx(20.1)
    pd.testing.assert_frame_equal(latest, fetch(path, isolated_port, "2026-10-13"))


def test_service_report_date_controls_cross_year_sheet_resolution(tmp_path, isolated_port):
    path = tmp_path / "cvd.xlsx"
    write_version(path, "ARRAY-1231", 200)
    result = fetch(path, isolated_port, "2027-01-01")
    assert result.iloc[0]["厂别"] == "ARRAY"
    assert result.iloc[0]["测量值"] == 201


def test_oled_and_array_cvd_sheets_merge_with_fractional_year_and_factory_filters(
    tmp_path, isolated_port,
):
    from app.sections.equipment_domain.parts_filters import apply_parts_filters, build_cascading_filter_options
    from app.sections.equipment_domain.parts_dashboard import _prepare_parts_table

    path = tmp_path / "cvd.xlsx"
    oled = pd.DataFrame([{
        "备件类型": "Mask", "设备类型": "CVD", "膜层": "SIN", "制程": "DEPO",
        "寿命规格": 1000, "站点": "21200", "机台号": "M1",
        "测量值": 200, "进度": 30,
    }])
    array = oled.copy()
    array["备件类型"] = "Backplate"
    array["寿命规格"] = "1.5年"
    array["测量值"] = 45351  # 2024-02-29
    array["进度"] = None
    with pd.ExcelWriter(path) as writer:
        oled.to_excel(writer, sheet_name="OLED-1012 ", index=False, startrow=6, startcol=2)
        array.to_excel(writer, sheet_name="ARRAY-1012", index=False, startrow=1)
    result = fetch(path, isolated_port, "2026-10-13")
    assert len(result) == 2
    filtered = apply_parts_filters(
        result, selected_factories=["ARRAY"], selected_equipment_types=["CVD"],
        selected_part_types=[],
    )
    assert len(filtered) == 1
    assert filtered.iloc[0]["测量值"] == "2024-02-29"
    assert filtered.iloc[0]["使用进度"] > 100
    assert filtered.iloc[0]["预警状态"] == "超规"
    factories, equipment, parts = build_cascading_filter_options(
        result, selected_factories=["ARRAY"], selected_equipment_types=["CVD"],
    )
    assert factories == ["ARRAY", "OLED"]
    assert equipment == ["CVD"]
    assert parts == ["Backplate"]
    public = _prepare_parts_table(result)
    assert "1.5年" in public["寿命规格"].tolist()
    assert "2024-02-29" in public["测量值"].tolist()
