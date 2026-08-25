"""AOI_TT 单片异常（Sheet OOS）预警测试：预警构建、工作簿加载降级、预警图像渲染。"""

from contextlib import nullcontext
from datetime import date

import pandas as pd
import pytest

from app.sections.inline_domain.aoi_tt import aoi_tt_dashboard
from app.sections.inline_domain.aoi_tt.aoi_tt_dashboard import (
    build_aoi_tt_sheet_oos_alerts,
    load_aoi_tt_oos_decoration,
    render_aoi_tt_sheet_oos_alert_indicator_sections,
)
from src.inline_domain.core.shared.sheet_oos_decoration import SheetOosDecorationReadError

# 参考日 2026-08-25（周二）：上一 ISO 周 = [2026-08-17 00:00, 2026-08-24 00:00)
REF = date(2026, 8, 25)

EXPECTED_COLUMNS = ["厂别", "站点", "TT名称", "Sheet ID", "超规时间", "TT数量", "规格上限"]


def _decoration_row(
    *,
    factory: str = "ARRAY",
    step_id: str = "11620",
    tt_name: str = "TDSUM",
    sheet_id: str = "S1",
    start_time: object = "2026-08-20 10:00:00",
    tt_qty: int = 5,
    usl: float = 3.0,
    flag: object = False,
) -> dict:
    return {
        "factory": factory,
        "prod_code": "M678",
        "step_id": step_id,
        "tt_name": tt_name,
        "sheet_id": sheet_id,
        "lot_id": "L1",
        "start_time": start_time,
        "tt_qty": tt_qty,
        "usl": usl,
        "flag": flag,
    }


# ---------------------------------------------------------------------------
# build_aoi_tt_sheet_oos_alerts
# ---------------------------------------------------------------------------


def test_build_alerts_keeps_false_flag_in_previous_iso_week() -> None:
    df = pd.DataFrame([_decoration_row()])

    alerts = build_aoi_tt_sheet_oos_alerts(df, reference_date=REF)

    assert len(alerts) == 1
    assert list(alerts.columns) == EXPECTED_COLUMNS
    row = alerts.iloc[0]
    assert row["厂别"] == "ARRAY"
    assert row["站点"] == "11620"
    assert row["TT名称"] == "TDSUM"
    assert row["Sheet ID"] == "S1"
    assert row["TT数量"] == 5
    assert row["规格上限"] == 3.0
    # 超规时间已转 str，不再是 Timestamp
    assert isinstance(row["超规时间"], str)
    assert row["超规时间"].startswith("2026-08-20")


def test_build_alerts_excludes_true_delete_and_out_of_window() -> None:
    df = pd.DataFrame(
        [
            _decoration_row(sheet_id="S1", flag=True),
            _decoration_row(sheet_id="S2", flag="Delete"),
            _decoration_row(sheet_id="S3", flag=False, start_time="2026-08-25 08:00:00"),  # 本周
            _decoration_row(sheet_id="S4", flag=False, start_time="2026-08-16 23:59:00"),  # 上上周
            _decoration_row(sheet_id="S5", flag=False, start_time="not-a-time"),  # 解析失败
            _decoration_row(sheet_id="S6", flag=False),  # 唯一命中
        ]
    )

    alerts = build_aoi_tt_sheet_oos_alerts(df, reference_date=REF)

    assert list(alerts["Sheet ID"]) == ["S6"]


def test_build_alerts_accepts_datetime_and_string_time_column() -> None:
    df = pd.DataFrame(
        [
            _decoration_row(sheet_id="S1", start_time=pd.Timestamp("2026-08-18 09:00")),
            _decoration_row(sheet_id="S2", start_time="2026-08-19 11:30:00"),
        ]
    )

    alerts = build_aoi_tt_sheet_oos_alerts(df, reference_date=REF)

    # 按超规时间倒序
    assert list(alerts["Sheet ID"]) == ["S2", "S1"]


def test_build_alerts_none_or_empty_returns_empty_display_frame() -> None:
    for source in (None, pd.DataFrame(), pd.DataFrame([_decoration_row(flag=True)])):
        alerts = build_aoi_tt_sheet_oos_alerts(source, reference_date=REF)
        assert alerts.empty
        assert list(alerts.columns) == EXPECTED_COLUMNS


# ---------------------------------------------------------------------------
# load_aoi_tt_oos_decoration（缓存键含 mtime/size，异常降级为 None）
# ---------------------------------------------------------------------------


def test_load_decoration_returns_none_on_read_error(monkeypatch, tmp_path) -> None:
    workbook = tmp_path / "aoi_tt_sheet_oos_decoration.xlsx"
    workbook.write_bytes(b"not-a-real-xlsx")

    monkeypatch.setattr(aoi_tt_dashboard, "resolve_product_resource_dir", lambda _prod: tmp_path)

    def _raise(*_args, **_kwargs):
        raise SheetOosDecorationReadError("unreadable decoration file")

    monkeypatch.setattr(aoi_tt_dashboard, "load_sheet_oos_decoration", _raise)

    assert load_aoi_tt_oos_decoration("M678") is None


def test_load_decoration_returns_dataframe_on_success(monkeypatch, tmp_path) -> None:
    workbook = tmp_path / "aoi_tt_sheet_oos_decoration.xlsx"
    workbook.write_bytes(b"fake-bytes")

    monkeypatch.setattr(aoi_tt_dashboard, "resolve_product_resource_dir", lambda _prod: tmp_path)
    expected = pd.DataFrame([_decoration_row()])
    monkeypatch.setattr(
        aoi_tt_dashboard, "load_sheet_oos_decoration", lambda *_a, **_kw: expected
    )

    loaded = load_aoi_tt_oos_decoration("M678")

    assert loaded is not None
    assert len(loaded) == 1


def test_load_decoration_missing_file_returns_none(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(aoi_tt_dashboard, "resolve_product_resource_dir", lambda _prod: tmp_path)
    called = {"count": 0}

    def _counting_loader(*_args, **_kwargs):
        called["count"] += 1
        return pd.DataFrame()

    monkeypatch.setattr(aoi_tt_dashboard, "load_sheet_oos_decoration", _counting_loader)

    assert load_aoi_tt_oos_decoration("M678") is None
    # 文件不存在时直接降级，不触发工作簿读取
    assert called["count"] == 0


# ---------------------------------------------------------------------------
# render_aoi_tt_sheet_oos_alert_indicator_sections
# ---------------------------------------------------------------------------


def _alert_df() -> pd.DataFrame:
    return pd.DataFrame(
        [{"厂别": "ARRAY", "站点": "11620", "TT名称": "TDSUM"}]
    )


def _tt_details() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 08:00"), "sheet_id": "S1", "lot_id": "L1", "step_id": "11620", "tt_name": "TDSUM", "tt_qty": 3},
            {"factory": "ARRAY", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 09:00"), "sheet_id": "S2", "lot_id": "L2", "step_id": "11620", "tt_name": "DSUM_L", "tt_qty": 1},
            {"factory": "TP", "prod_code": "M678", "start_time": pd.Timestamp("2026-08-09 10:00"), "sheet_id": "G1", "lot_id": "L9", "step_id": "43620", "tt_name": "TOTAL_O_L", "tt_qty": 2},
        ]
    )


def _indicators() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11620", "tt_name": "TDSUM"},
            {"prod_code": "M678", "factory": "ARRAY", "step_id": "11620", "tt_name": "DSUM_L"},
            {"prod_code": "M678", "factory": "TP", "step_id": "43620", "tt_name": "TOTAL_O_L"},
        ]
    )


def _install_fake_st(monkeypatch) -> dict:
    captured = {"expander_titles": [], "figures": [], "warnings": [], "infos": []}

    class _FakeExpander:
        def __init__(self, title, expanded):
            self.title = title
            self.expanded = expanded

        def __enter__(self):
            captured["expander_titles"].append(self.title)
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        aoi_tt_dashboard.st,
        "expander",
        lambda title, expanded=False, **_kw: _FakeExpander(title, expanded),
    )
    monkeypatch.setattr(
        aoi_tt_dashboard.st,
        "plotly_chart",
        lambda fig, **_kw: captured["figures"].append(fig),
    )
    monkeypatch.setattr(
        aoi_tt_dashboard.st,
        "columns",
        lambda n, **_kw: [nullcontext() for _ in range(n if isinstance(n, int) else len(n))],
    )
    monkeypatch.setattr(aoi_tt_dashboard.st, "subheader", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        aoi_tt_dashboard.st, "warning", lambda msg, **_kw: captured["warnings"].append(msg)
    )
    monkeypatch.setattr(
        aoi_tt_dashboard.st, "info", lambda msg, **_kw: captured["infos"].append(msg)
    )
    return captured


def test_render_alert_sections_empty_alerts_render_nothing(monkeypatch) -> None:
    captured = _install_fake_st(monkeypatch)

    render_aoi_tt_sheet_oos_alert_indicator_sections(
        alerts_df=pd.DataFrame(columns=EXPECTED_COLUMNS),
        tt_details_df=_tt_details(),
        spec_df=pd.DataFrame(),
        indicators_df=_indicators(),
        end_date=date(2026, 8, 10),
    )

    assert captured["expander_titles"] == []
    assert captured["figures"] == []
    assert captured["warnings"] == []


def test_render_alert_sections_filters_indicators_exactly(monkeypatch) -> None:
    captured = _install_fake_st(monkeypatch)

    render_aoi_tt_sheet_oos_alert_indicator_sections(
        alerts_df=_alert_df(),
        tt_details_df=_tt_details(),
        spec_df=pd.DataFrame(),
        indicators_df=_indicators(),
        end_date=date(2026, 8, 10),
    )

    titles = captured["expander_titles"]
    # 外层预警图像 Expander（精确计数 1 个指标）+ 内层 1 个 TT Expander
    assert titles[0] == "🚨 单片异常预警指标图像（1 个指标）"
    inner_titles = titles[1:]
    assert len(inner_titles) == 1
    assert "TDSUM" in inner_titles[0]
    assert "11620" in inner_titles[0]
    # 只有命中的 TDSUM 出图（3 张），DSUM_L 与 TOTAL_O_L 不出图
    assert len(captured["figures"]) == 3
    assert not captured["warnings"]


def test_render_alert_sections_warns_when_no_matching_report_data(monkeypatch) -> None:
    captured = _install_fake_st(monkeypatch)
    alerts = pd.DataFrame([{"厂别": "CELL", "站点": "99999", "TT名称": "UNKNOWN"}])

    render_aoi_tt_sheet_oos_alert_indicator_sections(
        alerts_df=alerts,
        tt_details_df=_tt_details(),
        spec_df=pd.DataFrame(),
        indicators_df=_indicators(),
        end_date=date(2026, 8, 10),
    )

    assert captured["figures"] == []
    assert len(captured["warnings"]) == 1
    # 无匹配数据时不渲染预警图像 Expander
    assert captured["expander_titles"] == []
