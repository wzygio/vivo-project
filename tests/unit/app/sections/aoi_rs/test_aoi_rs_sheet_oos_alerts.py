"""AOI_RS 单片异常（Sheet OOS）预警测试：构建口径、渲染过滤、加载降级。

口径见 docs/PRD/PRD-2026-08-25-Inline自动预警中心.md：
仅报上一 ISO 周内 flag=FALSE 的明细；历史行无 sheet_start_time 不参与。
"""

from contextlib import nullcontext
from datetime import date
from pathlib import Path

import pandas as pd

from app.sections.inline_domain.aoi_rs import aoi_rs_dashboard
from app.sections.inline_domain.aoi_rs.aoi_rs_dashboard import (
    AOI_RS_ALERT_COLUMNS,
    build_aoi_rs_sheet_oos_alerts,
    render_aoi_rs_indicator_sections,
    render_aoi_rs_sheet_oos_alert_indicator_sections,
)
from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range
from src.inline_domain.core.shared.sheet_oos_decoration import SheetOosDecorationReadError

REFERENCE_DATE = date(2026, 8, 25)  # 周二：上一 ISO 周为 [周一 00:00, 本周一 00:00)


def _decoration_row(
    *,
    point_id: str,
    chart_kind: str = "sheet",
    rs_code: str = "A1PPS",
    flag: object = False,
    sheet_start_time: object = None,
) -> dict[str, object]:
    return {
        "prod_code": "M678",
        "factory": "ARRAY",
        "step_id": "11629",
        "rs_code": rs_code,
        "chart_kind": chart_kind,
        "point_id": point_id,
        "value": 5.0,
        "spec": 3.0,
        "sheet_start_time": sheet_start_time,
        "flag": flag,
    }


def _decoration_df() -> pd.DataFrame:
    week_start, week_end = previous_iso_week_range(REFERENCE_DATE)
    in_week = week_start + pd.Timedelta(days=1)
    return pd.DataFrame(
        [
            # flag=FALSE + 上周 → 命中（sheet 图）
            _decoration_row(point_id="S1", sheet_start_time=in_week),
            # flag 字符串 "FALSE" + 上周 → 命中（lot 图，时间更晚应排前）
            _decoration_row(
                point_id="L1",
                chart_kind="lot",
                flag="FALSE",
                sheet_start_time=in_week + pd.Timedelta(hours=6),
            ),
            # 周起点边界（含）→ 命中
            _decoration_row(point_id="S0", sheet_start_time=week_start),
            # flag=True（截断修饰）→ 不命中
            _decoration_row(point_id="S2", flag=True, sheet_start_time=in_week),
            # flag=Delete → 不命中
            _decoration_row(point_id="S3", flag="Delete", sheet_start_time=in_week),
            # 本周（周终点边界，不含）→ 不命中
            _decoration_row(point_id="S4", sheet_start_time=week_end),
            # 上上周（窗口外）→ 不命中
            _decoration_row(point_id="S5", sheet_start_time=week_start - pd.Timedelta(days=1)),
            # 历史行无时间 → 不参与
            _decoration_row(point_id="S6", sheet_start_time=None),
            # 时间解析失败 → 不参与
            _decoration_row(point_id="S7", sheet_start_time="not-a-time"),
        ]
    )


def test_build_sheet_oos_alerts_keeps_only_false_flag_in_previous_iso_week() -> None:
    alerts_df = build_aoi_rs_sheet_oos_alerts(
        _decoration_df(),
        reference_date=REFERENCE_DATE,
    )

    assert alerts_df.columns.tolist() == AOI_RS_ALERT_COLUMNS
    # 按超规时间倒序：L1（in_week+6h）> S1（in_week）> S0（week_start）
    assert alerts_df["点位ID"].tolist() == ["L1", "S1", "S0"]


def test_build_sheet_oos_alerts_maps_chart_kind_and_stringifies_time() -> None:
    alerts_df = build_aoi_rs_sheet_oos_alerts(
        _decoration_df(),
        reference_date=REFERENCE_DATE,
    )

    assert set(alerts_df["图类型"]) == {"By Sheet", "By Lot"}
    assert all(isinstance(value, str) for value in alerts_df["超规时间"])
    assert alerts_df["厂别"].tolist() == ["ARRAY", "ARRAY", "ARRAY"]
    assert alerts_df["站点"].tolist() == ["11629", "11629", "11629"]


def test_build_sheet_oos_alerts_returns_empty_columns_for_none_or_empty() -> None:
    for source in (None, pd.DataFrame()):
        alerts_df = build_aoi_rs_sheet_oos_alerts(source, reference_date=REFERENCE_DATE)
        assert alerts_df.empty
        assert alerts_df.columns.tolist() == AOI_RS_ALERT_COLUMNS


def _alert_frames() -> dict[str, pd.DataFrame]:
    details = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "v": 1},
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A2CIP", "v": 2},
        ]
    )
    pass_through = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11629", "sheet_id": "S1"},
            {"factory": "TP", "step_id": "43629", "sheet_id": "G1"},
        ]
    )
    indicators = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A1PPS", "code_desc": "d1"},
            {"factory": "ARRAY", "step_id": "11629", "rs_code": "A2CIP", "code_desc": "d2"},
        ]
    )
    return {
        "rs_details_df": details,
        "pass_through_df": pass_through,
        "spec_df": pd.DataFrame(),
        "indicators_df": indicators,
        "lot_points_df": details,
        "sheet_points_df": details,
    }


def _sample_alerts_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "厂别": "ARRAY",
                "站点": "11629",
                "RS Code": "A1PPS",
                "图类型": "By Sheet",
                "点位ID": "S1",
                "超规时间": "2026-08-18 08:00:00",
                "实测值": 5.0,
                "规格上限": 3.0,
            }
        ]
    )


def test_render_alert_indicator_sections_filters_frames_by_alert_keys(monkeypatch) -> None:
    captured: dict[str, object] = {}
    expander_titles: list[str] = []

    monkeypatch.setattr(
        aoi_rs_dashboard.st,
        "expander",
        lambda title, **_kw: expander_titles.append(title) or nullcontext(),
    )
    monkeypatch.setattr(aoi_rs_dashboard.st, "caption", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        aoi_rs_dashboard,
        "render_aoi_rs_indicator_sections",
        lambda **kwargs: captured.update(kwargs),
    )

    render_aoi_rs_sheet_oos_alert_indicator_sections(
        alerts_df=_sample_alerts_df(),
        **_alert_frames(),
        end_date=REFERENCE_DATE,
    )

    assert expander_titles == ["🚨 单片异常预警指标图像（1 个指标）"]
    # 指标帧按 厂别+站点+RS Code 精确过滤
    assert captured["rs_details_df"]["rs_code"].tolist() == ["A1PPS"]
    assert captured["indicators_df"]["rs_code"].tolist() == ["A1PPS"]
    assert captured["lot_points_df"]["rs_code"].tolist() == ["A1PPS"]
    assert captured["sheet_points_df"]["rs_code"].tolist() == ["A1PPS"]
    # 过货帧无 rs_code，仅按 厂别+站点 过滤
    assert captured["pass_through_df"]["sheet_id"].tolist() == ["S1"]
    # 预警区图表 key 与主筛选区隔离
    assert captured["chart_key_prefix"] == "aoi_rs_alert"


def test_render_alert_indicator_sections_skips_when_no_alerts(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        aoi_rs_dashboard.st,
        "expander",
        lambda *_a, **_kw: calls.append("expander") or nullcontext(),
    )
    monkeypatch.setattr(
        aoi_rs_dashboard,
        "render_aoi_rs_indicator_sections",
        lambda **_kw: calls.append("render"),
    )

    render_aoi_rs_sheet_oos_alert_indicator_sections(
        alerts_df=pd.DataFrame(),
        **_alert_frames(),
        end_date=REFERENCE_DATE,
    )

    assert calls == []


def test_render_sections_chart_keys_are_isolated_by_prefix(monkeypatch) -> None:
    rendered_keys: list[str | None] = []

    monkeypatch.setattr(
        aoi_rs_dashboard.st,
        "expander",
        lambda *_a, **_kw: nullcontext(),
    )
    monkeypatch.setattr(
        aoi_rs_dashboard.st,
        "columns",
        lambda n, **_kw: [nullcontext() for _ in range(n if isinstance(n, int) else len(n))],
    )
    monkeypatch.setattr(aoi_rs_dashboard.st, "subheader", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        aoi_rs_dashboard.st,
        "plotly_chart",
        lambda fig, **kw: rendered_keys.append(kw.get("key")),
    )

    frames = _alert_frames()
    frames["rs_details_df"] = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-08-09 08:00"),
                "sheet_id": "S1",
                "lot_id": "L1",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "code_qty": 3,
            }
        ]
    )
    frames["pass_through_df"] = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-08-09 07:00"),
                "sheet_id": "S1",
                "step_id": "11629",
            }
        ]
    )
    frames["lot_points_df"] = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "lot_id": "L1",
                "rs_qty": 3,
                "sheet_qty": 1,
                "value": 3.0,
                "first_start_time": pd.Timestamp("2026-08-09 08:00"),
            }
        ]
    )
    frames["sheet_points_df"] = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "sheet_id": "S1",
                "rs_qty": 3,
                "first_start_time": pd.Timestamp("2026-08-09 08:00"),
            }
        ]
    )
    render_aoi_rs_indicator_sections(
        **frames,
        end_date=REFERENCE_DATE,
        chart_key_prefix="aoi_rs_report",
    )
    report_keys = list(rendered_keys)
    render_aoi_rs_indicator_sections(
        **frames,
        end_date=REFERENCE_DATE,
        chart_key_prefix="aoi_rs_alert",
    )
    alert_keys = rendered_keys[len(report_keys):]

    assert report_keys and all(key and key.startswith("aoi_rs_report") for key in report_keys)
    assert alert_keys and all(key and key.startswith("aoi_rs_alert") for key in alert_keys)
    assert not set(report_keys) & set(alert_keys)


def test_load_decoration_degrades_to_none_on_read_error(monkeypatch, tmp_path: Path) -> None:
    def _raise(*_args, **_kwargs):
        raise SheetOosDecorationReadError("enterprise-encrypted workbook unreadable")

    monkeypatch.setattr(aoi_rs_dashboard, "load_sheet_oos_decoration", _raise)

    assert aoi_rs_dashboard._load_aoi_rs_sheet_oos_decoration(tmp_path, "M678") is None


def test_load_decoration_returns_loaded_frame(monkeypatch, tmp_path: Path) -> None:
    loaded = pd.DataFrame([{"rs_code": "A1PPS", "flag": False}])
    monkeypatch.setattr(
        aoi_rs_dashboard,
        "load_sheet_oos_decoration",
        lambda *_args, **_kwargs: loaded,
    )

    assert aoi_rs_dashboard._load_aoi_rs_sheet_oos_decoration(tmp_path, "M678") is loaded
