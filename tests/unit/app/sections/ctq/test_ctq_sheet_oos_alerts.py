from contextlib import nullcontext
from datetime import date
from pathlib import Path

import pandas as pd

from app.sections.inline_domain.ctq import ctq_dashboard
from app.sections.inline_domain.ctq.ctq_dashboard import (
    build_ctq_sheet_oos_alerts,
    render_ctq_sheet_oos_alert_indicator_sections,
)
from src.inline_domain.application.shared.sheet_oos_decoration_service import (
    SheetOosDecorationResult,
)

# 2026-08-25 为周二：上一 ISO 周 = [2026-08-17, 2026-08-24)
REFERENCE_DATE = date(2026, 8, 25)
EXPECTED_COLUMNS = ["厂别", "站点", "参数名称", "Sheet ID", "超规时间", "超规类型"]


def _make_decoration_result(detail_df: pd.DataFrame) -> SheetOosDecorationResult:
    return SheetOosDecorationResult(
        raw_measurements_df=pd.DataFrame(),
        decoration_df=detail_df,
        decoration_path=Path("ctq_sheet_oos_decoration.xlsx"),
        decoration_sheet="CTQ",
    )


def _detail_row(
    flag: object,
    sheet_start_time: str,
    *,
    sheet_id: str = "CTQ001",
    factory: str = "ARRAY",
    step_id: str = "12140",
    param_name: str = "THK",
    oos_type: str = "USL",
) -> dict[str, object]:
    return {
        "factory": factory,
        "step_id": step_id,
        "param_name": param_name,
        "sheet_id": sheet_id,
        "sheet_start_time": sheet_start_time,
        "oos_type": oos_type,
        "flag": flag,
    }


def test_build_ctq_sheet_oos_alerts_returns_false_flag_rows_in_previous_iso_week() -> None:
    detail_df = pd.DataFrame(
        [
            _detail_row(False, "2026-08-18 08:00:00", sheet_id="CTQ001"),
            _detail_row(False, "2026-08-20 09:30:00", sheet_id="CTQ002"),
        ]
    )

    alerts_df = build_ctq_sheet_oos_alerts(
        _make_decoration_result(detail_df), REFERENCE_DATE
    )

    assert list(alerts_df.columns) == EXPECTED_COLUMNS
    # 按超规时间倒序
    assert alerts_df["Sheet ID"].tolist() == ["CTQ002", "CTQ001"]
    assert alerts_df["厂别"].tolist() == ["ARRAY", "ARRAY"]
    assert alerts_df["站点"].tolist() == ["12140", "12140"]
    assert alerts_df["参数名称"].tolist() == ["THK", "THK"]
    assert alerts_df["超规类型"].tolist() == ["USL", "USL"]
    assert all(isinstance(value, str) for value in alerts_df["超规时间"])
    assert alerts_df["超规时间"].iloc[0].startswith("2026-08-20")


def test_build_ctq_sheet_oos_alerts_excludes_non_false_flags_and_out_of_window_rows() -> None:
    detail_df = pd.DataFrame(
        [
            _detail_row(True, "2026-08-18 08:00:00"),
            _detail_row("Delete", "2026-08-18 08:00:00"),
            _detail_row(False, "2026-08-24 00:00:00"),  # 本周一（半开区间上界，排除）
            _detail_row(False, "2026-08-16 23:59:59"),  # 上上周（排除）
        ]
    )

    alerts_df = build_ctq_sheet_oos_alerts(
        _make_decoration_result(detail_df), REFERENCE_DATE
    )

    assert alerts_df.empty
    assert list(alerts_df.columns) == EXPECTED_COLUMNS


def test_build_ctq_sheet_oos_alerts_returns_empty_table_for_none_result() -> None:
    alerts_df = build_ctq_sheet_oos_alerts(None, REFERENCE_DATE)

    assert alerts_df.empty
    assert list(alerts_df.columns) == EXPECTED_COLUMNS


def test_render_ctq_sheet_oos_alert_indicator_sections_skips_empty_alerts(monkeypatch) -> None:
    monkeypatch.setattr(
        ctq_dashboard,
        "render_ctq_indicator_sections",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not render without alerts")),
    )
    monkeypatch.setattr(
        ctq_dashboard.st,
        "expander",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not expand without alerts")),
    )

    render_ctq_sheet_oos_alert_indicator_sections(
        alerts_df=pd.DataFrame(columns=EXPECTED_COLUMNS),
        sheet_features_df=pd.DataFrame([{"factory": "A", "step_id": "S1", "param_name": "P1"}]),
        raw_measurements_df=pd.DataFrame(),
    )


def test_render_ctq_sheet_oos_alert_indicator_sections_filters_exact_indicator_keys(monkeypatch) -> None:
    alerts_df = pd.DataFrame(
        [
            {
                "厂别": "ARRAY",
                "站点": "S1",
                "参数名称": "P1",
                "Sheet ID": "CTQ001",
                "超规时间": "2026-08-20 09:30:00",
                "超规类型": "USL",
            },
            {
                "厂别": "ARRAY",
                "站点": "S1",
                "参数名称": "P1",
                "Sheet ID": "CTQ002",
                "超规时间": "2026-08-21 09:30:00",
                "超规类型": "LSL",
            },
        ]
    )
    sheet_features_df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "S1", "param_name": "P1", "value": 1},
            {"factory": "ARRAY", "step_id": "S1", "param_name": "P2", "value": 2},
            {"factory": "ARRAY", "step_id": "S2", "param_name": "P1", "value": 3},
            {"factory": "OLED", "step_id": "S1", "param_name": "P1", "value": 4},
        ]
    )
    expander_titles: list[str] = []
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        ctq_dashboard.st,
        "expander",
        lambda title, **kwargs: expander_titles.append(title) or nullcontext(),
    )
    monkeypatch.setattr(ctq_dashboard.st, "caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        ctq_dashboard,
        "render_ctq_indicator_sections",
        lambda **kwargs: captured.update(kwargs),
    )

    render_ctq_sheet_oos_alert_indicator_sections(
        alerts_df=alerts_df,
        sheet_features_df=sheet_features_df,
        raw_measurements_df=sheet_features_df,
        period_box_source="point_value",
    )

    assert expander_titles == ["🚨 单片异常预警指标图像（1 个指标）"]
    assert captured["period_box_source"] == "point_value"
    assert captured["chart_key_prefix"] == "ctq_oos_alert"
    for frame_name in ["sheet_features_df", "raw_measurements_df"]:
        filtered = captured[frame_name]
        assert filtered["value"].tolist() == [1]


def test_render_ctq_sheet_oos_alert_indicator_sections_warns_when_no_matching_data(monkeypatch) -> None:
    alerts_df = pd.DataFrame(
        [
            {
                "厂别": "ARRAY",
                "站点": "S1",
                "参数名称": "P1",
                "Sheet ID": "CTQ001",
                "超规时间": "2026-08-20 09:30:00",
                "超规类型": "USL",
            }
        ]
    )
    warnings: list[str] = []

    monkeypatch.setattr(ctq_dashboard.st, "warning", warnings.append)
    monkeypatch.setattr(
        ctq_dashboard,
        "render_ctq_indicator_sections",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not render without matched data")),
    )

    render_ctq_sheet_oos_alert_indicator_sections(
        alerts_df=alerts_df,
        sheet_features_df=pd.DataFrame([{"factory": "OLED", "step_id": "S9", "param_name": "P9"}]),
        raw_measurements_df=pd.DataFrame(),
    )

    assert warnings == ["预警指标暂无可绘制的 Sheet 数据。"]
