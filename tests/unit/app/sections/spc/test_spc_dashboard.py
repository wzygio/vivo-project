from contextlib import nullcontext
from datetime import date
from pathlib import Path

import pandas as pd

from app.sections.inline_domain.spc import spc_dashboard
from app.sections.inline_domain.spc.spc_dashboard import (
    _create_period_capability_table,
    _create_period_overview_chart,
    _create_sheet_points_box_chart,
    _create_sheet_points_box_charts,
    build_spc_sheet_oos_alerts,
    build_weekly_cpk_alerts,
    build_weekly_cpm_alerts,
    filter_spc_report_by_alerts,
    filter_spc_report,
    get_available_factories,
    get_default_spc_start_date,
    get_params_for_factory_steps,
    get_steps_for_factory,
    render_cpk_alert_section,
    render_cpm_alert_section,
    render_sheet_oos_alert_indicator_sections,
    render_spc_decoration_admin,
    render_spc_indicator_sections,
)
from src.inline_domain.core.spc.cpk_decoration import CpkDecorationResult
from src.inline_domain.core.shared.sheet_oos_decoration import SheetOosDecorationResult


def _sample_report_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "15260", "param_name": "4PP_Rs", "period_type": "month"},
            {"factory": "ARRAY", "step_id": "15260", "param_name": "4PP_UNI", "period_type": "month"},
            {"factory": "ARRAY", "step_id": "17450", "param_name": "CD1", "period_type": "month"},
            {"factory": "OLED", "step_id": "21200", "param_name": "PPA_B_X", "period_type": "month"},
            {"factory": "TP", "step_id": "41140", "param_name": "SE_L1T", "period_type": "month"},
        ]
    )


def test_build_weekly_cpk_alerts_returns_only_values_from_previous_week() -> None:
    period_capability_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "MONTH_TYPE",
                "period_type": "month",
                "period_label": "2026-07",
                "cpk": 0.80,
            },
            {
                "prod_code": "M673",
                "factory": "TP",
                "step_id": "41260",
                "param_name": "DAY_TYPE",
                "period_type": "day",
                "period_label": "2026-07-22",
                "cpk": 1.10,
            },
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpk": 0.90,
            },
            {
                "factory": "TP",
                "step_id": "41140",
                "param_name": "SE_L1T",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpk": 1.20,
            },
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "AT_THRESHOLD",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpk": 1.33,
            },
            {
                "factory": "TP",
                "step_id": "41140",
                "param_name": "NOT_A_NUMBER",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpk": "not-a-number",
            },
            {
                "factory": "OLED",
                "step_id": "21200",
                "param_name": "BEFORE_PREVIOUS_WEEK",
                "period_type": "week",
                "period_label": "2026-W29",
                "cpk": 0.90,
            },
            {
                "factory": "OLED",
                "step_id": "21200",
                "param_name": "CURRENT_WEEK",
                "period_type": "week",
                "period_label": "2026-W31",
                "cpk": 0.80,
            },
        ]
    )

    alerts_df = build_weekly_cpk_alerts(
        period_capability_df,
        reference_date=date(2026, 7, 28),
    )

    assert alerts_df.to_dict("records") == [
        {
            "厂别": "ARRAY",
            "站点": "15260",
            "参数名称": "4PP_Rs",
            "超规周次": "2026-W30",
            "CPK值": 0.90,
        },
        {
            "厂别": "TP",
            "站点": "41140",
            "参数名称": "SE_L1T",
            "超规周次": "2026-W30",
            "CPK值": 1.20,
        },
    ]


def test_build_weekly_cpk_alerts_excludes_decorated_records() -> None:
    period_capability_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "13450",
                "param_name": "OVL1_X",
                "period_type": "week",
                "period_label": "2026-W31",
                "cpk": 1.051,
                "cpk_decorated": True,
            },
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "period_type": "week",
                "period_label": "2026-W31",
                "cpk": 1.20,
                "cpk_decorated": False,
            },
        ]
    )

    alerts_df = build_weekly_cpk_alerts(
        period_capability_df,
        reference_date=date(2026, 8, 4),
    )

    assert alerts_df["参数名称"].tolist() == ["4PP_Rs"]


def test_build_weekly_cpm_alerts_returns_only_values_from_previous_week() -> None:
    period_capability_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "MONTH_TYPE",
                "period_type": "month",
                "period_label": "2026-07",
                "cpm": 0.80,
            },
            {
                "factory": "TP",
                "step_id": "41260",
                "param_name": "DAY_TYPE",
                "period_type": "day",
                "period_label": "2026-07-22",
                "cpm": 1.10,
            },
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpm": 0.90,
            },
            {
                "factory": "TP",
                "step_id": "41140",
                "param_name": "SE_L1T",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpm": 1.20,
            },
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "AT_THRESHOLD",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpm": 1.33,
            },
            {
                "factory": "OLED",
                "step_id": "21200",
                "param_name": "CURRENT_WEEK",
                "period_type": "week",
                "period_label": "2026-W31",
                "cpm": 0.80,
            },
        ]
    )

    alerts_df = build_weekly_cpm_alerts(
        period_capability_df,
        reference_date=date(2026, 7, 28),
    )

    assert alerts_df.to_dict("records") == [
        {
            "厂别": "ARRAY",
            "站点": "15260",
            "参数名称": "4PP_Rs",
            "超规周次": "2026-W30",
            "CPM值": 0.90,
        },
        {
            "厂别": "TP",
            "站点": "41140",
            "参数名称": "SE_L1T",
            "超规周次": "2026-W30",
            "CPM值": 1.20,
        },
    ]


def test_build_weekly_cpm_alerts_excludes_decorated_rows() -> None:
    period_capability_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpm": 0.90,
                "cpm_decorated": True,
            },
            {
                "factory": "TP",
                "step_id": "41140",
                "param_name": "SE_L1T",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpm": 1.20,
                "cpm_decorated": False,
            },
        ]
    )

    alerts_df = build_weekly_cpm_alerts(
        period_capability_df,
        reference_date=date(2026, 7, 28),
    )

    assert alerts_df["参数名称"].tolist() == ["SE_L1T"]


def test_build_weekly_cpm_alerts_returns_empty_when_cpm_column_missing() -> None:
    period_capability_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpk": 0.90,
            }
        ]
    )

    alerts_df = build_weekly_cpm_alerts(
        period_capability_df,
        reference_date=date(2026, 7, 28),
    )

    assert alerts_df.empty
    assert alerts_df.columns.tolist() == ["厂别", "站点", "参数名称", "超规周次", "CPM值"]


def test_filter_spc_report_by_alerts_matches_exact_indicator_combinations() -> None:
    report_df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "S1", "param_name": "P1", "value": 1},
            {"factory": "ARRAY", "step_id": "S1", "param_name": "P2", "value": 2},
            {"factory": "ARRAY", "step_id": "S2", "param_name": "P2", "value": 3},
            {"factory": "OLED", "step_id": "S1", "param_name": "P1", "value": 4},
        ]
    )
    alerts_df = pd.DataFrame(
        [
            {"厂别": "ARRAY", "站点": "S1", "参数名称": "P1", "超规日期": "2026-07-20"},
            {"厂别": "ARRAY", "站点": "S1", "参数名称": "P1", "超规日期": "2026-07-21"},
            {"厂别": "ARRAY", "站点": "S2", "参数名称": "P2", "超规日期": "2026-07-21"},
        ]
    )

    result = filter_spc_report_by_alerts(report_df, alerts_df)

    assert result["value"].tolist() == [1, 3]


def test_render_cpk_alert_section_renders_only_alerted_indicators(monkeypatch) -> None:
    alerts_df = pd.DataFrame(
        [{"厂别": "ARRAY", "站点": "S1", "参数名称": "P1", "超规日期": "2026-07-21"}]
    )
    report_df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "S1", "param_name": "P1", "value": 1},
            {"factory": "ARRAY", "step_id": "S1", "param_name": "P2", "value": 2},
        ]
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(spc_dashboard.st, "expander", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(spc_dashboard.st, "error", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "dataframe", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "markdown", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_indicator_sections",
        lambda **kwargs: captured.update(kwargs),
    )

    render_cpk_alert_section(
        alerts_df=alerts_df,
        has_capability_data=True,
        period_capability_df=report_df,
        sheet_features_df=report_df,
        raw_measurements_df=report_df,
        period_box_source="point_value",
    )

    assert captured["period_box_source"] == "point_value"
    for frame_name in ["period_capability_df", "sheet_features_df", "raw_measurements_df"]:
        assert captured[frame_name]["param_name"].tolist() == ["P1"]


def test_render_cpk_alert_section_expands_and_displays_alert_details(monkeypatch) -> None:
    expander_calls: list[tuple[str, bool]] = []
    error_messages: list[str] = []
    rendered_tables: list[pd.DataFrame] = []
    alerts_df = pd.DataFrame(
        [
            {
                "厂别": "OLED",
                "站点": "21200",
                "参数名称": "PPA_B_X",
                "超规周次": "2026-W29",
                "CPK值": 1.10,
            }
        ]
    )

    def fake_expander(label: str, expanded: bool = False):
        expander_calls.append((label, expanded))
        return nullcontext()

    monkeypatch.setattr(spc_dashboard.st, "expander", fake_expander)
    monkeypatch.setattr(spc_dashboard.st, "error", error_messages.append)
    monkeypatch.setattr(spc_dashboard.st, "warning", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        spc_dashboard.st,
        "dataframe",
        lambda frame, **_kwargs: rendered_tables.append(frame),
    )

    render_cpk_alert_section(
        alerts_df,
        has_capability_data=True,
        period_capability_df=pd.DataFrame(),
        sheet_features_df=pd.DataFrame(),
        raw_measurements_df=pd.DataFrame(),
    )

    assert expander_calls == [("CPK预警中心（CPK < 1.33）", True)]
    assert error_messages == ["检测到 1 条 CPK 预警，请关注。"]
    assert rendered_tables[0].equals(alerts_df)


def test_render_cpk_alert_section_distinguishes_missing_capability_data(monkeypatch) -> None:
    expander_calls: list[tuple[str, bool]] = []
    info_messages: list[str] = []
    success_messages: list[str] = []

    def fake_expander(label: str, expanded: bool = False):
        expander_calls.append((label, expanded))
        return nullcontext()

    monkeypatch.setattr(spc_dashboard.st, "expander", fake_expander)
    monkeypatch.setattr(spc_dashboard.st, "info", info_messages.append)
    monkeypatch.setattr(spc_dashboard.st, "success", success_messages.append)

    render_cpk_alert_section(
        pd.DataFrame(),
        has_capability_data=False,
        period_capability_df=pd.DataFrame(),
        sheet_features_df=pd.DataFrame(),
        raw_measurements_df=pd.DataFrame(),
    )

    assert expander_calls == [("CPK预警中心（CPK < 1.33）", True)]
    assert info_messages == ["当前产品暂无可计算的 CPK 数据。"]
    assert success_messages == []


def test_render_cpk_alert_section_shows_all_clear_when_weekly_cpk_is_normal(monkeypatch) -> None:
    expander_calls: list[tuple[str, bool]] = []
    info_messages: list[str] = []
    success_messages: list[str] = []

    def fake_expander(label: str, expanded: bool = False):
        expander_calls.append((label, expanded))
        return nullcontext()

    monkeypatch.setattr(spc_dashboard.st, "expander", fake_expander)
    monkeypatch.setattr(spc_dashboard.st, "info", info_messages.append)
    monkeypatch.setattr(spc_dashboard.st, "success", success_messages.append)

    render_cpk_alert_section(
        pd.DataFrame(),
        has_capability_data=True,
        period_capability_df=pd.DataFrame(),
        sheet_features_df=pd.DataFrame(),
        raw_measurements_df=pd.DataFrame(),
    )

    assert expander_calls == [("CPK预警中心（CPK < 1.33）", True)]
    assert success_messages == ["未发现低于 1.33 的 CPK。"]
    assert info_messages == []


def test_render_cpm_alert_section_renders_cpm_alert_charts(monkeypatch) -> None:
    """CPM 预警同样自动渲染对应超规指标的图像。"""
    expander_calls: list[tuple[str, bool]] = []
    markdown_messages: list[str] = []
    captured: dict[str, object] = {}
    alerts_df = pd.DataFrame(
        [{"厂别": "ARRAY", "站点": "S1", "参数名称": "P1", "超规周次": "2026-W29", "CPM值": 1.10}]
    )
    report_df = pd.DataFrame(
        [{"factory": "ARRAY", "step_id": "S1", "param_name": "P1", "value": 1}]
    )

    def fake_expander(label: str, expanded: bool = False):
        expander_calls.append((label, expanded))
        return nullcontext()

    monkeypatch.setattr(spc_dashboard.st, "expander", fake_expander)
    monkeypatch.setattr(spc_dashboard.st, "error", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "dataframe", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "markdown", markdown_messages.append)
    monkeypatch.setattr(spc_dashboard.st, "caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_indicator_sections",
        lambda **kwargs: captured.update(kwargs),
    )

    render_cpm_alert_section(
        alerts_df,
        has_capability_data=True,
        period_capability_df=report_df,
        sheet_features_df=report_df,
        raw_measurements_df=report_df,
    )

    assert expander_calls == [("CPM预警中心（CPM < 1.33）", True)]
    assert markdown_messages == ["**🚨 CPM自动预警指标图像（1 个指标）**"]
    assert captured["memo_state_key"] == "spc_cpm_alert_charts_memo"
    assert captured["chart_key_prefix"] == "spc_cpm_alert"
    assert captured["sheet_features_df"]["param_name"].tolist() == ["P1"]


def test_admin_decoration_panel_places_oos_and_cpk_controls_in_separate_tabs(monkeypatch, tmp_path: Path) -> None:
    tab_labels: list[str] = []

    class FakeColumn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    oos_result = SheetOosDecorationResult(
        raw_measurements_df=pd.DataFrame(),
        decoration_df=pd.DataFrame(),
        decoration_path=tmp_path / "spc_sheet_oos_decoration.xlsx",
        decoration_sheet="M678",
    )
    cpk_result = CpkDecorationResult(
        period_capability_df=pd.DataFrame(),
        decoration_df=pd.DataFrame(),
        decoration_path=tmp_path / "spc_cpk_cpm_decoration.xlsx",
        decoration_sheet="M678",
    )
    cpm_result = CpkDecorationResult(
        period_capability_df=pd.DataFrame(),
        decoration_df=pd.DataFrame(),
        decoration_path=tmp_path / "spc_cpk_cpm_decoration.xlsx",
        decoration_sheet="M678_cpm",
    )

    monkeypatch.setattr(spc_dashboard.st, "expander", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(
        spc_dashboard.st,
        "tabs",
        lambda labels: tab_labels.extend(labels) or [nullcontext() for _ in labels],
    )
    monkeypatch.setattr(spc_dashboard.st, "columns", lambda *_args, **_kwargs: [FakeColumn()] * 2)
    monkeypatch.setattr(spc_dashboard.st, "caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "markdown", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "download_button", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "file_uploader", lambda *_args, **_kwargs: None)

    render_spc_decoration_admin(oos_result, cpk_result, cpm_result)

    assert tab_labels == ["超规片修饰", "CPK修饰", "CPM修饰"]


def test_default_cpm_start_date_uses_previous_month_first_day() -> None:
    assert get_default_spc_start_date(date(2026, 6, 30)) == date(2026, 5, 1)
    assert get_default_spc_start_date(date(2026, 1, 10)) == date(2025, 12, 1)


def test_filter_options_follow_factory_step_param_cascade() -> None:
    report_df = _sample_report_df()

    assert get_available_factories(report_df) == ["ARRAY", "OLED", "TP"]
    assert get_steps_for_factory(report_df, "ARRAY") == ["15260", "17450"]
    assert get_steps_for_factory(report_df, "OLED") == ["21200"]
    assert get_params_for_factory_steps(report_df, "ARRAY", ["15260"]) == ["4PP_Rs", "4PP_UNI"]


def test_filter_spc_report_uses_single_factory_and_selected_steps_params() -> None:
    filtered = filter_spc_report(
        report_df=_sample_report_df(),
        selected_factory="ARRAY",
        selected_params=["4PP_Rs", "4PP_UNI"],
        selected_steps=["15260"],
    )

    assert filtered["factory"].tolist() == ["ARRAY", "ARRAY"]
    assert filtered["step_id"].tolist() == ["15260", "15260"]
    assert filtered["param_name"].tolist() == ["4PP_Rs", "4PP_UNI"]


def _sample_sheet_features() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "sheet_id": "S1",
                "sheet_start_time": "2026-06-24",
                "sheet_mean": 9.0,
                "usl": 12.0,
                "lsl": 8.0,
                "ucl": 11.0,
                "lcl": 9.0,
            },
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "sheet_id": "S2",
                "sheet_start_time": "2026-06-25",
                "sheet_mean": 10.0,
                "usl": 12.0,
                "lsl": 8.0,
                "ucl": 11.0,
                "lcl": 9.0,
            },
        ]
    )


def _sample_sparse_sheet_features() -> pd.DataFrame:
    rows = []
    for idx, sheet_date in enumerate(
        [
            "2026-06-10",
            "2026-06-15",
            "2026-06-16",
            "2026-06-17",
            "2026-06-29",
            "2026-06-30",
            "2026-07-01",
            "2026-07-02",
        ],
        start=1,
    ):
        rows.append(
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "sheet_id": f"S{idx}",
                "sheet_start_time": sheet_date,
                "sheet_mean": float(idx),
                "usl": 12.0,
                "lsl": 0.0,
                "ucl": 10.0,
                "lcl": 2.0,
            }
        )
    return pd.DataFrame(rows)


def _sample_metric_backfill_sheet_features() -> pd.DataFrame:
    rows = []
    for idx, sheet_date in enumerate(
        [
            "2026-05-18",
            "2026-05-25",
            "2026-06-01",
            "2026-06-02",
            "2026-06-03",
            "2026-06-04",
            "2026-06-05",
            "2026-06-06",
            "2026-06-07",
            "2026-06-08",
            "2026-07-02",
        ],
        start=1,
    ):
        rows.append(
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "sheet_id": f"S{idx}",
                "sheet_start_time": sheet_date,
                "sheet_mean": float(idx),
                "usl": 12.0,
                "lsl": 0.0,
                "ucl": 10.0,
                "lcl": 2.0,
            }
        )
    return pd.DataFrame(rows)


def _sample_period_capability() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "period_type": "month",
                "period_label": "2026-06",
                "period_sort": 102,
                "cpm": 1.2,
                "cpk": 1.1,
                "sample_count": 2,
            },
            {
                "period_type": "day",
                "period_label": "2026-06-25",
                "period_sort": 307,
                "cpm": 1.5,
                "cpk": 1.4,
                "sample_count": 2,
            },
        ]
    )


def _sample_full_period_capability() -> pd.DataFrame:
    rows = []
    for period_type, labels, base_sort in [
        ("month", ["2026-05", "2026-06"], 100),
        ("week", ["2026-W24", "2026-W25", "2026-W26"], 200),
        ("day", ["2026-06-23", "2026-06-24", "2026-06-25"], 304),
    ]:
        for idx, label in enumerate(labels, start=1):
            rows.append(
                {
                    "period_type": period_type,
                    "period_label": label,
                    "period_sort": base_sort + idx,
                    "period_end": "2026-06-25",
                    "cpm": 1.0 + idx / 10,
                    "cpk": 0.9 + idx / 10,
                    "sample_count": 2,
                    "mean_value": 10.0,
                    "std_value": 1.0,
                }
            )
    return pd.DataFrame(rows)


def test_period_overview_chart_uses_box_without_metric_lines() -> None:
    fig = _create_period_overview_chart(
        sheet_features_df=_sample_sheet_features(),
        period_capability_df=_sample_period_capability(),
        title="ARRAY | 15260 | 4PP_Rs",
    )

    box_traces = [trace for trace in fig.data if trace.type == "box"]
    line_traces = [trace for trace in fig.data if trace.type == "scatter"]
    assert box_traces
    assert all(trace.boxpoints is False for trace in box_traces)
    assert not line_traces
    assert all(getattr(shape, "y0", None) != 1.33 for shape in fig.layout.shapes)
    assert any(getattr(shape, "y0", None) == 10.0 for shape in fig.layout.shapes)
    annotation_texts = {annotation.text for annotation in fig.layout.annotations}
    assert {"USL: 12", "LSL: 8", "UCL: 11", "LCL: 9"}.issubset(annotation_texts)
    assert fig.layout.yaxis.range == (8.0, 12.0)
    assert fig.layout.height <= 480


def test_period_overview_chart_is_always_a_box_distribution() -> None:
    fig = _create_period_overview_chart(
        sheet_features_df=_sample_sheet_features(),
        period_capability_df=_sample_period_capability(),
        title="ARRAY | 15260 | 4PP_UNI",
    )

    assert [trace for trace in fig.data if trace.type == "box"]
    assert not [trace for trace in fig.data if trace.type == "scatter"]


def test_period_overview_chart_uses_all_measurement_points_in_point_value_mode() -> None:
    raw_measurements = pd.DataFrame(
        [
            {"sheet_id": "S1", "sheet_start_time": "2026-06-24", "param_value": 8.5},
            {"sheet_id": "S1", "sheet_start_time": "2026-06-24", "param_value": 9.5},
            {"sheet_id": "S2", "sheet_start_time": "2026-06-25", "param_value": 10.5},
            {"sheet_id": "S2", "sheet_start_time": "2026-06-25", "param_value": 11.5},
        ]
    )

    fig = _create_period_overview_chart(
        sheet_features_df=_sample_sheet_features(),
        period_capability_df=_sample_period_capability(),
        raw_measurements_df=raw_measurements,
        period_box_source="point_value",
        title="ARRAY | 15260 | 4PP_Rs",
    )

    month_trace = next(trace for trace in fig.data if trace.name == "月 | 2026-06")
    assert list(month_trace.y) == [8.5, 9.5, 10.5, 11.5]
    assert fig.layout.yaxis.title.text == "Point Value"


def test_render_indicator_sections_forwards_point_box_source_and_measurements(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeColumn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def metric(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(spc_dashboard.st, "expander", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(
        spc_dashboard.st,
        "columns",
        lambda spec, **_kwargs: [FakeColumn() for _ in range(spec if isinstance(spec, int) else len(spec))],
    )
    monkeypatch.setattr(spc_dashboard.st, "plotly_chart", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        spc_dashboard,
        "_create_period_overview_chart",
        lambda **kwargs: captured.update(kwargs) or object(),
    )
    monkeypatch.setattr(
        spc_dashboard,
        "_create_sheet_points_box_charts",
        lambda **_kwargs: (object(), object()),
    )
    raw_measurements = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "sheet_start_time": "2026-06-24",
                "param_value": 8.5,
            }
        ]
    )

    render_spc_indicator_sections(
        period_capability_df=pd.DataFrame(),
        sheet_features_df=_sample_sheet_features(),
        raw_measurements_df=raw_measurements,
        period_box_source="point_value",
    )

    assert captured["period_box_source"] == "point_value"
    assert captured["raw_measurements_df"].equals(raw_measurements)


def test_render_indicator_sections_uses_frontend_config_for_line_parameters(monkeypatch) -> None:
    captured: dict[str, dict[str, object]] = {}

    class FakeColumn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def metric(self, *_args, **_kwargs):
            return None

    def capture_period(**kwargs):
        captured["period"] = kwargs
        return object()

    def capture_sheet(**kwargs):
        captured["sheet"] = kwargs
        return object(), object()

    monkeypatch.setattr(spc_dashboard.st, "expander", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(
        spc_dashboard.st,
        "columns",
        lambda spec, **_kwargs: [FakeColumn() for _ in range(spec if isinstance(spec, int) else len(spec))],
    )
    monkeypatch.setattr(spc_dashboard.st, "plotly_chart", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        spc_dashboard,
        "_create_period_overview_chart",
        capture_period,
    )
    monkeypatch.setattr(
        spc_dashboard,
        "_create_sheet_points_box_charts",
        capture_sheet,
    )

    monkeypatch.setattr(
        spc_dashboard.ConfigLoader,
        "get_spc_line_chart_param_name_contains",
        staticmethod(lambda: ["UNI"]),
    )
    sheet_features = _sample_sheet_features().assign(param_name="SE_L1T_UNI")
    raw_measurements = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "SE_L1T_UNI",
                "sheet_start_time": "2026-06-24",
                "param_value": 8.5,
            }
        ]
    )
    period_capability = _sample_period_capability().assign(
        factory="ARRAY",
        step_id="15260",
        param_name="SE_L1T_UNI",
    )

    render_spc_indicator_sections(
        period_capability_df=period_capability,
        sheet_features_df=sheet_features,
        raw_measurements_df=raw_measurements,
    )

    assert "chart_type" not in captured["period"]
    assert captured["sheet"]["chart_type"] == "line"


def test_render_indicator_sections_places_sheet_distributions_on_full_width_rows(monkeypatch) -> None:
    column_specs: list[int | list[float]] = []
    rendered_figures: list[object] = []
    rendered_tables: list[pd.DataFrame] = []
    period_figure = object()
    chamber_figure = object()
    time_figure = object()

    class FakeColumn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def metric(self, *_args, **_kwargs):
            return None

    def fake_columns(spec, **_kwargs):
        column_specs.append(spec)
        return [FakeColumn() for _ in range(spec if isinstance(spec, int) else len(spec))]

    monkeypatch.setattr(spc_dashboard.st, "expander", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(spc_dashboard.st, "columns", fake_columns)
    monkeypatch.setattr(
        spc_dashboard.st,
        "dataframe",
        lambda table, **_kwargs: rendered_tables.append(table),
    )
    monkeypatch.setattr(
        spc_dashboard.st,
        "plotly_chart",
        lambda figure, **_kwargs: rendered_figures.append(figure),
    )
    monkeypatch.setattr(spc_dashboard, "_create_period_overview_chart", lambda **_kwargs: period_figure)
    monkeypatch.setattr(
        spc_dashboard,
        "_create_sheet_points_box_charts",
        lambda **_kwargs: (chamber_figure, time_figure),
    )
    raw_measurements = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "sheet_start_time": "2026-06-24",
                "param_value": 8.5,
            }
        ]
    )
    period_capability = _sample_period_capability().assign(
        factory="ARRAY",
        step_id="15260",
        param_name="4PP_Rs",
    )

    render_spc_indicator_sections(
        period_capability_df=period_capability,
        sheet_features_df=_sample_sheet_features(),
        raw_measurements_df=raw_measurements,
    )

    assert column_specs == [4, [1.15, 1]]
    assert rendered_tables[0].columns.tolist() == ["周期", "CPM", "CPK"]
    assert rendered_figures == [period_figure, chamber_figure, time_figure]


def test_indicator_payload_assigns_unique_plotly_keys_across_page_sections(monkeypatch) -> None:
    rendered_keys: list[str | None] = []
    shared_figure = object()

    class FakeColumn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def metric(self, *_args, **_kwargs):
            return None

    payload = {
        "label": "ARRAY | 15260 | 4PP_Rs",
        "cpk_median": "-",
        "cpk_min": "-",
        "cpm_median": "-",
        "cpm_min": "-",
        "capability_table": pd.DataFrame(),
        "fig1": shared_figure,
        "chamber_fig": shared_figure,
        "time_fig": shared_figure,
    }
    monkeypatch.setattr(spc_dashboard.st, "expander", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(
        spc_dashboard.st,
        "columns",
        lambda spec, **_kwargs: [FakeColumn() for _ in range(spec if isinstance(spec, int) else len(spec))],
    )
    monkeypatch.setattr(
        spc_dashboard.st,
        "plotly_chart",
        lambda _figure, **kwargs: rendered_keys.append(kwargs.get("key")),
    )

    spc_dashboard._render_indicator_payload(payload, chart_key_prefix="spc_alert")
    spc_dashboard._render_indicator_payload(payload, chart_key_prefix="spc_report")

    assert all(rendered_keys)
    assert len(rendered_keys) == len(set(rendered_keys)) == 6


def test_period_capability_table_shows_cpm_and_cpk_together() -> None:
    table = _create_period_capability_table(_sample_full_period_capability())

    assert table.columns.tolist() == ["周期", "CPM", "CPK"]
    assert table["周期"].tolist() == [
        "月 2026-05",
        "月 2026-06",
        "周 2026-W24",
        "周 2026-W25",
        "周 2026-W26",
        "日 2026-06-23",
        "日 2026-06-24",
        "日 2026-06-25",
    ]
    assert table.loc[0, "CPM"] == "1.100"
    assert table.loc[0, "CPK"] == "1.000"


def test_period_overview_chart_handles_empty_capability_with_reserved_period_axis() -> None:
    fig = _create_period_overview_chart(
        sheet_features_df=_sample_sheet_features(),
        period_capability_df=pd.DataFrame(),
        title="ARRAY | 15260 | 4PP_Rs",
    )

    labels = list(fig.layout.xaxis.categoryarray)
    assert labels == [
        "月 | 2026-06",
        "周 | 2026-W26",
        "日 | 2026-06-24",
        "日 | 2026-06-25",
    ]
    assert [trace.type for trace in fig.data].count("box") == 4
    assert all(trace.type != "scatter" for trace in fig.data)


def test_period_overview_chart_uses_recent_available_periods_without_calendar_gaps() -> None:
    fig = _create_period_overview_chart(
        sheet_features_df=_sample_sparse_sheet_features(),
        period_capability_df=pd.DataFrame(),
        title="ARRAY | 15260 | 4PP_Rs",
    )

    assert list(fig.layout.xaxis.categoryarray) == [
        "月 | 2026-06",
        "月 | 2026-07",
        "周 | 2026-W24",
        "周 | 2026-W25",
        "周 | 2026-W27",
        "日 | 2026-06-15",
        "日 | 2026-06-16",
        "日 | 2026-06-17",
        "日 | 2026-06-29",
        "日 | 2026-06-30",
        "日 | 2026-07-01",
        "日 | 2026-07-02",
    ]


def test_period_capability_table_limits_each_period_type_to_compact_window() -> None:
    period_capability_df = pd.DataFrame(
        [
            {"period_type": "month", "period_label": "2026-05", "period_sort": 101, "cpm": 1.1, "sample_count": 2},
            {"period_type": "month", "period_label": "2026-06", "period_sort": 102, "cpm": 1.2, "sample_count": 2},
            {"period_type": "month", "period_label": "2026-07", "period_sort": 103, "cpm": float("nan"), "sample_count": 1},
            {"period_type": "week", "period_label": "2026-W21", "period_sort": 201, "cpm": 2.1, "sample_count": 2},
            {"period_type": "week", "period_label": "2026-W22", "period_sort": 202, "cpm": 2.2, "sample_count": 2},
            {"period_type": "week", "period_label": "2026-W23", "period_sort": 203, "cpm": 2.3, "sample_count": 2},
            {"period_type": "week", "period_label": "2026-W27", "period_sort": 204, "cpm": float("nan"), "sample_count": 1},
            {"period_type": "day", "period_label": "2026-06-01", "period_sort": 301, "cpm": 3.1, "sample_count": 2},
            {"period_type": "day", "period_label": "2026-06-02", "period_sort": 302, "cpm": 3.2, "sample_count": 2},
            {"period_type": "day", "period_label": "2026-06-03", "period_sort": 303, "cpm": 3.3, "sample_count": 2},
            {"period_type": "day", "period_label": "2026-06-04", "period_sort": 304, "cpm": 3.4, "sample_count": 2},
            {"period_type": "day", "period_label": "2026-06-05", "period_sort": 305, "cpm": 3.5, "sample_count": 2},
            {"period_type": "day", "period_label": "2026-06-06", "period_sort": 306, "cpm": 3.6, "sample_count": 2},
            {"period_type": "day", "period_label": "2026-06-07", "period_sort": 307, "cpm": 3.7, "sample_count": 2},
            {"period_type": "day", "period_label": "2026-07-02", "period_sort": 308, "cpm": float("nan"), "sample_count": 1},
        ]
    )

    table = _create_period_capability_table(period_capability_df)

    assert table.columns.tolist() == ["周期", "CPM", "CPK"]
    assert table["周期"].tolist() == [
        "月 2026-06",
        "月 2026-07",
        "周 2026-W22",
        "周 2026-W23",
        "周 2026-W27",
        "日 2026-06-02",
        "日 2026-06-03",
        "日 2026-06-04",
        "日 2026-06-05",
        "日 2026-06-06",
        "日 2026-06-07",
        "日 2026-07-02",
    ]
    assert table.loc[table["周期"] == "月 2026-07", "CPM"].item() == "-"


def test_period_overview_chart_expands_measurement_axis_when_sheet_mean_exceeds_specs() -> None:
    sheet_features_df = _sample_sheet_features().copy()
    sheet_features_df.loc[0, "sheet_mean"] = 15.0

    fig = _create_period_overview_chart(
        sheet_features_df=sheet_features_df,
        period_capability_df=_sample_period_capability(),
        title="ARRAY | 15260 | 4PP_Rs",
    )

    assert fig.layout.yaxis.range[0] < 8.0
    assert fig.layout.yaxis.range[1] > 15.0


def test_period_overview_chart_draws_upper_lines_when_lower_specs_are_null() -> None:
    sheet_features_df = _sample_sheet_features().assign(
        usl=9.9,
        lsl=None,
        ucl=7.5,
        lcl=None,
        target=None,
    )

    figure = _create_period_overview_chart(
        sheet_features_df=sheet_features_df,
        period_capability_df=pd.DataFrame(),
        title="OLED | 21200 | MT_CH_PRESS_MANTISSA",
    )

    annotation_texts = {annotation.text for annotation in figure.layout.annotations}

    assert annotation_texts == {"USL: 9.9", "UCL: 7.5"}
    assert figure.layout.yaxis.range[1] > 9.9


def test_sheet_points_box_chart_marks_unknown_when_main_process_trace_is_missing() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {
                "sheet_id": "LOT00000102",
                "sheet_start_time": "2026-06-02 09:00:00",
                "site_name": "P2",
                "param_value": 11.0,
            },
            {
                "sheet_id": "LOT00000101",
                "sheet_start_time": "2026-06-02 08:00:00",
                "site_name": "P1",
                "param_value": 10.0,
            },
        ]
    )

    fig = _create_sheet_points_box_chart(raw_measurements_df, sort_mode="按腔室排序", title="Sheet点位分布")

    assert all(trace.type == "box" for trace in fig.data)
    assert all(trace.boxpoints is False for trace in fig.data)
    assert [trace.name for trace in fig.data] == ["UNKNOWN", "UNKNOWN"]


def test_sheet_points_box_chart_does_not_use_measurement_unit_as_main_process_trace() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {
                "sheet_id": "S1",
                "sheet_start_time": "2026-06-02 08:00:00",
                "unit_id": "3CEE02-PPA",
                "site_name": "P1",
                "param_value": 10.0,
            },
            {
                "sheet_id": "S2",
                "sheet_start_time": "2026-06-02 09:00:00",
                "unit_id": "3CEE02-CVD",
                "site_name": "P2",
                "param_value": 11.0,
            },
            {
                "sheet_id": "S3",
                "sheet_start_time": "2026-06-02 10:00:00",
                "unit_id": "3CEE03",
                "site_name": "P1",
                "param_value": 12.0,
            },
        ]
    )

    fig = _create_sheet_points_box_chart(raw_measurements_df, sort_mode="按腔室排序", title="Sheet点位分布")

    box_traces = [trace for trace in fig.data if trace.type == "box"]
    assert [list(trace.x)[0] for trace in box_traces] == ["S1", "S2", "S3"]
    assert [trace.name for trace in box_traces] == ["UNKNOWN", "UNKNOWN", "UNKNOWN"]
    assert [list(trace.y) for trace in box_traces] == [[10.0], [11.0], [12.0]]
    assert box_traces[0].marker.color == box_traces[1].marker.color
    assert box_traces[1].marker.color == box_traces[2].marker.color


def test_sheet_points_box_chart_uses_full_main_process_unit_instead_of_measurement_unit() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {
                "sheet_id": "S1",
                "sheet_start_time": "2026-06-02 08:00:00",
                "unit_id": "MEASURE-EQP-02",
                "main_process_unit_id": "MAIN-CVD-CH02",
                "param_value": 10.0,
            },
            {
                "sheet_id": "S2",
                "sheet_start_time": "2026-06-02 09:00:00",
                "unit_id": "MEASURE-EQP-01",
                "main_process_unit_id": "MAIN-CVD-CH01",
                "param_value": 11.0,
            },
        ]
    )

    fig = _create_sheet_points_box_chart(
        raw_measurements_df,
        sort_mode="按腔室排序",
        title="Sheet点位分布 By主站点设备/腔室",
    )

    box_traces = [trace for trace in fig.data if trace.type == "box"]
    assert [list(trace.x)[0] for trace in box_traces] == ["S2", "S1"]
    assert [trace.name for trace in box_traces] == ["MAIN-CVD-CH01", "MAIN-CVD-CH02"]


def test_sheet_points_box_chart_sorts_sheet_boxes_by_chamber_then_time() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {"sheet_id": "S3", "sheet_start_time": "2026-06-02 10:00:00", "unit_id": "3CEE02-PPA", "param_value": 12.0},
            {"sheet_id": "S1", "sheet_start_time": "2026-06-02 08:00:00", "unit_id": "3CEE01-PPA", "param_value": 10.0},
            {"sheet_id": "S2", "sheet_start_time": "2026-06-02 09:00:00", "unit_id": "3CEE02-CVD", "param_value": 11.0},
        ]
    )

    fig = _create_sheet_points_box_chart(raw_measurements_df, sort_mode="按腔室排序", title="Sheet点位分布")

    assert [list(trace.x)[0] for trace in fig.data if trace.type == "box"] == ["S1", "S2", "S3"]


def test_sheet_points_box_chart_draws_spec_lines_and_single_color_for_time_sort() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {"sheet_id": "S2", "sheet_start_time": "2026-06-02 09:00:00", "param_value": 11.0},
            {"sheet_id": "S1", "sheet_start_time": "2026-06-01 08:00:00", "param_value": 10.0},
        ]
    )
    spec_df = pd.DataFrame(
        [
            {
                "usl": 12.0,
                "lsl": 8.0,
                "ucl": 11.0,
                "lcl": 9.0,
                "target": 10.0,
            }
        ]
    )

    fig = _create_sheet_points_box_chart(
        raw_measurements_df,
        sort_mode="按过货时间排序",
        title="Sheet点位分布",
        spec_df=spec_df,
    )

    assert [trace.name for trace in fig.data if trace.type == "box"] == ["S1", "S2"]
    assert {trace.marker.color for trace in fig.data if trace.type == "box"} == {"#1d4ed8"}
    assert any(getattr(shape, "y0", None) == 12.0 for shape in fig.layout.shapes)
    assert any(getattr(shape, "y0", None) == 10.0 for shape in fig.layout.shapes)
    annotation_texts = {annotation.text for annotation in fig.layout.annotations}
    assert {"USL: 12", "LSL: 8", "UCL: 11", "LCL: 9"}.issubset(annotation_texts)


def test_sheet_points_box_chart_draws_only_upper_lines_when_lsl_is_zero() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {"sheet_id": "S1", "sheet_start_time": "2026-06-01 08:00:00", "param_value": 3.0},
            {"sheet_id": "S2", "sheet_start_time": "2026-06-02 09:00:00", "param_value": 4.0},
        ]
    )
    spec_df = pd.DataFrame(
        [
            {
                "usl": 8.0,
                "lsl": 0.0,
                "ucl": 6.0,
                "lcl": 0.0,
                "target": 4.0,
            }
        ]
    )

    fig = _create_sheet_points_box_chart(
        raw_measurements_df,
        sort_mode="\u6309\u8fc7\u8d27\u65f6\u95f4\u6392\u5e8f",
        title="Sheet\u70b9\u4f4d\u5206\u5e03",
        spec_df=spec_df,
    )

    annotation_texts = {annotation.text for annotation in fig.layout.annotations}

    assert annotation_texts == {"USL: 8", "UCL: 6"}


def test_sheet_points_box_chart_draws_upper_lines_when_lower_specs_are_null() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {"sheet_id": "S1", "sheet_start_time": "2026-06-01", "param_value": 3.0},
            {"sheet_id": "S2", "sheet_start_time": "2026-06-02", "param_value": 6.0},
        ]
    )
    spec_df = pd.DataFrame(
        [
            {
                "usl": 9.9,
                "lsl": None,
                "ucl": 7.5,
                "lcl": None,
                "target": None,
            }
        ]
    )

    figure = _create_sheet_points_box_chart(
        raw_measurements_df,
        sort_mode="按过货时间排序",
        title="Sheet点位分布",
        spec_df=spec_df,
    )

    annotation_texts = {annotation.text for annotation in figure.layout.annotations}

    assert annotation_texts == {"USL: 9.9", "UCL: 7.5"}
    assert figure.layout.yaxis.range[1] > 9.9


def test_sheet_points_box_chart_preserves_tiny_upper_spec_values_in_labels() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {"sheet_id": "S1", "sheet_start_time": "2026-06-01", "param_value": 3.0e-12},
            {"sheet_id": "S2", "sheet_start_time": "2026-06-02", "param_value": 3.2e-12},
        ]
    )
    spec_df = pd.DataFrame(
        [
            {
                "usl": 1.6e-11,
                "lsl": 0.0,
                "ucl": 9.7e-12,
                "lcl": 0.0,
                "target": 0.0,
            }
        ]
    )

    figure = _create_sheet_points_box_chart(
        raw_measurements_df,
        sort_mode="按过货时间排序",
        title="Sheet点位分布",
        spec_df=spec_df,
    )

    annotation_texts = {annotation.text for annotation in figure.layout.annotations}
    assert annotation_texts == {"USL: 1.6e-11", "UCL: 9.7e-12"}


def test_sheet_points_box_chart_uses_point_lines_when_frontend_style_is_line() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {
                "sheet_id": "S1",
                "sheet_start_time": "2026-06-01 08:00:00",
                "site_name": "P1",
                "param_value": 3.0,
            },
            {
                "sheet_id": "S1",
                "sheet_start_time": "2026-06-01 08:00:00",
                "site_name": "P2",
                "param_value": 5.0,
            },
            {
                "sheet_id": "S2",
                "sheet_start_time": "2026-06-02 09:00:00",
                "site_name": "P1",
                "param_value": 4.0,
            },
            {
                "sheet_id": "S2",
                "sheet_start_time": "2026-06-02 09:00:00",
                "site_name": "P2",
                "param_value": 6.0,
            },
        ]
    )

    fig = _create_sheet_points_box_chart(
        raw_measurements_df,
        sort_mode="\u6309\u8fc7\u8d27\u65f6\u95f4\u6392\u5e8f",
        title="Sheet\u70b9\u4f4d\u5206\u5e03",
        chart_type="line",
    )

    assert not [trace for trace in fig.data if trace.type == "box"]
    line_trace = next(trace for trace in fig.data if trace.type == "scatter")
    assert line_trace.mode == "lines+markers"
    assert list(line_trace.x) == [
        "S1<br>06-01 08时",
        "S1<br>06-01 08时",
        "S2<br>06-02 09时",
        "S2<br>06-02 09时",
    ]
    assert list(line_trace.y) == [3.0, 5.0, 4.0, 6.0]
    assert [tuple(row) for row in line_trace.customdata] == [
        ("S1", "2026-06-01 08:00:00"),
        ("S1", "2026-06-01 08:00:00"),
        ("S2", "2026-06-02 09:00:00"),
        ("S2", "2026-06-02 09:00:00"),
    ]
    assert line_trace.name == "Point Value"
    assert fig.layout.xaxis.type != "date"
    assert list(fig.layout.xaxis.categoryarray) == ["S1<br>06-01 08时", "S2<br>06-02 09时"]
    assert fig.layout.xaxis.title.text == "Sheet ID / 过货时间（小时）"


def test_sheet_points_box_chart_expands_axis_when_param_values_exceed_specs() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {"sheet_id": "S2", "sheet_start_time": "2026-06-02 09:00:00", "param_value": 14.0},
            {"sheet_id": "S1", "sheet_start_time": "2026-06-01 08:00:00", "param_value": 7.0},
        ]
    )
    spec_df = pd.DataFrame([{"usl": 12.0, "lsl": 8.0, "ucl": 11.0, "lcl": 9.0, "target": 10.0}])

    fig = _create_sheet_points_box_chart(
        raw_measurements_df,
        sort_mode="按过货时间排序",
        title="Sheet点位分布",
        spec_df=spec_df,
    )

    assert fig.layout.yaxis.range[0] < 7.0
    assert fig.layout.yaxis.range[1] > 14.0


def test_sheet_points_box_charts_returns_chamber_and_time_views() -> None:
    raw_measurements_df = pd.DataFrame(
        [
            {
                "sheet_id": "S2",
                "sheet_start_time": "2026-06-02 09:00:00",
                "unit_id": "3CEE02-PPA",
                "main_process_unit_id": "MAIN-CH02",
                "param_value": 11.0,
            },
            {
                "sheet_id": "S1",
                "sheet_start_time": "2026-06-01 08:00:00",
                "unit_id": "3CEE01-PPA",
                "main_process_unit_id": "MAIN-CH01",
                "param_value": 10.0,
            },
        ]
    )

    chamber_fig, time_fig = _create_sheet_points_box_charts(
        raw_measurements_df=raw_measurements_df,
        title_prefix="ARRAY | 10140 | PPA_B_X",
        spec_df=None,
    )

    assert chamber_fig.layout.title.text.endswith("By主站点设备/腔室")
    assert time_fig.layout.title.text.endswith("By过货时间")
    assert [list(trace.x)[0] for trace in chamber_fig.data if trace.type == "box"] == ["S1", "S2"]
    assert [trace.name for trace in chamber_fig.data if trace.type == "box"] == ["MAIN-CH01", "MAIN-CH02"]
    assert [trace.name for trace in time_fig.data if trace.type == "box"] == ["S1", "S2"]


def test_render_cpk_alert_section_reuses_memoized_charts(monkeypatch) -> None:
    """同一版预警数据重复渲染时不应重建图表（memo 命中）。"""
    build_calls: list[str] = []

    class FakeColumn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def metric(self, *_args, **_kwargs):
            return None

    def fake_build(**kwargs):
        build_calls.append(kwargs["label"])
        return {
            "label": kwargs["label"],
            "cpk_median": "-",
            "cpk_min": "-",
            "cpm_median": "-",
            "cpm_min": "-",
            "capability_table": pd.DataFrame(),
            "fig1": object(),
            "chamber_fig": object(),
            "time_fig": object(),
        }

    monkeypatch.setattr(spc_dashboard.st, "session_state", {})
    monkeypatch.setattr(spc_dashboard.st, "expander", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(spc_dashboard.st, "error", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "dataframe", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "markdown", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        spc_dashboard.st,
        "columns",
        lambda spec, **_kwargs: [FakeColumn() for _ in range(spec if isinstance(spec, int) else len(spec))],
    )
    monkeypatch.setattr(spc_dashboard.st, "plotly_chart", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spc_dashboard, "_build_indicator_render_payload", fake_build)

    alerts_df = pd.DataFrame([{"厂别": "ARRAY", "站点": "S1", "参数名称": "P1", "超规周次": "2026-W29"}])
    report_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "S1",
                "param_name": "P1",
                "prod_code": "M626",
                "sheet_start_time": "2026-07-21",
                "param_value": 1.0,
            }
        ]
    )

    kwargs = dict(
        alerts_df=alerts_df,
        has_capability_data=True,
        period_capability_df=report_df,
        sheet_features_df=report_df,
        raw_measurements_df=report_df,
    )
    render_cpk_alert_section(**kwargs)
    assert len(build_calls) == 1  # 首次：构建一次

    render_cpk_alert_section(**kwargs)
    assert len(build_calls) == 1  # 第二次：memo 命中，不再构建


def _sample_sheet_oos_decoration_result(
    tmp_path: Path,
    decoration_df: pd.DataFrame,
) -> SheetOosDecorationResult:
    return SheetOosDecorationResult(
        raw_measurements_df=pd.DataFrame(),
        decoration_df=decoration_df,
        decoration_path=tmp_path / "spc_sheet_oos_decoration.xlsx",
        decoration_sheet="M678",
    )


def _sheet_oos_decoration_row(
    sheet_id: str,
    sheet_start_time: str,
    flag: object,
    *,
    param_name: str = "4PP_Rs",
) -> dict[str, object]:
    return {
        "factory": "ARRAY",
        "prod_code": "M678",
        "step_id": "15260",
        "param_name": param_name,
        "sheet_id": sheet_id,
        "sheet_start_time": sheet_start_time,
        "oos_type": "USL",
        "flag": flag,
    }


def test_build_spc_sheet_oos_alerts_keeps_false_flag_records_from_previous_week(tmp_path: Path) -> None:
    decoration_df = pd.DataFrame(
        [
            _sheet_oos_decoration_row("S2", "2026-08-19 08:00:00", False),
            _sheet_oos_decoration_row("S1", "2026-08-18 10:00:00", False),
        ]
    )
    result = _sample_sheet_oos_decoration_result(tmp_path, decoration_df)

    alerts_df = build_spc_sheet_oos_alerts(result, reference_date=date(2026, 8, 25))

    assert alerts_df.columns.tolist() == ["厂别", "站点", "参数名称", "Sheet ID", "超规时间", "超规类型"]
    # 按超规时间倒序
    assert alerts_df["Sheet ID"].tolist() == ["S2", "S1"]
    assert alerts_df["超规时间"].map(type).eq(str).all()
    assert alerts_df["厂别"].tolist() == ["ARRAY", "ARRAY"]
    assert alerts_df["站点"].tolist() == ["15260", "15260"]
    assert alerts_df["超规类型"].tolist() == ["USL", "USL"]


def test_build_spc_sheet_oos_alerts_excludes_true_and_delete_flags(tmp_path: Path) -> None:
    decoration_df = pd.DataFrame(
        [
            _sheet_oos_decoration_row("S1", "2026-08-18 10:00:00", True),
            _sheet_oos_decoration_row("S2", "2026-08-19 08:00:00", "Delete"),
            _sheet_oos_decoration_row("S3", "2026-08-20 09:00:00", False),
        ]
    )
    result = _sample_sheet_oos_decoration_result(tmp_path, decoration_df)

    alerts_df = build_spc_sheet_oos_alerts(result, reference_date=date(2026, 8, 25))

    assert alerts_df["Sheet ID"].tolist() == ["S3"]


def test_build_spc_sheet_oos_alerts_excludes_records_outside_previous_week(tmp_path: Path) -> None:
    decoration_df = pd.DataFrame(
        [
            _sheet_oos_decoration_row("S1", "2026-08-10 10:00:00", False),  # 上上周
            _sheet_oos_decoration_row("S2", "2026-08-24 00:00:00", False),  # 本周一（半开区间外）
            _sheet_oos_decoration_row("S3", "2026-08-25 09:00:00", False),  # 本周
            _sheet_oos_decoration_row("S4", "2026-08-17 00:00:00", False),  # 上周一（区间内）
        ]
    )
    result = _sample_sheet_oos_decoration_result(tmp_path, decoration_df)

    alerts_df = build_spc_sheet_oos_alerts(result, reference_date=date(2026, 8, 25))

    assert alerts_df["Sheet ID"].tolist() == ["S4"]


def test_build_spc_sheet_oos_alerts_returns_empty_display_when_result_is_none() -> None:
    alerts_df = build_spc_sheet_oos_alerts(None, reference_date=date(2026, 8, 25))

    assert alerts_df.empty
    assert alerts_df.columns.tolist() == ["厂别", "站点", "参数名称", "Sheet ID", "超规时间", "超规类型"]


def test_render_sheet_oos_alert_indicator_sections_skips_rendering_when_no_alerts(monkeypatch) -> None:
    render_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_indicator_sections",
        lambda **kwargs: render_calls.append(kwargs),
    )

    render_sheet_oos_alert_indicator_sections(
        alerts_df=pd.DataFrame(columns=["厂别", "站点", "参数名称", "Sheet ID", "超规时间", "超规类型"]),
        period_capability_df=pd.DataFrame(),
        sheet_features_df=pd.DataFrame(),
        raw_measurements_df=pd.DataFrame(),
        period_box_source="point_value",
    )

    assert render_calls == []


def test_render_sheet_oos_alert_indicator_sections_filters_by_alert_keys(monkeypatch) -> None:
    alerts_df = pd.DataFrame(
        [
            {
                "厂别": "ARRAY",
                "站点": "S1",
                "参数名称": "P1",
                "Sheet ID": "SHEET-1",
                "超规时间": "2026-08-18 10:00:00",
                "超规类型": "USL",
            }
        ]
    )
    report_df = pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "S1", "param_name": "P1", "value": 1},
            {"factory": "ARRAY", "step_id": "S1", "param_name": "P2", "value": 2},
        ]
    )
    captured: dict[str, object] = {}
    expander_calls: list[tuple[str, bool]] = []

    def fake_expander(label: str, expanded: bool = False):
        expander_calls.append((label, expanded))
        return nullcontext()

    monkeypatch.setattr(spc_dashboard.st, "expander", fake_expander)
    monkeypatch.setattr(spc_dashboard.st, "caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_indicator_sections",
        lambda **kwargs: captured.update(kwargs),
    )

    render_sheet_oos_alert_indicator_sections(
        alerts_df=alerts_df,
        period_capability_df=report_df,
        sheet_features_df=report_df,
        raw_measurements_df=report_df,
        period_box_source="point_value",
    )

    assert expander_calls == [("🚨 单片异常预警指标图像（1 个指标）", False)]
    assert captured["period_box_source"] == "point_value"
    assert captured["chart_key_prefix"] == "spc_oos_alert"
    assert captured["memo_signature"]
    for frame_name in ["period_capability_df", "sheet_features_df", "raw_measurements_df"]:
        assert captured[frame_name]["param_name"].tolist() == ["P1"]
