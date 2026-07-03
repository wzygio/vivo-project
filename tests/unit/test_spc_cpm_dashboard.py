from datetime import date

import pandas as pd

from app.sections.spc_cpm_dashboard import (
    _create_period_overview_chart,
    _create_sheet_points_box_chart,
    filter_cpm_report,
    get_available_factories,
    get_default_cpm_start_date,
    get_params_for_factory_steps,
    get_steps_for_factory,
)


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


def test_default_cpm_start_date_uses_previous_month_first_day() -> None:
    assert get_default_cpm_start_date(date(2026, 6, 30)) == date(2026, 5, 1)
    assert get_default_cpm_start_date(date(2026, 1, 10)) == date(2025, 12, 1)


def test_filter_options_follow_factory_step_param_cascade() -> None:
    report_df = _sample_report_df()

    assert get_available_factories(report_df) == ["ARRAY", "OLED", "TP"]
    assert get_steps_for_factory(report_df, "ARRAY") == ["15260", "17450"]
    assert get_steps_for_factory(report_df, "OLED") == ["21200"]
    assert get_params_for_factory_steps(report_df, "ARRAY", ["15260"]) == ["4PP_Rs", "4PP_UNI"]


def test_filter_cpm_report_uses_single_factory_and_selected_steps_params() -> None:
    filtered = filter_cpm_report(
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


def test_period_overview_chart_uses_box_without_points_and_cpk_spec_line() -> None:
    fig = _create_period_overview_chart(
        sheet_features_df=_sample_sheet_features(),
        period_capability_df=_sample_period_capability(),
        metric_key="cpk",
        metric_label="CPK",
        title="ARRAY | 15260 | 4PP_Rs",
    )

    box_traces = [trace for trace in fig.data if trace.type == "box"]
    line_traces = [trace for trace in fig.data if trace.type == "scatter"]
    assert box_traces
    assert all(trace.boxpoints is False for trace in box_traces)
    assert line_traces
    assert all(trace.yaxis == "y2" for trace in line_traces)
    assert fig.layout.yaxis2.overlaying == "y"
    assert any(getattr(shape, "y0", None) == 1.33 for shape in fig.layout.shapes)
    assert any(getattr(shape, "y0", None) == 10.0 for shape in fig.layout.shapes)
    assert fig.layout.yaxis.range == (8.0, 12.0)
    assert fig.layout.height <= 480


def test_period_overview_chart_splits_month_week_day_metric_lines() -> None:
    fig = _create_period_overview_chart(
        sheet_features_df=_sample_sheet_features(),
        period_capability_df=_sample_full_period_capability(),
        metric_key="cpm",
        metric_label="CPM",
        title="ARRAY | 15260 | 4PP_Rs",
    )

    line_traces = [trace for trace in fig.data if trace.type == "scatter"]
    assert [trace.name for trace in line_traces] == ["月CPM", "周CPM", "日CPM"]
    assert all(trace.yaxis == "y2" for trace in line_traces)


def test_period_overview_chart_does_not_draw_cpm_spec_line() -> None:
    fig = _create_period_overview_chart(
        sheet_features_df=_sample_sheet_features(),
        period_capability_df=_sample_period_capability(),
        metric_key="cpm",
        metric_label="CPM",
        title="ARRAY | 15260 | 4PP_Rs",
    )

    assert all(getattr(shape, "y0", None) != 1.33 for shape in fig.layout.shapes)


def test_period_overview_chart_handles_empty_capability_with_reserved_period_axis() -> None:
    fig = _create_period_overview_chart(
        sheet_features_df=_sample_sheet_features(),
        period_capability_df=pd.DataFrame(),
        metric_key="cpm",
        metric_label="CPM",
        title="ARRAY | 15260 | 4PP_Rs",
    )

    labels = list(fig.layout.xaxis.categoryarray)
    assert labels == [
        "月 | 2026-05",
        "月 | 2026-06",
        "周 | 2026-W24",
        "周 | 2026-W25",
        "周 | 2026-W26",
        "日 | 2026-06-19",
        "日 | 2026-06-20",
        "日 | 2026-06-21",
        "日 | 2026-06-22",
        "日 | 2026-06-23",
        "日 | 2026-06-24",
        "日 | 2026-06-25",
    ]
    assert [trace.type for trace in fig.data].count("box") == 4
    assert all(trace.type != "scatter" for trace in fig.data)


def test_sheet_points_box_chart_uses_site_name_as_chamber_fallback() -> None:
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
    assert [trace.name for trace in fig.data] == ["P1", "P2"]


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
