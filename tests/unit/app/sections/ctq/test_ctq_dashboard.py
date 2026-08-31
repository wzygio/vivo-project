from contextlib import nullcontext

import pandas as pd

from app.sections.inline_domain.ctq import ctq_dashboard
from app.sections.inline_domain.ctq.ctq_dashboard import (
    create_ctq_period_overview_chart,
    render_ctq_indicator_sections,
)


def test_ctq_period_overview_stays_box_for_uni_parameters() -> None:
    sheet_features_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "12140",
                "param_name": "SE_L1T_UNI",
                "sheet_id": "CTQ00000101",
                "sheet_start_time": "2026-07-01 08:00:00",
                "sheet_mean": 1.0,
                "usl": 1.2,
                "lsl": 0.8,
                "ucl": 1.15,
                "lcl": 0.85,
                "target": 1.0,
                "chart_type": "line",
            },
            {
                "factory": "ARRAY",
                "step_id": "12140",
                "param_name": "SE_L1T_UNI",
                "sheet_id": "CTQ00000102",
                "sheet_start_time": "2026-07-02 08:00:00",
                "sheet_mean": 1.05,
                "usl": 1.2,
                "lsl": 0.8,
                "ucl": 1.15,
                "lcl": 0.85,
                "target": 1.0,
                "chart_type": "line",
            },
        ]
    )
    raw_measurements_df = sheet_features_df.rename(columns={"sheet_mean": "param_value"})

    figure = create_ctq_period_overview_chart(
        sheet_features_df=sheet_features_df,
        raw_measurements_df=raw_measurements_df,
        title="ARRAY | 12140 | SE_L1T_UNI | 月周天分布",
    )

    assert [trace for trace in figure.data if trace.type == "box"]
    assert not [trace for trace in figure.data if trace.type == "scatter"]


def test_ctq_indicator_sections_render_distributions_without_capability_widgets(monkeypatch) -> None:
    period_figure = object()
    chamber_figure = object()
    time_figure = object()
    rendered_figures: list[object] = []
    column_calls: list[int] = []
    captured: dict[str, object] = {}
    sheet_features_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "12140",
                "param_name": "SE_L1T_UNI",
                "sheet_id": "S1",
                "sheet_start_time": "2026-07-01",
                "sheet_mean": 1.0,
                "chart_type": "line",
            }
        ]
    )
    raw_measurements_df = sheet_features_df.rename(columns={"sheet_mean": "param_value"})

    monkeypatch.setattr(ctq_dashboard.st, "expander", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(
        ctq_dashboard.st,
        "columns",
        lambda count: column_calls.append(count) or [nullcontext() for _ in range(count)],
    )
    monkeypatch.setattr(
        ctq_dashboard.st,
        "plotly_chart",
        lambda figure, **_kwargs: rendered_figures.append(figure),
    )
    monkeypatch.setattr(
        ctq_dashboard.st,
        "metric",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("CTQ must not render metrics")),
        raising=False,
    )
    monkeypatch.setattr(
        ctq_dashboard.st,
        "dataframe",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("CTQ must not render capability tables")),
    )
    monkeypatch.setattr(
        ctq_dashboard,
        "create_ctq_period_overview_chart",
        lambda **kwargs: captured.update(kwargs) or period_figure,
    )
    monkeypatch.setattr(
        ctq_dashboard,
        "_create_sheet_points_box_charts",
        lambda **kwargs: captured.update({"sheet_chart_type": kwargs["chart_type"]})
        or (chamber_figure, time_figure),
    )

    render_ctq_indicator_sections(
        sheet_features_df=sheet_features_df,
        raw_measurements_df=raw_measurements_df,
    )

    assert rendered_figures == [period_figure, chamber_figure, time_figure]
    assert column_calls == [3]
    assert "chart_type" not in captured
    assert captured["sheet_chart_type"] == "line"


def test_ctq_chart_with_zero_lsl_draws_only_upper_spec_lines() -> None:
    sheet_features_df = pd.DataFrame(
        [
            {
                "sheet_id": "S1",
                "sheet_start_time": "2026-07-01",
                "sheet_mean": 9.0,
                "usl": 12.0,
                "lsl": 0.0,
                "ucl": 10.0,
                "lcl": 2.0,
                "target": 6.0,
                "chart_type": "box",
            }
        ]
    )
    raw_measurements_df = sheet_features_df.rename(columns={"sheet_mean": "param_value"})

    figure = create_ctq_period_overview_chart(
        sheet_features_df=sheet_features_df,
        raw_measurements_df=raw_measurements_df,
        title="ARRAY | 12140 | THK | 月周天分布",
    )

    annotation_texts = {annotation.text for annotation in figure.layout.annotations}
    assert annotation_texts == {"USL: 12", "UCL: 10"}


def test_ctq_period_chart_preserves_tiny_upper_spec_values_in_labels() -> None:
    sheet_features_df = pd.DataFrame(
        [
            {
                "sheet_id": "S1",
                "sheet_start_time": "2026-07-01",
                "sheet_mean": 3.0e-12,
                "usl": 1.6e-11,
                "lsl": 0.0,
                "ucl": 9.7e-12,
                "lcl": 0.0,
                "target": 0.0,
            }
        ]
    )
    raw_measurements_df = sheet_features_df.rename(columns={"sheet_mean": "param_value"})

    figure = create_ctq_period_overview_chart(
        sheet_features_df=sheet_features_df,
        raw_measurements_df=raw_measurements_df,
        title="ARRAY | 1B990 | TFT_7_IOFF1 | 月周天分布",
    )

    annotation_texts = {annotation.text for annotation in figure.layout.annotations}
    assert annotation_texts == {"USL: 1.6e-11", "UCL: 9.7e-12"}


def test_ctq_indicator_sections_report_empty_filter_results(monkeypatch) -> None:
    messages: list[str] = []
    monkeypatch.setattr(ctq_dashboard.st, "info", messages.append)

    render_ctq_indicator_sections(
        sheet_features_df=pd.DataFrame(),
        raw_measurements_df=pd.DataFrame(),
    )

    assert messages == ["当前筛选条件下无 CTQ 数据。"]


def test_ctq_admin_panel_contains_only_the_namespaced_oos_modifier(monkeypatch) -> None:
    captured: dict[str, object] = {}
    decoration_result = object()
    monkeypatch.setattr(ctq_dashboard.st, "expander", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(
        ctq_dashboard.st,
        "tabs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("CTQ must not render CPK tabs")),
    )
    monkeypatch.setattr(
        ctq_dashboard,
        "render_sheet_oos_decoration_admin",
        lambda result, **kwargs: captured.update({"result": result, **kwargs}),
    )

    ctq_dashboard.render_ctq_decoration_admin(decoration_result)

    assert captured == {
        "result": decoration_result,
        "show_expander": False,
        "report_name": "CTQ",
        "key_prefix": "ctq",
    }
