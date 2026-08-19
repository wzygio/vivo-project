import pandas as pd
import pytest

from app.sections.inline_domain.shared.chart_type import CHART_TYPE_BOX, CHART_TYPE_LINE
from app.sections.inline_domain.shared.sheet_charts import (
    create_period_overview_chart,
    create_sheet_points_box_chart,
    create_sheet_points_box_charts,
)


def _sheet_features_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "12140",
                "param_name": "THK",
                "sheet_id": "S1",
                "sheet_start_time": "2026-07-01 08:00:00",
                "sheet_mean": 9.0,
                "main_process_unit_id": "C1",
                "usl": 12.0,
                "lsl": 0.0,
                "ucl": 10.0,
            },
            {
                "factory": "ARRAY",
                "step_id": "12140",
                "param_name": "THK",
                "sheet_id": "S2",
                "sheet_start_time": "2026-07-02 09:00:00",
                "sheet_mean": 9.5,
                "main_process_unit_id": "C2",
                "usl": 12.0,
                "lsl": 0.0,
                "ucl": 10.0,
            },
        ]
    )


def _raw_df() -> pd.DataFrame:
    return _sheet_features_df().rename(columns={"sheet_mean": "param_value"})


def test_period_overview_draws_box_traces_with_upper_only_spec() -> None:
    figure = create_period_overview_chart(
        sheet_features_df=_sheet_features_df(),
        period_capability_df=pd.DataFrame(),
        raw_measurements_df=_raw_df(),
        period_box_source="point_value",
        title="ARRAY | 12140 | THK | 月周天分布",
    )
    assert [trace for trace in figure.data if trace.type == "box"]
    annotation_texts = {annotation.text for annotation in figure.layout.annotations}
    assert annotation_texts == {"USL: 12", "UCL: 10"}


def test_pass_time_line_chart_uses_date_axis() -> None:
    figure = create_sheet_points_box_chart(
        raw_measurements_df=_raw_df(),
        sort_mode="按过货时间排序",
        title="t",
        spec_df=_sheet_features_df(),
        chart_type=CHART_TYPE_LINE,
    )
    assert figure.layout.xaxis.type == "date"
    assert figure.layout.xaxis.title.text == "过货时间"
    scatter_traces = [trace for trace in figure.data if trace.type == "scatter"]
    assert scatter_traces


def test_pass_time_box_chart_keeps_category_axis() -> None:
    figure = create_sheet_points_box_chart(
        raw_measurements_df=_raw_df(),
        sort_mode="按过货时间排序",
        title="t",
        spec_df=_sheet_features_df(),
        chart_type=CHART_TYPE_BOX,
    )
    assert figure.layout.xaxis.type != "date"
    assert [trace for trace in figure.data if trace.type == "box"]


def test_chamber_box_chart_colors_traces_by_chamber() -> None:
    figure = create_sheet_points_box_chart(
        raw_measurements_df=_raw_df(),
        sort_mode="按腔室排序",
        title="t",
        spec_df=_sheet_features_df(),
        chart_type=CHART_TYPE_BOX,
    )
    box_traces = [trace for trace in figure.data if trace.type == "box"]
    assert len(box_traces) == 2
    colors = {trace.marker.color for trace in box_traces}
    assert len(colors) == 2  # C1/C2 两腔室不同色


def test_sheet_points_box_charts_returns_chamber_and_time_figures() -> None:
    chamber_fig, time_fig = create_sheet_points_box_charts(
        raw_measurements_df=_raw_df(),
        title_prefix="ARRAY | 12140 | THK",
        spec_df=_sheet_features_df(),
        chart_type=CHART_TYPE_BOX,
    )
    assert "By主站点设备/腔室" in chamber_fig.layout.title.text
    assert "By过货时间" in time_fig.layout.title.text


def test_empty_raw_measurements_returns_placeholder_figure() -> None:
    figure = create_sheet_points_box_chart(
        raw_measurements_df=pd.DataFrame(),
        sort_mode="按腔室排序",
        title="t",
    )
    assert figure.layout.title.text == "t"
    assert len(figure.data) == 0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
