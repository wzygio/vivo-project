"""Box populations use detail samples, never the five summary statistics."""

import json

import pandas as pd

from app.charts.indicator_domain.point_test.chart import create_detail_box_charts


def test_boxes_separate_parameters_and_sheets_and_keep_all_finite_samples():
    detail = pd.DataFrame({
        "SHEET_ID": ["S1"] * 6 + ["S2", "S1", None],
        "PARAM_NAME": ["CD1"] * 7 + ["CD2", "CD1"],
        "STEP_ID": ["STEP1"] * 9,
        "PARAM_VALUE": [1, 2, 3, 4, "5", float("inf"), 10, 1000, 99],
        "PRIVATE_COLUMN": ["private"] * 9,
    })
    original = detail.copy(deep=True)
    charts = create_detail_box_charts(detail)
    assert list(charts) == [("STEP1", "CD1"), ("STEP1", "CD2")]
    assert [list(trace.y) for trace in charts[("STEP1", "CD1")].data] == [[1, 2, 3, 4, 5], [10]]
    assert [trace.name for trace in charts[("STEP1", "CD1")].data] == ["S1", "S2"]
    assert list(charts[("STEP1", "CD2")].data[0].y) == [1000]
    assert all(trace.quartilemethod == "linear" for fig in charts.values() for trace in fig.data)
    assert all(trace.boxpoints is False for fig in charts.values() for trace in fig.data)
    assert "private" not in json.dumps([fig.to_plotly_json() for fig in charts.values()])
    pd.testing.assert_frame_equal(detail, original)


def test_no_usable_detail_produces_no_charts():
    assert create_detail_box_charts(pd.DataFrame()) == {}
    assert create_detail_box_charts(pd.DataFrame({
        "SHEET_ID": ["S"], "PARAM_NAME": ["CD1"], "PARAM_VALUE": ["invalid"],
    })) == {}


def test_same_sheet_and_parameter_at_different_steps_are_never_pooled():
    charts = create_detail_box_charts(pd.DataFrame({
        "SHEET_ID": ["S1"] * 3, "STEP_ID": ["STEP1", "STEP2", "STEP1"],
        "PARAM_NAME": ["CD1"] * 3, "PARAM_VALUE": [1, 100, 2],
    }))
    assert list(charts[("STEP1", "CD1")].data[0].y) == [1, 2]
    assert list(charts[("STEP2", "CD1")].data[0].y) == [100]
