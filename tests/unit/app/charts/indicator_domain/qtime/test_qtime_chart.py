import pandas as pd

from app.charts.indicator_domain.qtime.chart import build_qtime_figure


def test_single_low_lot_has_visible_full_width_spec_and_axis_headroom():
    figure = build_qtime_figure(pd.DataFrame({
        "lot_id": ["L001"], "wait_time": [0.1], "q_spec": [370.0],
    }))
    assert figure.layout.yaxis.range[1] > 370
    line = figure.layout.shapes[0]
    assert line.y0 == line.y1 == 370
    assert line.x0 == 0 and line.x1 == 1 and line.xref == "x domain"


def test_qtime_figure_renders_wait_time_bars_and_the_matching_specification() -> None:
    details = pd.DataFrame(
        {
            "lot_id": ["L001", "L002"],
            "step_desc": ["M3_DE->M3_STR", "M3_DE->M3_STR"],
            "wait_time": [0.41, 1.26],
            "q_spec": [2.5, 2.5],
        }
    )

    figure = build_qtime_figure(details)

    assert [trace.type for trace in figure.data] == ["bar", "scatter"]
    assert list(figure.data[0].x) == ["L001", "L002"]
    assert list(figure.data[0].y) == [0.41, 1.26]
    assert figure.data[0].name == "M3_DE->M3_STR"
    assert list(figure.data[1].y) == [2.5, 2.5]
    assert figure.data[1].name == "QTime规格"
    assert figure.data[1].line.color == "#ef4444"
    assert figure.data[1].showlegend is False
    assert figure.layout.xaxis.tickangle == -45
    assert figure.layout.yaxis.zeroline is True
    assert figure.layout.yaxis.zerolinecolor == "#94a3b8"
    assert figure.layout.yaxis.zerolinewidth == 1.5


def test_qtime_figure_omits_the_spec_line_when_specification_is_unavailable() -> None:
    details = pd.DataFrame({"lot_id": ["L001"], "wait_time": [0.41]})

    figure = build_qtime_figure(details)

    assert [trace.type for trace in figure.data] == ["bar"]


def test_qtime_figure_keeps_selected_paths_visually_distinct() -> None:
    details = pd.DataFrame(
        {
            "lot_id": ["L001", "L002", "L003", "L004"],
            "step_desc": ["A->B", "A->B", "B->C", "B->C"],
            "wait_time": [10, 12, 20, 21],
            "q_spec": [15, 15, 25, 25],
        }
    )

    figure = build_qtime_figure(details)

    assert [trace.type for trace in figure.data] == [
        "bar",
        "bar",
        "scatter",
        "scatter",
    ]
    assert [trace.name for trace in figure.data[:2]] == ["A->B", "B->C"]
    assert list(figure.data[2].x) == ["L001", "L002"]
    assert list(figure.data[3].x) == ["L003", "L004"]
    assert figure.data[2].showlegend is False
    assert figure.data[3].showlegend is False


def test_repeated_lot_passages_have_separate_bars_without_changing_facts():
    details = pd.DataFrame({
        "lot_id": ["REPEATED_LOT", "REPEATED_LOT"],
        "step_desc": ["1A200->21100", "1A200->21100"],
        "timekey": ["20261101080000", "20261101180000"],
        "wait_time": [130., 120.], "q_spec": [200., 200.],
    })
    before = details.copy(deep=True)

    figure = build_qtime_figure(details)

    assert len(set(figure.data[0].x)) == 2
    assert list(figure.data[0].y) == [130., 120.]
    assert list(figure.data[1].x) == list(figure.data[0].x)
    assert list(figure.layout.xaxis.ticktext) == ["REPEATED_LOT", "REPEATED_LOT"]
    assert [row[0] for row in figure.data[0].customdata] == ["REPEATED_LOT", "REPEATED_LOT"]
    assert figure.layout.shapes[0].y0 == 200.
    pd.testing.assert_frame_equal(details, before)
