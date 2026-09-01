import pandas as pd

from app.charts.qtime_domain.qtime_chart import build_qtime_figure


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


def test_qtime_figure_omits_the_spec_line_when_specification_is_unavailable() -> None:
    details = pd.DataFrame({"lot_id": ["L001"], "wait_time": [0.41]})

    figure = build_qtime_figure(details)

    assert [trace.type for trace in figure.data] == ["bar"]
