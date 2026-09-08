import pandas as pd

from app.charts.indicator_domain.ijp.chart import build_ijp_glass_figure


def _ratios() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "glass_id": ["G1", "G1", "G2", "G2"],
            "rs_code": ["C3DM1", "C3RA1", "C3DM1", "C3BH1"],
            "code_num": [3, 1, 1, 1],
            "ratio": [0.75, 0.25, 0.5, 0.5],
        }
    )


def test_ijp_figure_stacks_glass_code_ratios_as_percentages() -> None:
    figure = build_ijp_glass_figure(_ratios())

    bars = [trace for trace in figure.data if trace.type == "bar"]
    assert [trace.name for trace in bars] == ["C3DM1", "C3RA1", "C3BH1"]
    c3dm1 = bars[0]
    assert list(c3dm1.x) == ["G1", "G2"]
    assert figure.layout.xaxis.title.text == "Glass ID"
    assert list(c3dm1.y) == [75.0, 50.0]
    assert figure.layout.barmode == "stack"
    assert figure.layout.title.text == "OLED RS Overflow By Glass ID"
    assert figure.layout.yaxis.range == (0, 100)
    assert figure.layout.xaxis.type == "category"
    assert figure.layout.legend.yanchor == "bottom"
    assert figure.layout.legend.y > 1
    assert figure.layout.margin.t >= 100


def test_ijp_figure_contains_only_code_ratio_bars() -> None:
    figure = build_ijp_glass_figure(_ratios())

    assert all(trace.type == "bar" for trace in figure.data)


def test_ijp_figure_accepts_a_printer_title() -> None:
    figure = build_ijp_glass_figure(_ratios(), title="3CEE01-IK2-PR1")

    assert figure.layout.title.text == "3CEE01-IK2-PR1"


def test_ijp_figure_handles_empty_and_malformed_frames() -> None:
    empty = build_ijp_glass_figure(pd.DataFrame())
    assert len(empty.data) == 0
    assert empty.layout.title.text == "OLED RS Overflow By Glass ID"

    malformed = build_ijp_glass_figure(pd.DataFrame({"glass_id": ["2026-08-31"]}))
    assert len(malformed.data) == 0
