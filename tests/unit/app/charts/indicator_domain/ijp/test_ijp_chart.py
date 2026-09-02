import pandas as pd

from app.charts.indicator_domain.ijp.chart import build_ijp_daily_figure


def _ratios() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "day": ["2026-08-31", "2026-08-31", "2026-09-01", "2026-09-01"],
            "rs_code": ["C3DM1", "C3RA1", "C3DM1", "C3BH1"],
            "code_num": [3, 1, 1, 1],
            "ratio": [0.75, 0.25, 0.5, 0.5],
        }
    )


def test_ijp_figure_stacks_daily_code_ratios_as_percentages() -> None:
    figure = build_ijp_daily_figure(_ratios())

    bars = [trace for trace in figure.data if trace.type == "bar"]
    assert [trace.name for trace in bars] == ["C3DM1", "C3RA1", "C3BH1"]
    c3dm1 = bars[0]
    assert list(c3dm1.x) == ["2026-08-31", "2026-09-01"]
    assert list(c3dm1.y) == [75.0, 50.0]
    assert figure.layout.barmode == "stack"
    assert figure.layout.title.text == "OLED RS Overflow By天"
    assert figure.layout.yaxis.range == (0, 100)


def test_ijp_figure_draws_the_target_reference_line_when_given() -> None:
    figure = build_ijp_daily_figure(_ratios(), target=5.0)

    target = figure.data[-1]
    assert target.type == "scatter"
    assert target.name == "Target"
    assert set(target.y) == {5.0}
    assert target.line.dash == "dash"


def test_ijp_figure_omits_target_line_when_not_given() -> None:
    figure = build_ijp_daily_figure(_ratios())

    assert all(trace.type == "bar" for trace in figure.data)


def test_ijp_figure_handles_empty_and_malformed_frames() -> None:
    empty = build_ijp_daily_figure(pd.DataFrame())
    assert len(empty.data) == 0
    assert empty.layout.title.text == "OLED RS Overflow By天"

    malformed = build_ijp_daily_figure(pd.DataFrame({"day": ["2026-08-31"]}))
    assert len(malformed.data) == 0
