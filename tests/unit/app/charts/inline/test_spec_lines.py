import pandas as pd
import plotly.graph_objects as go

from app.charts.inline_domain.spec_lines import (
    apply_measurement_spec_lines,
    format_spec_value,
    resolve_measurement_y_range,
)


def _spec_df(**overrides) -> pd.DataFrame:
    row = {"usl": None, "lsl": None, "ucl": None, "lcl": None, "target": None}
    row.update(overrides)
    return pd.DataFrame([row])


def _annotation_texts(fig: go.Figure) -> set[str]:
    return {annotation.text for annotation in fig.layout.annotations}


def test_lsl_zero_draws_only_upper_spec_lines() -> None:
    fig = go.Figure()
    apply_measurement_spec_lines(
        fig,
        _spec_df(usl=12.0, lsl=0.0, ucl=10.0, lcl=2.0, target=6.0),
    )
    assert _annotation_texts(fig) == {"USL: 12", "UCL: 10"}


def test_lsl_missing_draws_only_upper_spec_lines() -> None:
    fig = go.Figure()
    apply_measurement_spec_lines(fig, _spec_df(usl=12.0, ucl=10.0, lcl=2.0))
    assert _annotation_texts(fig) == {"USL: 12", "UCL: 10"}


def test_full_spec_draws_all_lines() -> None:
    fig = go.Figure()
    apply_measurement_spec_lines(
        fig,
        _spec_df(usl=1.2, lsl=0.8, ucl=1.15, lcl=0.85, target=1.0),
    )
    assert _annotation_texts(fig) == {
        "USL: 1.2",
        "LSL: 0.8",
        "UCL: 1.15",
        "LCL: 0.85",
        "Target: 1",
        "CL: 1",
    }


def test_empty_spec_draws_nothing() -> None:
    fig = go.Figure()
    apply_measurement_spec_lines(fig, pd.DataFrame())
    assert _annotation_texts(fig) == set()


def test_format_spec_value_preserves_tiny_values() -> None:
    assert format_spec_value(1.6e-11) == "1.6e-11"
    assert format_spec_value(None) == "-"
    assert format_spec_value(12.0) == "12"


def test_y_range_covers_usl_lsl_when_defined() -> None:
    y_range = resolve_measurement_y_range([0.9, 1.0, 1.1], _spec_df(usl=1.2, lsl=0.8))
    assert y_range == [0.8, 1.2]


def test_y_range_none_without_spec() -> None:
    assert resolve_measurement_y_range([1.0, 2.0], pd.DataFrame()) is None
