import pandas as pd
import plotly.graph_objects as go
import pytest

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


def test_full_spec_draws_all_lines_except_cl() -> None:
    # CL 线已在前端注释停用（后续可能重新启用），仅断言 Target Line
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
    }


def test_empty_spec_draws_nothing() -> None:
    fig = go.Figure()
    apply_measurement_spec_lines(fig, pd.DataFrame())
    assert _annotation_texts(fig) == set()


@pytest.mark.parametrize("target_fields", [{}, {"target": None}, {"target": float("nan")}, {"target": pd.NA}])
def test_missing_target_omits_line_without_midpoint_fallback(target_fields) -> None:
    fig = go.Figure()
    limits = pd.DataFrame([dict(usl=36.0, lsl=21.0, ucl=30.0, lcl=21.3, **target_fields)])
    apply_measurement_spec_lines(fig, limits)
    assert _annotation_texts(fig) == {"USL: 36", "LSL: 21", "UCL: 30", "LCL: 21.3"}
    assert len(fig.layout.shapes) == 4


def test_zero_target_is_drawn() -> None:
    fig = go.Figure()
    apply_measurement_spec_lines(fig, _spec_df(usl=2.0, lsl=-1.0, target=0.0))
    assert "Target: 0" in _annotation_texts(fig)


def test_format_spec_value_preserves_tiny_values() -> None:
    assert format_spec_value(1.6e-11) == "1.6e-11"
    assert format_spec_value(None) == "-"
    assert format_spec_value(12.0) == "12"


def test_y_range_covers_usl_lsl_when_defined() -> None:
    y_range = resolve_measurement_y_range([0.9, 1.0, 1.1], _spec_df(usl=1.2, lsl=0.8))
    assert y_range == [0.8, 1.2]


def test_y_range_none_without_spec() -> None:
    assert resolve_measurement_y_range([1.0, 2.0], pd.DataFrame()) is None
