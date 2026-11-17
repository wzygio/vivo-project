from __future__ import annotations

import pandas as pd
import pytest

from src.inline_domain.infrastructure.shared.measurement_preprocessor import (
    filter_invalid_measurement_values,
    filter_excluded_steps,
    filter_excluded_param_names,
    keep_latest_measurements,
)


def test_invalid_measurement_value_is_exact_and_preserves_other_negatives():
    from decimal import Decimal
    source = pd.DataFrame({"param_value": [-299.99, " -299.990 ", Decimal("-299.99"),
                                            -299.9901, -300., -1., 0., 6024., None, "invalid"]})
    original = source.copy(deep=True)
    source.attrs["coverage"] = "original"
    result = filter_invalid_measurement_values(source)
    assert result.index.tolist() == list(range(3, 10))
    assert result.attrs == source.attrs
    pd.testing.assert_frame_equal(source, original)


def test_excluded_step_preserves_other_steps_nulls_and_source() -> None:
    source = pd.DataFrame({
        "step_id": ["15690", 15690, 15690.0, " 15690 ", "11620", None, "15690A"],
        "param_value": range(7),
    })
    source.attrs["spc_source_coverage"] = {"source_generation": "original"}
    original = source.copy(deep=True)

    result = filter_excluded_steps(source)

    assert result.param_value.tolist() == [4, 5, 6]
    assert result.attrs == original.attrs
    pd.testing.assert_frame_equal(source, original)


def test_preprocessor_excludes_loss_but_keeps_mt_ch_parameters() -> None:
    measurements = pd.DataFrame(
        {
            "param_name": ["PPA_B_X", "TOTAL_LOSS_RATE", "MT_CH_PRESS_A"],
            "param_value": [1.0, 2.0, 3.0],
        }
    )

    result = filter_excluded_param_names(measurements)

    assert result["param_name"].tolist() == ["PPA_B_X", "MT_CH_PRESS_A"]


def test_latest_measurements_preserves_whole_winner_with_duplicate_indices() -> None:
    source = pd.DataFrame({
        "sheet_id": ["A", "B", "A", "A", "C", "C"],
        "site_name": [None, "P1", None, None, "P1", "P1"],
        "start_time": pd.to_datetime([
            "2026-09-15", "2026-09-14", "2026-09-13", "2026-09-15", None, "2026-09-14",
        ]),
        "param_value": [3, 5, 1, 4, 8, 9],
        "unit_id": ["old", "B", "older", "winner", "invalid", "C"],
    }, index=[0] * 6)
    original = source.copy(deep=True)

    result = keep_latest_measurements(source, ["sheet_id", "site_name"], "start_time")

    assert result.param_value.tolist() == [5, 4, 9]
    assert result.unit_id.tolist() == ["B", "winner", "C"]
    pd.testing.assert_frame_equal(source, original)


@pytest.mark.parametrize("empty", [False, True])
def test_unique_measurements_skip_sort_and_preserve_input(monkeypatch, empty: bool) -> None:
    source = pd.DataFrame({"sheet_id": ["B", "A"], "param_value": [2, 1]})
    if empty:
        source = source.iloc[:0]

    def unexpected_sort(*args, **kwargs):
        pytest.fail("Unique measurements should not require sorting")

    monkeypatch.setattr(pd.DataFrame, "sort_values", unexpected_sort)
    result = keep_latest_measurements(source, ["sheet_id"], "start_time")

    pd.testing.assert_frame_equal(result, source)
    assert result is not source
