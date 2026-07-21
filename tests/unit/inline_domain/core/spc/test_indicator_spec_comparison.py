# -*- coding: utf-8 -*-
from src.inline_domain.core.spc.indicator_spec_comparison import (
    compare_specs,
    make_display_name,
    make_spec_identity,
    parse_monitor_spec,
)


def test_parse_target_arrow_uses_final_tolerance() -> None:
    parsed = parse_monitor_spec("Target±0.6μm→Target±0.5μm")

    assert parsed.comparable is True
    assert parsed.constraints[0].relation == "tolerance"
    assert parsed.constraints[0].value == 0.5
    assert parsed.constraints[0].unit == "μm"


def test_compare_tightened_when_upper_bound_decreases() -> None:
    result = compare_specs("Target±0.6μm", "Target±0.5μm")

    assert result.is_tightened is True
    assert "tolerance" in result.reason


def test_compare_tightened_when_lower_bound_increases() -> None:
    result = compare_specs("≥1.17E-05", "≥2.05E-05")

    assert result.is_tightened is True
    assert "lower" in result.reason


def test_compare_ambiguous_multivalue_expression_is_not_tightened() -> None:
    result = compare_specs("30° 5.8\n45° 5.9\n60° 5.0", "30° 5.0\n45° 5.0\n60° 4.8")

    assert result.is_tightened is False
    assert result.new_spec.comparable is False


def test_make_spec_identity_uses_occurrence_for_duplicate_rows() -> None:
    first = make_spec_identity(
        factory="Array",
        department="EPM",
        process_layer="ALL",
        station="电性检测",
        monitor_factor="STFT",
        rs_code="/",
        description="/",
        occurrence=1,
    )
    second = make_spec_identity(
        factory="Array",
        department="EPM",
        process_layer="ALL",
        station="电性检测",
        monitor_factor="STFT",
        rs_code="/",
        description="/",
        occurrence=2,
    )

    assert first != second
    assert first.endswith("#1")
    assert second.endswith("#2")


def test_display_name_keeps_identifiable_process_fields() -> None:
    display_name = make_display_name(
        factory="Array",
        department="DE",
        monitor_factor="CD",
        description="ESD",
        process_layer="BP-ILD",
        station="DE干刻",
        rs_code="/",
        occurrence=1,
    )

    assert "BP-ILD" in display_name
    assert "DE干刻" in display_name
