from app.sections.inline_domain.shared.chart_type import (
    CHART_TYPE_BOX,
    CHART_TYPE_LINE,
    resolve_chart_type,
)


def test_resolve_chart_type_returns_line_when_token_matches_param_name() -> None:
    assert resolve_chart_type("SE_L1T_UNI", ["UNI"]) == CHART_TYPE_LINE


def test_resolve_chart_type_matches_case_insensitively() -> None:
    assert resolve_chart_type("se_l1t_uni", ["UNI"]) == CHART_TYPE_LINE
    assert resolve_chart_type("SE_L1T_UNI", ["uni"]) == CHART_TYPE_LINE


def test_resolve_chart_type_returns_box_when_no_token_matches() -> None:
    assert resolve_chart_type("4PP_Rs", ["UNI"]) == CHART_TYPE_BOX


def test_resolve_chart_type_returns_box_for_empty_config() -> None:
    assert resolve_chart_type("SE_L1T_UNI", []) == CHART_TYPE_BOX


def test_resolve_chart_type_tolerates_none_and_blank_tokens() -> None:
    assert resolve_chart_type(None, ["UNI"]) == CHART_TYPE_BOX
    assert resolve_chart_type("SE_L1T_UNI", ["", "  "]) == CHART_TYPE_BOX
