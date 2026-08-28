from contextlib import nullcontext

import pandas as pd

from app.sections.inline_domain.shared import filters as shared_filters
from app.sections.inline_domain.shared.filters import (
    apply_report_filter,
    filter_signature,
    get_available_factories,
    get_options_for_factory_steps,
    get_steps_for_factory,
    normalise_selection,
    render_cascade_filters,
    unique_sorted,
)


def _report_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"factory": "OLED", "step_id": "21200", "param_name": "PPA_B_X"},
            {"factory": "ARRAY", "step_id": "15260", "param_name": "4PP_Rs"},
            {"factory": "ARRAY", "step_id": "15260", "param_name": "4PP_UNI"},
            {"factory": "TP", "step_id": "41140", "param_name": "SE_L1T"},
        ]
    )


def test_unique_sorted_handles_empty_and_missing_column() -> None:
    assert unique_sorted(pd.DataFrame(), "factory") == []
    assert unique_sorted(pd.DataFrame({"a": [1]}), "factory") == []
    assert unique_sorted(_report_df(), "step_id") == ["15260", "21200", "41140"]


def test_normalise_selection_drops_unavailable_items() -> None:
    assert normalise_selection(["A", "X", "B"], ["A", "B"]) == ["A", "B"]
    assert normalise_selection(None, ["A"]) == []


def test_filter_signature_is_order_preserving_tuple() -> None:
    assert filter_signature("ARRAY", ["s2", "s1"], ["p1"]) == ("ARRAY", ("s2", "s1"), ("p1",))


def test_get_available_factories_preserves_standard_order() -> None:
    assert get_available_factories(_report_df()) == ["ARRAY", "OLED", "TP"]
    assert get_available_factories(pd.DataFrame()) == []


def test_get_steps_and_third_options_cascade() -> None:
    df = _report_df()
    assert get_steps_for_factory(df, "ARRAY") == ["15260"]
    assert get_steps_for_factory(df, "") == []
    assert get_options_for_factory_steps(df, "ARRAY", ["15260"], "param_name") == ["4PP_Rs", "4PP_UNI"]
    assert get_options_for_factory_steps(df, "ARRAY", [], "param_name") == []
    assert get_options_for_factory_steps(df, "ARRAY", ["15260"], "missing_column") == []


def test_apply_report_filter_by_factory_third_and_steps() -> None:
    df = _report_df().assign(v=[1, 2, 3, 4])
    out = apply_report_filter(df, "ARRAY", ["4PP_Rs"], ["15260"], third_column="param_name")
    assert list(out["v"]) == [2]
    assert apply_report_filter(pd.DataFrame(), "ARRAY", ["4PP_Rs"], ["15260"], third_column="param_name").empty


def _install_fake_widgets(monkeypatch, *, button_clicked: bool) -> dict:
    session: dict = {}
    captured = {"multiselect": {}, "button_kwargs": None}

    def fake_selectbox(_label, *, options, key, **_kw):
        return session.get(key, options[0])

    def fake_multiselect(label, *, options, key, **kw):
        captured["multiselect"][label] = {"options": options, "disabled": kw.get("disabled", False)}
        return session.get(key, [])

    def fake_button(_label, **kw):
        captured["button_kwargs"] = kw
        return button_clicked

    monkeypatch.setattr(shared_filters.st, "container", lambda **_kw: nullcontext())
    monkeypatch.setattr(shared_filters.st, "markdown", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        shared_filters.st, "columns", lambda spec, **_kw: [nullcontext() for _ in spec]
    )
    monkeypatch.setattr(shared_filters.st, "selectbox", fake_selectbox)
    monkeypatch.setattr(shared_filters.st, "multiselect", fake_multiselect)
    monkeypatch.setattr(shared_filters.st, "button", fake_button)
    monkeypatch.setattr(shared_filters.st, "session_state", session)
    captured["session"] = session
    return captured


def test_render_cascade_filters_applies_signature_under_prefix(monkeypatch) -> None:
    captured = _install_fake_widgets(monkeypatch, button_clicked=True)
    captured["session"].update(
        {
            "ctq_previous_factory_filter": "ARRAY",
            "ctq_step_filter": ["15260"],
        }
    )

    factory, params, steps, should_render = render_cascade_filters(
        _report_df(),
        key_prefix="ctq",
        third_label="参数名称",
        third_column="param_name",
    )

    assert factory == "ARRAY"
    assert steps == ["15260"]
    assert params == ["4PP_Rs", "4PP_UNI"]
    assert should_render is True
    assert captured["session"]["ctq_applied_filter_signature"] == (
        "ARRAY",
        ("15260",),
        ("4PP_Rs", "4PP_UNI"),
    )


def test_render_cascade_filters_factory_switch_resets_and_blocks_query(monkeypatch) -> None:
    captured = _install_fake_widgets(monkeypatch, button_clicked=True)
    captured["session"].update(
        {
            "aoi_rs_previous_factory_filter": "TP",
            "aoi_rs_step_filter": ["41140"],
            "aoi_rs_code_filter": ["SE_L1T"],
        }
    )

    factory, codes, steps, should_render = render_cascade_filters(
        _report_df(),
        key_prefix="aoi_rs",
        third_label="Code名称",
        third_column="param_name",
        third_kind="code",
    )

    assert factory == "ARRAY"
    assert steps == []
    assert codes == []
    assert should_render is False
    assert captured["multiselect"]["Code名称"]["disabled"] is True
    assert captured["button_kwargs"]["disabled"] is True
