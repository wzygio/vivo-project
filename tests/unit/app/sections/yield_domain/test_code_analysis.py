from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from app.sections.yield_domain import code_analysis


@pytest.mark.parametrize("selection,expected_message", [
    ({"groups": [], "codes_by_group": {}}, "请至少选择"),
    ({"groups": ["G"], "codes_by_group": {"G": ["C"]}, "should_render": False}, "尚未查询"),
])
def test_unsubmitted_filters_do_not_build_details(monkeypatch, selection, expected_message):
    messages = []
    monkeypatch.setattr(code_analysis, "st", SimpleNamespace(subheader=lambda *_: None, info=messages.append))
    monkeypatch.setattr(code_analysis, "create_group_batch_selection_ui", lambda **_: selection)
    render = Mock()
    monkeypatch.setattr(code_analysis, "render_code_compact_expanders", render)
    code_analysis.render_code_analysis_section.__wrapped__(
        pd.DataFrame(), warning_lines={}, mwd_code_data={}, lot_data={},
        sheet_data={}, mapping_data=None, hotspot_scripts=[], product_code="M678",
    )
    render.assert_not_called()
    assert expected_message in messages[0]


def test_submitted_filters_reuse_supplied_report_data(monkeypatch):
    monkeypatch.setattr(code_analysis, "st", SimpleNamespace(subheader=lambda *_: None))
    monkeypatch.setattr(code_analysis, "create_group_batch_selection_ui", lambda **_: {
        "groups": ["G"], "codes_by_group": {"G": ["C"]}, "should_render": True,
    })
    render = Mock()
    monkeypatch.setattr(code_analysis, "render_code_compact_expanders", render)
    data = dict(warning_lines={}, mwd_code_data={}, lot_data={}, sheet_data={},
                mapping_data=pd.DataFrame(), hotspot_scripts=[], product_code="M678",
                mapping_layout={"panel_rows": 5})
    code_analysis.render_code_analysis_section.__wrapped__(pd.DataFrame(), **data)
    render.assert_called_once()
    for name, value in data.items():
        assert render.call_args.kwargs[name] is value


def test_custom_threshold_is_forwarded_to_selector(monkeypatch):
    monkeypatch.setattr(code_analysis, "st", SimpleNamespace(subheader=lambda *_: None, info=lambda *_: None))
    selector = Mock(return_value={"groups": [], "codes_by_group": {}})
    monkeypatch.setattr(code_analysis, "create_group_batch_selection_ui", selector)
    code_analysis.render_code_analysis_section.__wrapped__(
        pd.DataFrame(), warning_lines={}, mwd_code_data={}, lot_data={},
        sheet_data={}, mapping_data=None, hotspot_scripts=[], product_code="M678",
        rate_threshold=0.0002,
    )
    assert selector.call_args.kwargs["rate_threshold"] == 0.0002
