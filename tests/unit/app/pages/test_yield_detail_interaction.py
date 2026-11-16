"""Detail controls use captured product data and never mutate/expose source internals."""
from datetime import date
import importlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest


@pytest.fixture
def detail_page(monkeypatch, tmp_path):
    from app.components import indicator_cache, page_header
    from app.manager.session_manager import SessionManager
    from src.shared_kernel.infrastructure import db_handler

    analysis = importlib.import_module("yield_domain.application.yield_service").YieldAnalysisService
    config = SimpleNamespace(data_source=SimpleNamespace(product_code="Z553"), processing={})
    monkeypatch.setattr(SessionManager, "get_active_config", lambda: config)
    monkeypatch.setattr(SessionManager, "get_product_dir", lambda: tmp_path)
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: object())
    monkeypatch.setattr(page_header, "render_page_header", lambda *args, **kwargs: None)
    monkeypatch.setattr(page_header, "extract_cached_funcs", lambda *args: [])
    monkeypatch.setattr(indicator_cache, "build_indicator_product_cache_signature", lambda *args: "test")
    monkeypatch.setattr(analysis, "build_cache_context", lambda *args: {
        "analysis_start_date": "2026-08-01", "analysis_end_date": str(date.today()),
    })
    monkeypatch.setattr(analysis, "get_data_health", lambda *args, **kwargs: {"status": "fresh"})
    day = date.today().strftime("%Y%m%d")
    summary = pd.DataFrame([dict(
        sheet_id="S1", lot_id="L1", warehousing_time=day, array_input_time=None,
        pass_rate=.98, array_pixel_rate=.01, array_line_rate=.01, oled_mura_rate=.0,
        internal_flag="private marker",
    )])
    details = pd.DataFrame([dict(
        sheet_id="S1", lot_id="L1", defect_group="Array_Pixel", defect_desc="CODE1",
        defect_panel_count=1, defect_rate=.01, internal_flag="private marker",
    )])
    payload = {"group_level_summary_for_table": summary, "code_level_details": {"Array_Pixel": details}}
    calls = []

    def load(**kwargs):
        calls.append(kwargs)
        return payload

    monkeypatch.setattr(analysis, "get_sheet_defect_rates", staticmethod(load))
    monkeypatch.setattr(analysis, "get_lot_defect_rates", staticmethod(load))
    return payload, calls


@pytest.mark.parametrize("grain", ["Sheet", "Lot"])
def test_detail_controls_are_fragment_owned_and_keep_input_immutable(detail_page, grain):
    payload, calls = detail_page
    before = payload["group_level_summary_for_table"].copy(deep=True)
    page = Path(__file__).parents[4] / "app/pages" / f"入库不良率By{grain}明细表.py"
    app = AppTest.from_file(str(page)).run()
    assert not app.exception
    assert len(calls) == 1
    metadata = app.session_state._state._new_widget_state.widget_metadata
    parent_ids = {metadata[widget.id].fragment_id for widget in app.date_input}
    assert len(parent_ids) == 1 and None not in parent_ids
    areas = [metadata[widget.id].fragment_id for widget in app.text_area]
    assert len(areas) == 2 and all(areas)
    assert areas[0] in parent_ids and areas[1] not in parent_ids
    selectors = {metadata[widget.id].fragment_id for widget in app.selectbox}
    assert selectors and None not in selectors and not selectors.intersection(parent_ids)
    assert areas[1] not in selectors
    pd.testing.assert_frame_equal(payload["group_level_summary_for_table"], before)
    for table in app.dataframe:
        assert "internal_flag" not in table.value.columns


@pytest.mark.parametrize("grain", ["Sheet", "Lot"])
@pytest.mark.parametrize("mode", ["empty", "failure"])
def test_detail_unavailable_states_do_not_render_old_controls(detail_page, monkeypatch, grain, mode):
    module = importlib.import_module("yield_domain.application.yield_service")

    def unavailable(**kwargs):
        if mode == "failure":
            raise module.YieldDataLoadError("private source diagnostic")
        return {}

    monkeypatch.setattr(module.YieldAnalysisService, f"get_{grain.lower()}_defect_rates", staticmethod(unavailable))
    page = Path(__file__).parents[4] / "app/pages" / f"入库不良率By{grain}明细表.py"
    app = AppTest.from_file(str(page)).run()
    assert not app.exception
    assert not app.date_input and not app.text_area
    assert bool(app.error) == (mode == "failure")
    assert all("private source diagnostic" not in error.value for error in app.error)
