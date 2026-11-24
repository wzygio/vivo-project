"""Each single-product report must wait for its own header's first submission."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from app.components import indicator_cache, page_header
from app.manager.session_manager import SessionManager
from app.sections.yield_domain import resource_config
from app.utils.app_setup import AppSetup
from src.inline_domain import composition
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure import db_handler


@pytest.mark.parametrize("page", [
    "SPC监控报表", "CTQ监控报表", "AOI_RS监控报表", "AOI_TT监控报表",
    "入库不良率分析看板", "入库不良率ByLot明细表", "入库不良率BySheet明细表",
])
def test_report_reads_and_refreshes_only_its_submitted_product(monkeypatch, page):
    db = Mock(side_effect=AssertionError("unexpected report load"))
    config = Mock(side_effect=AssertionError("unexpected default product"))
    reads, refreshes, resets = [], [], []

    def capture_read(_signature, product, _indicators):
        reads.append(product)
        st.stop()  # The page has reached its first product-specific cache lookup.

    def capture_refresh(_db, product, _end):
        refreshes.append(product)
        return True

    monkeypatch.setattr(db_handler.DatabaseManager, "__new__", db)
    monkeypatch.setattr(AppSetup, "initialize_app", lambda: None)
    monkeypatch.setattr(SessionManager, "get_active_config", config)
    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda: ["M626", "Z622"])
    monkeypatch.setattr(page_header, "perform_hard_reset",
                        lambda _funcs, product, _indicators, **kw: resets.append(product))
    monkeypatch.setattr(page_header, "detect_project_changes", lambda: False)
    app = AppTest.from_file(str(Path("app/pages") / f"{page}.py"))
    app.query_params["admin"] = "true"
    app.session_state["selected_product_code"] = "M626"
    app.session_state["header_product_query_其他页面"] = "Z622"
    app.run(timeout=30)

    config.assert_not_called()
    db.assert_not_called()
    assert not app.exception
    assert [button.label for button in app.button if not button.disabled] == ["查询"]
    assert len([button for button in app.button if button.disabled]) == 2
    assert reads == []

    monkeypatch.setattr(ConfigLoader, "load_config", lambda product: SimpleNamespace(
        data_source=SimpleNamespace(product_code=product),
    ))
    monkeypatch.setattr(indicator_cache, "build_indicator_product_cache_signature", capture_read)
    monkeypatch.setattr(composition, "refresh_raw_measurements", capture_refresh)
    monkeypatch.setattr(composition, "refresh_aoi_rs_snapshots", capture_refresh)
    monkeypatch.setattr(resource_config.YieldAnalysisService, "safe_refresh_snapshots",
                        lambda _db, cfg: capture_refresh(_db, cfg.data_source.product_code, None))
    title = app.title[0].value
    app.selectbox[0].select("Z622")
    app.button(key=f"header_product_query_{title}_submit").click().run()
    assert not app.exception
    assert reads == ["Z622"]

    # Refresh handlers construct their own connection only when clicked.
    db.side_effect = None
    db.return_value = object()
    app.selectbox[0].select("M626").run()
    app.button(key=f"btn_refresh_{title}").click().run()
    app.button(key=f"btn_clear_{title}").click().run()
    assert not app.exception
    assert refreshes == ["Z622"]
    assert resets == ["Z622", "Z622"]
    assert set(reads) == {"Z622"}
    app.button(key=f"header_product_query_{title}_submit").click().run()
    assert not app.exception
    assert reads[-1] == "M626"
