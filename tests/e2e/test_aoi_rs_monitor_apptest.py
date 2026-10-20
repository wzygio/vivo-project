from io import BytesIO
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest


def _app(monkeypatch, tmp_path):
    from app.components import page_header
    from app.manager.session_manager import SessionManager
    from app.utils.app_setup import AppSetup
    from src.inline_domain import composition
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

    for target, name in [(AppSetup, "initialize_app"), (page_header, "render_page_header"),
                         (DatabaseManager, "__init__"), (ConfigLoader, "get_enabled_products"),
                         (SessionManager, "AVAILABLE_PRODUCTS"), (composition, "build_aoi_rs_monitor_service")]:
        monkeypatch.setattr(target, name, getattr(target, name))
    monkeypatch.setenv("VIVO_RS_MONITOR_FIXTURE_ROOT", str(tmp_path))
    return AppTest.from_file(str(Path(__file__).parent / "fixtures/aoi_rs_monitor_app.py")).run(timeout=60)


def test_actual_page_query_filters_products_and_failure(monkeypatch, tmp_path):
    app = _app(monkeypatch, tmp_path)
    assert not app.exception and not app.error
    assert not app.dataframe
    assert app.selectbox(key="aoi_rs_monitor_factory").value == "ARRAY"
    assert app.number_input(key="aoi_rs_monitor_weeks").value == 5
    app.button(key="aoi_rs_monitor_query_submit").click().run(timeout=60)
    assert not app.exception and not app.error
    assert len(app.dataframe) == 2
    assert set(app.dataframe[0].value["RSCode"]) == {"A1", "A2"}
    assert app.multiselect(key="aoi_rs_monitor_steps").value == ["11629"]
    payload = app.session_state["aoi_rs_monitor_payload"]
    exported = pd.read_excel(BytesIO(payload["excel"]), sheet_name=None, dtype={"站点": str, "RSCode": str})
    assert set(exported) == {"M626", "M678"}
    assert all("flag" not in frame and "policy_signature" not in frame for frame in exported.values())
    for product, frame in exported.items():
        pd.testing.assert_frame_equal(frame, payload["tables"][product], check_dtype=False)
    app.multiselect(key="aoi_rs_monitor_codes").set_value(["A1"]).run()
    assert not app.dataframe
    app.button(key="aoi_rs_monitor_query_submit").click().run(timeout=60)
    assert app.dataframe[0].value["RSCode"].tolist() == ["A1"]
    app.multiselect(key="aoi_rs_monitor_products").set_value(["M678"]).run()
    app.button(key="aoi_rs_monitor_query_submit").click().run(timeout=60)
    assert len(app.dataframe) == 1
    assert set(app.dataframe[0].value["产品"]) == {"M678"}
    app.selectbox(key="fixture_rs_mode").set_value("failure").run()
    app.query_params["admin"] = "true"
    app.run()
    before = (tmp_path / "history.xlsx").read_bytes()
    app.button(key="aoi_rs_monitor_refresh_data").click().run(timeout=60)
    assert not app.exception
    assert "AOI_RS 数据读取或计算失败" in app.error[0].value
    assert "FIXTURE_PRIVATE_FAILURE" not in app.error[0].value
    assert (tmp_path / "history.xlsx").read_bytes() == before


def test_policy_and_history_changes_hide_submitted_results(monkeypatch, tmp_path):
    import os

    app = _app(monkeypatch, tmp_path)
    app.button(key="aoi_rs_monitor_query_submit").click().run(timeout=60)
    assert len(app.dataframe) == 2
    app.session_state["fixture_rs_policy"] = "fixture-rs-v2"
    app.run()
    assert not app.dataframe
    app.button(key="aoi_rs_monitor_query_submit").click().run(timeout=60)
    assert len(app.dataframe) == 2
    path = tmp_path / "history.xlsx"
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    app.run()
    assert not app.dataframe and not app.exception


def test_ordinary_empty_and_failure_states(monkeypatch, tmp_path):
    app = _app(monkeypatch, tmp_path)
    app.selectbox(key="fixture_rs_mode").set_value("failure").run()
    app.button(key="aoi_rs_monitor_query_submit").click().run(timeout=60)
    assert not app.exception
    assert "AOI_RS 数据读取或计算失败" in app.error[0].value
    assert "FIXTURE_PRIVATE_FAILURE" not in app.error[0].value
    assert not app.dataframe
    app.selectbox(key="fixture_rs_mode").set_value("empty").run()
    app.button(key="aoi_rs_monitor_query_submit").click().run(timeout=60)
    assert not app.exception and not app.error and not app.dataframe
    assert any("暂无所选范围" in item.value for item in app.info)
