"""超规片自动预警「查询」门控的 AppTest 测试（2026-09-03 需求轮次）。

覆盖：
- 未点击「查询」：不执行数据加载，且不渲染任何 st.info 提醒条
  （门控语义由「查询」按钮本身承担，2026-09-03 UI 优化轮次）；
- 点击后：置 session_state 签名并执行数据加载；
- 已提交状态普通 rerun 保持；
- 筛选 signature 变化（产品/监控类型/厂别）：回到未提交态（静默，无 info）。
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

# monitor_dashboard 的第三方组件在 bare/AppTest 收集阶段无法真实注册，沿用
# test_monitor_dashboard_type_rollup.py 的 stub 惯例。
streamlit_echarts_stub = types.ModuleType("streamlit_echarts")
streamlit_echarts_stub.st_echarts = lambda *args, **kwargs: None
streamlit_echarts_stub.JsCode = lambda code: code
sys.modules.setdefault("streamlit_echarts", streamlit_echarts_stub)

st_aggrid_stub = types.ModuleType("st_aggrid")
st_aggrid_stub.AgGrid = lambda *args, **kwargs: {}
st_aggrid_stub.GridOptionsBuilder = object
st_aggrid_stub.GridUpdateMode = types.SimpleNamespace(SELECTION_CHANGED="SELECTION_CHANGED")
st_aggrid_stub.DataReturnMode = types.SimpleNamespace()
st_aggrid_stub.JsCode = lambda code: code
sys.modules.setdefault("st_aggrid", st_aggrid_stub)

from streamlit.testing.v1 import AppTest

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "monitor_query_gate_app.py"


def _new_app() -> AppTest:
    return AppTest.from_file(str(FIXTURE_PATH))


def _assert_no_info(app: AppTest) -> None:
    """本页面渲染面不允许出现 st.info 提醒条（UI 优化轮次禁令）。"""
    assert len(app.info) == 0, [m.value for m in app.info]


def test_gate_blocks_data_loading_until_query_clicked() -> None:
    app = _new_app().run()

    assert not app.exception
    assert "gate_load_count" not in app.session_state
    _assert_no_info(app)
    assert not any("GATE_OPEN" in m.value for m in app.markdown)
    # 门控语义由按钮本身承担
    assert app.button(key="btn_monitor_query_submit").label == "🔍 查询"


def test_query_click_opens_gate_and_loads_data() -> None:
    app = _new_app().run()
    app.button(key="btn_monitor_query_submit").click().run()

    assert not app.exception
    assert app.session_state["gate_load_count"] == 1
    assert any("GATE_OPEN_DATA_LOADED" in m.value for m in app.markdown)
    _assert_no_info(app)


def test_submitted_state_persists_across_plain_reruns() -> None:
    app = _new_app().run()
    app.button(key="btn_monitor_query_submit").click().run()

    app.run()  # 普通 rerun：已提交状态保持，数据加载照常执行

    assert not app.exception
    assert app.session_state["gate_load_count"] == 2
    _assert_no_info(app)


def test_product_filter_change_returns_to_unsubmitted_state() -> None:
    app = _new_app().run()
    app.button(key="btn_monitor_query_submit").click().run()
    assert app.session_state["gate_load_count"] == 1

    product_multiselect = next(m for m in app.multiselect if m.label == "产品型号")
    product_multiselect.set_value(["M678"]).run()

    assert not app.exception
    assert app.session_state["gate_load_count"] == 1  # 本轮未执行数据加载
    _assert_no_info(app)  # 签名过期静默回到未提交态，无 info 文案
    assert not any("GATE_OPEN" in m.value for m in app.markdown)

    # 重新点击「查询」后按新签名加载
    app.button(key="btn_monitor_query_submit").click().run()
    assert app.session_state["gate_load_count"] == 2


def test_monitor_type_change_returns_to_unsubmitted_state() -> None:
    app = _new_app().run()
    app.button(key="btn_monitor_query_submit").click().run()

    app.selectbox(key="spc_data_type_filter").set_value("SPC").run()

    assert not app.exception
    assert app.session_state["gate_load_count"] == 1
    _assert_no_info(app)


def test_factory_filter_change_returns_to_unsubmitted_state() -> None:
    app = _new_app().run()
    app.button(key="btn_monitor_query_submit").click().run()

    factory_multiselect = next(m for m in app.multiselect if m.label == "厂别")
    factory_multiselect.set_value(["ARRAY", "OLED"]).run()

    assert not app.exception
    assert app.session_state["gate_load_count"] == 1
    _assert_no_info(app)


# ---------------------------------------------------------------------------
# Phase 11：行内布局（查询按钮在控制台行最右列，clicked 覆盖传回门控）
# ---------------------------------------------------------------------------
def _new_row_layout_app() -> AppTest:
    app = AppTest.from_file(str(FIXTURE_PATH))
    app.session_state["fixture_mode"] = "row_layout"
    return app


def test_row_layout_gate_blocks_until_query_clicked() -> None:
    """行内布局路径：未点击不加载、无 st.info；点击「查询」后加载。"""
    app = _new_row_layout_app().run()

    assert not app.exception
    assert "gate_load_count" not in app.session_state
    _assert_no_info(app)
    # 按钮渲染在控制台行内（同一脚本片段），key 不变
    assert app.button(key="btn_monitor_query_submit").label == "🔍 查询"

    app.button(key="btn_monitor_query_submit").click().run()

    assert not app.exception
    assert app.session_state["gate_load_count"] == 1
    assert any("GATE_OPEN_DATA_LOADED" in m.value for m in app.markdown)


def test_row_layout_filter_change_returns_to_unsubmitted_state() -> None:
    """行内布局路径：筛选变更后签名过期，静默回到未提交态。"""
    app = _new_row_layout_app().run()
    app.button(key="btn_monitor_query_submit").click().run()
    assert app.session_state["gate_load_count"] == 1

    product_multiselect = next(m for m in app.multiselect if m.label == "产品型号")
    product_multiselect.set_value(["M678"]).run()

    assert not app.exception
    assert app.session_state["gate_load_count"] == 1
    _assert_no_info(app)
    assert not any("GATE_OPEN" in m.value for m in app.markdown)
