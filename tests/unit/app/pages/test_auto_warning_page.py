"""自动预警看板页面组合层测试（2026-09-03 需求轮次）。

- 页头不渲染产品筛选（show_product_filter=False，全产品视图）；
- 「超规片自动预警」区查询门控：未点击「查询」不执行签名预算与数据加载，
  点击后才执行（monkeypatch 计数）；
- 模块化结构：每个模块 = st.subheader 标题 + st.expander（默认展开）；
- 渲染面无 st.info 提醒条（UI 优化轮次禁令）；
- 矩阵筛选条常驻模块 Expander（未加载也渲染，且只渲染一处），加载后
  由 render_alert_matrix_board 按同一选择切片（filter_selection 透传）。

bare 模式运行页面脚本（runpy），重依赖全部 monkeypatch 替换；页面门控交互
语义本身由 tests/unit/app/sections/monitor/test_monitor_query_gate.py 覆盖。
"""

from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
import runpy
import sys
import types
from types import SimpleNamespace

import pandas as pd

# monitor_dashboard 的第三方组件在 bare 模式下无法真实注册，沿用
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

import streamlit as st

from app.components import page_header
from app.manager import compliance_manager
from app.manager.session_manager import SessionManager
from app.sections.inline_domain.monitor import alert_matrix, monitor_dashboard
from app.utils import step_labels
from app.utils.app_setup import AppSetup
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.shared import decision_signature
from src.shared_kernel.infrastructure import db_handler

PAGE_PATH = Path(__file__).parents[4] / "app" / "pages" / "自动预警看板.py"
QUERY_BUTTON_KEY = "btn_monitor_query_submit"
MATRIX_FILTER_KEYS = (
    "alert_matrix_data_type",
    "alert_matrix_products",
    "alert_matrix_factories",
)


def _stub_page_dependencies(monkeypatch, clicked_keys: frozenset = frozenset()) -> dict:
    trackers = {
        "header_kwargs": {},
        "load_calls": [],
        "decision_calls": [],
        "subheaders": [],
        "expanders": [],
        "infos": [],
        "buttons": [],
        "selectboxes": [],
        "multiselects": [],
        "columns": [],
        "widget_events": [],
        "matrix_board_calls": [],
    }
    active_config = SimpleNamespace(
        data_source=SimpleNamespace(product_code="M626"),
    )

    # UI 优化轮次结构探针：记录模块标题 / Expander / 残留的 st.info / 按钮与
    # 筛选 widget（key），用于断言筛选条常驻与渲染次数
    monkeypatch.setattr(
        "streamlit.subheader",
        lambda label, *args, **kwargs: trackers["subheaders"].append(label),
    )

    def _fake_expander(label, *args, **kwargs):
        trackers["expanders"].append({"label": label, **kwargs})
        return nullcontext()

    monkeypatch.setattr("streamlit.expander", _fake_expander)
    monkeypatch.setattr(
        "streamlit.info",
        lambda *args, **kwargs: trackers["infos"].append(args[0] if args else ""),
    )

    real_button = st.button

    def _fake_button(*args, **kwargs):
        key = kwargs.get("key")
        trackers["buttons"].append(
            {"label": args[0] if args else kwargs.get("label"), "key": key}
        )
        trackers["widget_events"].append(("button", key))
        if key in clicked_keys:
            # 模拟 Streamlit 真实行为：点击时先执行 on_click 回调再返回 True
            on_click = kwargs.get("on_click")
            if callable(on_click):
                on_click(*kwargs.get("args", ()))
            return True
        return real_button(*args, **kwargs)

    monkeypatch.setattr("streamlit.button", _fake_button)

    real_selectbox = st.selectbox

    def _fake_selectbox(*args, **kwargs):
        trackers["selectboxes"].append(kwargs.get("key"))
        trackers["widget_events"].append(("selectbox", kwargs.get("key")))
        return real_selectbox(*args, **kwargs)

    monkeypatch.setattr("streamlit.selectbox", _fake_selectbox)

    real_multiselect = st.multiselect

    def _fake_multiselect(*args, **kwargs):
        trackers["multiselects"].append(kwargs.get("key"))
        trackers["widget_events"].append(("multiselect", kwargs.get("key")))
        return real_multiselect(*args, **kwargs)

    monkeypatch.setattr("streamlit.multiselect", _fake_multiselect)

    real_columns = st.columns

    def _fake_columns(spec, *args, **kwargs):
        trackers["columns"].append({"spec": spec, **kwargs})
        return real_columns(spec, *args, **kwargs)

    monkeypatch.setattr("streamlit.columns", _fake_columns)

    # 矩阵本体加载替换为记录器，避免真实 payload 计算；筛选条常驻由页面渲染
    monkeypatch.setattr(
        alert_matrix,
        "render_alert_matrix_board",
        lambda **kwargs: trackers["matrix_board_calls"].append(kwargs),
    )

    monkeypatch.setattr(AppSetup, "initialize_app", staticmethod(lambda: None))
    monkeypatch.setattr(
        SessionManager,
        "get_active_config",
        staticmethod(lambda: active_config),
    )
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: object())
    monkeypatch.setattr(step_labels, "get_cached_step_description_map", lambda db: {})
    monkeypatch.setattr(
        MonitorAnalysisService,
        "get_time_window",
        staticmethod(lambda: (datetime(2026, 6, 1), datetime(2026, 9, 2))),
    )
    monkeypatch.setattr(
        MonitorAnalysisService,
        "safe_refresh_snapshots",
        staticmethod(lambda *args, **kwargs: True),
    )

    def _fake_dashboard_data(**kwargs):
        trackers["load_calls"].append(kwargs)
        return SimpleNamespace(
            detail_df=pd.DataFrame(),
            global_summary_df=pd.DataFrame(),
            station_detail_df=pd.DataFrame(),
        )

    monkeypatch.setattr(
        MonitorAnalysisService, "get_monitor_dashboard_data", _fake_dashboard_data
    )
    monkeypatch.setattr(
        decision_signature,
        "get_scope_decision_signature",
        lambda scope, prod: trackers["decision_calls"].append((scope, prod)) or "sig",
    )
    monkeypatch.setattr(
        compliance_manager, "get_compliance_file_signature", lambda: "compliance-sig"
    )
    monkeypatch.setattr(
        page_header,
        "render_page_header",
        lambda **kwargs: trackers["header_kwargs"].update(kwargs),
    )
    # 门控放行后的渲染积木替换为 no-op，本测试只关心门控与加载时机
    monkeypatch.setattr(monitor_dashboard, "render_monitor_summary_chart", lambda *a, **k: None)
    monkeypatch.setattr(monitor_dashboard, "render_station_top10_section", lambda *a, **k: None)
    monkeypatch.setattr(monitor_dashboard, "render_alarm_detail_tables", lambda *a, **k: None)
    return trackers


def _run_page() -> None:
    runpy.run_path(str(PAGE_PATH), run_name="__main__")


def _assert_module_structure(trackers: dict) -> None:
    """模块化结构：两个模块各有 subheader 标题 + 默认展开的 expander，且无 st.info。"""
    assert trackers["infos"] == []
    assert trackers["subheaders"] == ["🚦 预警矩阵", "⚠️ 超规片自动预警"]
    module_expanders = [
        item for item in trackers["expanders"] if item.get("expanded") is True
    ]
    assert len(module_expanders) == 2
    # subheader 与 expander 文案不重复堆砌
    for item, title in zip(module_expanders, trackers["subheaders"]):
        assert item["label"] != title


def _assert_row_layout(trackers: dict, matrix_button_key: str) -> None:
    """筛选与操作按钮同行（Phase 11）：两处 4 列布局、底对齐、按钮最右。"""
    four_col_rows = [
        c
        for c in trackers["columns"]
        if isinstance(c["spec"], (list, tuple)) and len(c["spec"]) == 4
    ]
    assert len(four_col_rows) == 2, trackers["columns"]
    assert all(c.get("vertical_alignment") == "bottom" for c in four_col_rows)

    events = trackers["widget_events"]

    def _assert_consecutive(expected: list) -> None:
        for start in range(len(events) - len(expected) + 1):
            if events[start : start + len(expected)] == expected:
                return
        raise AssertionError(f"未找到连续同行 widget 序列 {expected}，实际: {events}")

    # 模块一：监控类型/产品型号/厂别 + 矩阵操作按钮（最右）
    _assert_consecutive(
        [
            ("selectbox", "alert_matrix_data_type"),
            ("multiselect", "alert_matrix_products"),
            ("multiselect", "alert_matrix_factories"),
            ("button", matrix_button_key),
        ]
    )
    # 模块二：监控类型/产品型号/厂别 + 查询按钮（最右）
    _assert_consecutive(
        [
            ("selectbox", "spc_data_type_filter"),
            ("multiselect", "monitor_products"),
            ("multiselect", "monitor_factories"),
            ("button", "btn_monitor_query_submit"),
        ]
    )


def _matrix_filter_keys(trackers: dict) -> list:
    """页面实际渲染的 alert_matrix_ 筛选 widget key（按渲染顺序）。"""
    return [
        key
        for key in trackers["selectboxes"] + trackers["multiselects"]
        if key in MATRIX_FILTER_KEYS
    ]


def test_page_hides_header_product_filter_and_gates_data_loading(monkeypatch) -> None:
    trackers = _stub_page_dependencies(monkeypatch)
    st.session_state.pop("monitor_query_signature", None)
    st.session_state.pop("alert_matrix_board_loaded", None)

    _run_page()

    assert trackers["header_kwargs"]["show_product_filter"] is False
    _assert_module_structure(trackers)
    # 未点击「查询」：签名预算与数据加载都不执行
    assert trackers["load_calls"] == []
    assert trackers["decision_calls"] == []
    # 筛选条常驻：未加载时也渲染（且只渲染一处），与加载按钮同处一行
    assert _matrix_filter_keys(trackers) == list(MATRIX_FILTER_KEYS)
    assert trackers["matrix_board_calls"] == []
    button_keys = [b["key"] for b in trackers["buttons"]]
    assert "btn_load_alert_matrix" in button_keys
    assert "btn_collapse_alert_matrix" not in button_keys
    _assert_row_layout(trackers, "btn_load_alert_matrix")


def test_page_loads_data_after_query_submitted(monkeypatch) -> None:
    trackers = _stub_page_dependencies(monkeypatch, clicked_keys={QUERY_BUTTON_KEY})
    st.session_state.pop("monitor_query_signature", None)
    st.session_state.pop("alert_matrix_board_loaded", None)

    _run_page()

    assert trackers["header_kwargs"]["show_product_filter"] is False
    _assert_module_structure(trackers)
    # 点击「查询」：先签名预算（7 产品 × spc/ctq 两 scope），再全量数据加载
    assert len(trackers["decision_calls"]) == len(SessionManager.AVAILABLE_PRODUCTS) * 2
    assert len(trackers["load_calls"]) == 1
    assert trackers["load_calls"][0]["data_type_filter"] == "ALL"


def test_page_renders_filter_bar_once_and_passes_selection_when_matrix_loaded(
    monkeypatch,
) -> None:
    """矩阵已加载：筛选条仍只由页面渲染一处，选择经 filter_selection 透传给矩阵。"""
    trackers = _stub_page_dependencies(monkeypatch)
    st.session_state.pop("monitor_query_signature", None)
    st.session_state["alert_matrix_board_loaded"] = True

    _run_page()

    assert trackers["header_kwargs"]["show_product_filter"] is False
    _assert_module_structure(trackers)
    # 筛选条只渲染一处（无 widget key 重复）
    assert _matrix_filter_keys(trackers) == list(MATRIX_FILTER_KEYS)
    # 矩阵本体被调用，且拿到当前筛选选择（监控类型/产品/厂别三元组）
    assert len(trackers["matrix_board_calls"]) == 1
    selection = trackers["matrix_board_calls"][0]["filter_selection"]
    assert selection is not None
    monitor_type, selected_products, selected_factories = selection
    assert monitor_type == "ALL"
    assert list(selected_products) == list(SessionManager.AVAILABLE_PRODUCTS)
    assert list(selected_factories) == ["ARRAY", "OLED", "TP"]
    # 已加载分支渲染「收起」而非「加载」按钮（同在筛选行最右列）
    button_keys = [b["key"] for b in trackers["buttons"]]
    assert "btn_collapse_alert_matrix" in button_keys
    assert "btn_load_alert_matrix" not in button_keys
    _assert_row_layout(trackers, "btn_collapse_alert_matrix")
