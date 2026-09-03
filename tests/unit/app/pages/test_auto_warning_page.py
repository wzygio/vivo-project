"""自动预警看板页面组合层测试（2026-09-03 需求轮次）。

- 页头不渲染产品筛选（show_product_filter=False，全产品视图）；
- 「超规片自动预警」区查询门控：未点击「查询」不执行签名预算与数据加载，
  点击后才执行（monkeypatch 计数）；
- 模块化结构：每个模块 = st.subheader 标题 + st.expander（默认展开）；
- 渲染面无 st.info 提醒条（UI 优化轮次禁令）。

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
from app.sections.inline_domain.monitor import monitor_dashboard
from app.utils import step_labels
from app.utils.app_setup import AppSetup
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.shared import decision_signature
from src.shared_kernel.infrastructure import db_handler

PAGE_PATH = Path(__file__).parents[4] / "app" / "pages" / "自动预警看板.py"
QUERY_BUTTON_KEY = "btn_monitor_query_submit"


def _stub_page_dependencies(monkeypatch) -> dict:
    trackers = {
        "header_kwargs": {},
        "load_calls": [],
        "decision_calls": [],
        "subheaders": [],
        "expanders": [],
        "infos": [],
    }
    active_config = SimpleNamespace(
        data_source=SimpleNamespace(product_code="M626"),
    )

    # UI 优化轮次结构探针：记录模块标题 / Expander / 残留的 st.info
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


def test_page_loads_data_after_query_submitted(monkeypatch) -> None:
    trackers = _stub_page_dependencies(monkeypatch)
    st.session_state.pop("monitor_query_signature", None)
    st.session_state.pop("alert_matrix_board_loaded", None)
    real_button = st.button

    def _fake_button(*args, **kwargs):
        if kwargs.get("key") == QUERY_BUTTON_KEY:
            # 模拟 Streamlit 真实行为：点击时先执行 on_click 回调再返回 True
            on_click = kwargs.get("on_click")
            if callable(on_click):
                on_click(*kwargs.get("args", ()))
            return True
        return real_button(*args, **kwargs)

    monkeypatch.setattr("streamlit.button", _fake_button)

    _run_page()

    assert trackers["header_kwargs"]["show_product_filter"] is False
    _assert_module_structure(trackers)
    # 点击「查询」：先签名预算（7 产品 × spc/ctq 两 scope），再全量数据加载
    assert len(trackers["decision_calls"]) == len(SessionManager.AVAILABLE_PRODUCTS) * 2
    assert len(trackers["load_calls"]) == 1
    assert trackers["load_calls"][0]["data_type_filter"] == "ALL"
