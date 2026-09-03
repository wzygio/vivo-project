"""超规片自动预警「查询」门控的隔离 Streamlit harness（AppTest 用，不触 DB/文件）。

- 渲染真实 ``render_monitor_control_panel`` + ``render_monitor_query_gate``；
- 门控放行时用 ``st.session_state["gate_load_count"]`` 模拟数据加载计数，
  并输出 ``GATE_OPEN_DATA_LOADED`` 标记供断言。
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

project_root = Path(__file__).resolve().parents[6]
for import_path in (project_root, project_root / "src"):
    path_text = str(import_path)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)

from app.sections.inline_domain.monitor.monitor_dashboard import (
    render_monitor_control_panel,
    render_monitor_query_gate,
)

st.set_page_config(page_title="monitor-query-gate-fixture", layout="wide")

filter_state = render_monitor_control_panel(["M678", "Z571"], ["ARRAY", "OLED", "TP"])
if render_monitor_query_gate(filter_state):
    st.session_state["gate_load_count"] = st.session_state.get("gate_load_count", 0) + 1
    st.markdown("GATE_OPEN_DATA_LOADED")
