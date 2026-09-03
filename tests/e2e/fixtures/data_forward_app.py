"""Isolated Streamlit harness for report data-forward browser verification."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

project_root = Path(__file__).resolve().parents[3]
for import_path in (project_root, project_root / "src"):
    path_text = str(import_path)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)

from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.snapshot_window import snapshot_window_start


st.set_page_config(page_title="报表数据日期前推 E2E", layout="wide")
st.title("报表数据日期前推验证")

enabled = st.toggle("启用日期前推", value=True)
policy = DataForwardPolicy(enabled=enabled, offset_days=4)
st.metric("当前模式", "开启" if enabled else "关闭")

snapshot_tab, query_tab = st.tabs(("快照型页面", "直接查询型页面"))

with snapshot_tab:
    source_frame = pd.DataFrame({"start_time": [pd.Timestamp("2026-08-29 08:30:00")]})
    display_frame = policy.shift_frame(source_frame, ("start_time",))
    st.write("源时间", source_frame.loc[0, "start_time"].strftime("%Y-%m-%d %H:%M:%S"))
    st.write("页面显示时间", display_frame.loc[0, "start_time"].strftime("%Y-%m-%d %H:%M:%S"))
    st.write("快照读取起点", snapshot_window_start("2026-09-02").strftime("%Y-%m-%d"))

with query_tab:
    display_start = pd.Timestamp("2026-09-02 00:00:00")
    display_end = pd.Timestamp("2026-09-02 23:59:59")
    source_start, source_end = policy.to_source_window(display_start, display_end)
    st.write("页面查询开始", display_start.strftime("%Y-%m-%d %H:%M:%S"))
    st.write("数据库查询开始", source_start.strftime("%Y-%m-%d %H:%M:%S"))
    result = policy.shift_frame(pd.DataFrame({"print_time": [source_start]}), ("print_time",))
    st.write("查询结果显示时间", result.loc[0, "print_time"].strftime("%Y-%m-%d %H:%M:%S"))
    st.caption(f"数据库查询结束：{source_end:%Y-%m-%d %H:%M:%S}")
