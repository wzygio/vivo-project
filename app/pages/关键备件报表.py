# src/vivo_project/app/pages/关键备件报表.py
"""
📋 关键备件报表 — Streamlit 前端页面

数据流:
1. 加载 resources/critical_parts_baseline.csv（规格基线）
2. 查询 PostgreSQL eda.ARRAY_PDS_RESULT_T（最新实测值）
3. 基于机台+腔室+备件类型进行子串匹配
4. 计算使用进度百分比，渲染带进度条的数据表

功能:
- 🔄 数据刷新按钮（清除 L2 缓存）
- 🔽 厂别筛选下拉框
- 📊 概览统计卡片（总备件数、超预警、正常、最后更新）
- 📈 使用进度 ProgressColumn 进度条
"""

import sys
from pathlib import Path

# ==============================================================================
# [关键] 动态锚定项目根目录与 sys.path 注入
# ==============================================================================
# 必须放在所有其他 import 之前，确保包解析正确
current_dir = Path(__file__).resolve().parent
project_root = None
for parent in [current_dir] + list(current_dir.parents):
    if (parent / "pyproject.toml").exists():
        project_root = parent
        break
if project_root:
    root_str = str(project_root)
    src_str = str(project_root / "src")
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)

import streamlit as st
import pandas as pd
from pathlib import Path

from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.equipment_domain.application.parts_service import PartsReportService
from app.components.parts_sections import (
    render_parts_header,
    render_parts_refresh_button,
    render_factory_filter,
    render_parts_metrics,
    render_parts_table,
)

# ==============================================================================
#  配置常量
# ==============================================================================

BASELINE_PATH = Path("resources/critical_parts_baseline.csv")


# ==============================================================================
#  页面初始化
# ==============================================================================

st.set_page_config(
    page_title="关键备件报表",
    layout="wide",
    initial_sidebar_state="collapsed",
)

render_parts_header("📋 关键备件报表")


# ==============================================================================
#  数据刷新控制
# ==============================================================================

if render_parts_refresh_button():
    st.cache_data.clear()
    st.rerun()


# ==============================================================================
#  获取厂别列表（用于筛选器）
# ==============================================================================

try:
    spec_df = pd.read_csv(BASELINE_PATH, encoding="utf-8-sig")
    available_factories = sorted(spec_df["厂别"].dropna().unique().tolist())
except Exception:
    available_factories = []

selected_factory = render_factory_filter(available_factories)


# ==============================================================================
#  加载报表数据
# ==============================================================================

# 快照签名：CSV 文件修改时间 → 用于 L2 缓存失效
sig_key = "parts_baseline_sig"
if sig_key not in st.session_state:
    st.session_state[sig_key] = str(BASELINE_PATH.stat().st_mtime)
snapshot_sig = st.session_state[sig_key]

db_manager = DatabaseManager()

with st.spinner("正在从数据库加载备件寿命数据..."):
    try:
        view_model = PartsReportService.get_report_data(
            _db_manager=db_manager,
            baseline_path=str(BASELINE_PATH),
            snapshot_signature=snapshot_sig,
        )
    except Exception as e:
        st.error(f"❌ 数据加载失败: {e}")
        st.stop()


# ==============================================================================
#  厂别过滤
# ==============================================================================

if selected_factory and not view_model.report_df.empty:
    filtered_df = view_model.report_df[
        view_model.report_df["厂别"] == selected_factory
    ].copy()
else:
    filtered_df = view_model.report_df.copy()


# ==============================================================================
#  渲染概览统计
# ==============================================================================

total = len(filtered_df)
warning = int((filtered_df["预警状态"] == "⚠️ 超预警").sum()) if not filtered_df.empty else 0
normal = total - warning

render_parts_metrics(
    total_count=total,
    warning_count=warning,
    normal_count=normal,
    last_update=view_model.last_update,
)


# ==============================================================================
#  渲染数据表
# ==============================================================================

render_parts_table(filtered_df)
