# src/vivo_project/app/pages/关键备件报表.py
"""
📋 关键备件报表 — Streamlit 前端页面

数据流:
1. 加载 resources/critical_parts_baseline.csv（规格基线）
2. 查询 PostgreSQL eda.ARRAY_PDS_RESULT_T（最新实测值）→ Parquet 快照
3. 自动匹配备件类型并计算使用进度、预警状态（超规/预警/正常）
4. 渲染卡片：总备件数、超规、预警、正常、最后更新
5. 中部渲染明细表，支持多维度厂别筛选
6. 点击联动：点击表格任一备件行，底部展开该备件 30 天趋势曲线
"""

import sys
from pathlib import Path

# ==============================================================================
# [关键] 动态锚定项目根目录与 sys.path 注入
# ==============================================================================
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

from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.equipment_domain.application.parts_service import PartsReportService
from app.components.page_header import extract_cached_funcs, render_page_header
from app.utils.session_manager import SessionManager
from app.sections.parts_dashboard import (
    render_factory_filter,
    render_parts_metrics,
    render_parts_table_selectable,
)

# ==============================================================================
#  配置常量
# ==============================================================================

BASELINE_PATH = Path("resources/critical_parts_baseline.csv")
PARTS_REPORT_CACHE_SIGNATURE = "parts_report_manual_refresh_v1"


# ==============================================================================
#  页面初始化
# ==============================================================================

st.set_page_config(
    page_title="关键备件报表",
    layout="wide",
    initial_sidebar_state="collapsed",
)

active_config = SessionManager.get_active_config()
db_manager = DatabaseManager()
render_page_header(
    "📋 关键备件报表",
    active_config,
    cached_funcs=extract_cached_funcs(PartsReportService),
    refresh_handlers=[
        lambda: PartsReportService.safe_refresh_snapshots(
            db_manager,
            str(BASELINE_PATH),
        )
    ],
)


# ==============================================================================
#  获取厂别、膜层、备件类型列表（用于筛选器与联动）
# ==============================================================================

try:
    spec_df = pd.read_csv(BASELINE_PATH, encoding="utf-8-sig")
    available_factories = sorted(spec_df["厂别"].dropna().unique().tolist())
    available_layers = sorted(spec_df["膜层"].dropna().unique().tolist())
    available_part_types = sorted(spec_df["备件类型"].dropna().unique().tolist())
except Exception:
    available_factories = []
    available_layers = []
    available_part_types = []

selected_factory = render_factory_filter(available_factories)


# ==============================================================================
#  加载原始报表数据
# ==============================================================================

with st.spinner("正在从数据库加载备件寿命数据..."):
    try:
        view_model = PartsReportService.get_report_data(
            _db_manager=db_manager,
            baseline_path=str(BASELINE_PATH),
            snapshot_signature=PARTS_REPORT_CACHE_SIGNATURE,
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

render_parts_metrics(
    total_count=len(filtered_df) if not filtered_df.empty else 0,
    over_count=int((filtered_df["预警状态"] == "超规").sum()) if not filtered_df.empty else 0,
    warning_count=int((filtered_df["预警状态"] == "预警").sum()) if not filtered_df.empty else 0,
    normal_count=int((filtered_df["预警状态"] == "正常").sum()) if not filtered_df.empty else 0,
    last_update=view_model.last_update,
)


# ==============================================================================
#  渲染交互式数据表
# ==============================================================================

st.markdown("### 📋 备件寿命明细列表")
selected_rows_dict = render_parts_table_selectable(filtered_df)


# ==============================================================================
#  [点击联动] 表格与趋势图点击联动
# ==============================================================================

selected_row_data = None
if not filtered_df.empty:
    rows_list = selected_rows_dict.get("selection", {}).get("rows", [])
    if rows_list:
        selected_row_data = filtered_df.iloc[rows_list[0]]
    else:
        selected_row_data = filtered_df.iloc[0]

if selected_row_data is not None:
    trend_factory = selected_row_data["厂别"]
    trend_layer = selected_row_data["膜层"]
    trend_part_type = selected_row_data["备件类型"]

    st.markdown("---")
    with st.expander(
        f"📈 备件寿命趋势分析 — {trend_factory} | {trend_layer} | {trend_part_type}",
        expanded=True,
    ):
        from app.charts.parts_chart import generate_trend_data, create_parts_trend_chart

        df_selected_trend = generate_trend_data(
            factory=trend_factory,
            layer=trend_layer,
            part_type=trend_part_type,
            spec_df=spec_df,
            station=selected_row_data.get("站点", ""),
            machine=selected_row_data.get("机台号-腔室", ""),
            days=90,
        )

        fig = create_parts_trend_chart(
            df_trend=df_selected_trend,
            factory=trend_factory,
            layer=trend_layer,
            part_type=trend_part_type,
        )

        st.plotly_chart(fig, use_container_width=True)
