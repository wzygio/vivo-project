# src/vivo_project/app/pages/关键备件报表.py
"""
📋 关键备件报表 — Streamlit 前端页面

数据流:
1. 加载 resources/equipment_domain/critical_parts_baseline.csv（规格基线）
2. 查询 PostgreSQL eda.ARRAY_PDS_RESULT_T（最新实测值）→ Parquet 快照
3. 自动匹配备件类型并计算使用进度、预警状态（超规/预警/正常）
4. 渲染卡片：总备件数、超规、预警、正常、最后更新
5. 中部渲染明细表，支持厂别、设备类型、备件类型多选筛选
6. 点击联动：仅在勾选表格备件行后，底部展开该备件趋势曲线
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

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.equipment_domain.application.parts_service import PartsReportService
from src.equipment_domain.infrastructure.data_loader import load_spec_baseline
from app.components.page_header import (
    build_product_cache_signature,
    extract_cached_funcs,
    render_page_header,
)
from app.manager.session_manager import SessionManager
from app.sections.equipment_domain.parts_filters import (
    apply_parts_filters,
    get_selected_parts_row,
    render_parts_filters,
)
from app.sections.equipment_domain.parts_dashboard import (
    render_parts_metrics,
    render_parts_table_selectable,
)

# ==============================================================================
#  配置常量
# ==============================================================================

BASELINE_PATH = ConfigLoader.get_domain_resource_path("equipment_domain", "critical_parts_baseline", "critical_parts_baseline.csv")
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
parts_report_cache_signature = build_product_cache_signature(
    PARTS_REPORT_CACHE_SIGNATURE,
    active_config.data_source.product_code,
)
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
#  加载规格并渲染级联筛选器
# ==============================================================================

try:
    spec_df = load_spec_baseline(BASELINE_PATH)
except Exception as error:
    st.error(f"❌ 规格基线加载失败: {error}")
    st.stop()

selected_factories, selected_equipment_types, selected_part_types = (
    render_parts_filters(spec_df)
)


# ==============================================================================
#  加载原始报表数据
# ==============================================================================

with st.spinner("正在从数据库加载备件寿命数据..."):
    try:
        view_model = PartsReportService.get_report_data(
            _db_manager=db_manager,
            baseline_path=str(BASELINE_PATH),
            snapshot_signature=parts_report_cache_signature,
        )
    except Exception as e:
        st.error(f"❌ 数据加载失败: {e}")
        st.stop()


# ==============================================================================
#  多维筛选
# ==============================================================================

filtered_df = apply_parts_filters(
    view_model.report_df,
    selected_factories=selected_factories,
    selected_equipment_types=selected_equipment_types,
    selected_part_types=selected_part_types,
)


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

selected_row_data = get_selected_parts_row(filtered_df, selected_rows_dict)

if selected_row_data is not None:
    trend_factory = selected_row_data["厂别"]
    trend_layer = selected_row_data["膜层"]
    trend_part_type = selected_row_data["备件类型"]

    st.markdown("---")
    with st.expander(
        f"📈 备件寿命趋势分析 — {trend_factory} | {trend_layer} | {trend_part_type}",
        expanded=True,
    ):
        from app.charts.equipment_domain.parts_chart import generate_trend_data, create_parts_trend_chart

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

        st.plotly_chart(fig, width="stretch")
