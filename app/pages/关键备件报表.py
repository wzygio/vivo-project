# src/vivo_project/app/pages/关键备件报表.py
"""
📋 关键备件报表 — Streamlit 前端页面

数据流:
1. 加载关键备件规格基线及 CVD 工作簿首个 sheet
2. 查询 PostgreSQL eda.ARRAY_PDS_RESULT_T（最新实测值）→ Parquet 快照
3. 自动匹配备件类型并计算使用进度、预警状态（超规/预警/正常）
4. 渲染卡片：总备件数、超规、预警、正常、最后更新
5. 中部渲染明细表，支持厂别、设备类型、备件类型多选筛选
"""

import logging
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
from src.equipment_domain.application.parts_service import (
    PartsReportService,
    build_parts_report_cache_context,
)
from app.components.page_header import (
    extract_cached_funcs,
    render_page_header,
)
from app.manager.session_manager import SessionManager
from app.sections.equipment_domain.parts_filters import (
    apply_parts_filters,
    render_parts_filters,
)
from app.sections.equipment_domain.parts_dashboard import (
    render_parts_metrics,
    render_parts_table,
)

# ==============================================================================
#  配置常量
# ==============================================================================

BASELINE_PATH = ConfigLoader.get_domain_resource_path("equipment_domain", "critical_parts_baseline", "critical_parts_baseline.csv")
CVD_PATH = ConfigLoader.get_domain_resource_path("equipment_domain", "cvd_parts", "供应商关键备件寿命管控清单-0921-CVD.xlsx")
PARTS_REPORT_CACHE_SIGNATURE = "parts_report_cvd_v3"
logger = logging.getLogger(__name__)


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
parts_report_cache_signature = PARTS_REPORT_CACHE_SIGNATURE
parts_report_cache_context = build_parts_report_cache_context(BASELINE_PATH, CVD_PATH)
render_page_header(
    "📋 关键备件报表",
    active_config,
    show_product_filter=False,
    cached_funcs=extract_cached_funcs(PartsReportService),
    refresh_handlers=[
        lambda: PartsReportService.safe_refresh_snapshots(
            db_manager,
            str(BASELINE_PATH),
        )
    ],
)


# ==============================================================================
#  加载原始报表数据
# ==============================================================================

with st.spinner("正在加载备件寿命数据..."):
    try:
        view_model = PartsReportService.get_report_data(
            _db_manager=db_manager,
            baseline_path=str(BASELINE_PATH),
            snapshot_signature=parts_report_cache_signature,
            as_of_date=parts_report_cache_context["as_of_date"],
            baseline_signature=parts_report_cache_context["baseline_signature"],
            runtime_config_signature=parts_report_cache_context["runtime_config_signature"],
            cvd_path=str(CVD_PATH),
            cvd_signature=parts_report_cache_context["cvd_signature"],
        )
    except Exception:
        logger.exception("关键备件报表加载失败")
        st.error("备件寿命数据暂时无法加载，请稍后重试。")
        st.stop()


# ==============================================================================
#  多维筛选
# ==============================================================================

selected_factories, selected_equipment_types, selected_part_types = (
    render_parts_filters(view_model.report_df)
)

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
render_parts_table(filtered_df)
