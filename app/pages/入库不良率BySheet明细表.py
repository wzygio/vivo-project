# src/vivo_project/app/pages/入库不良率BySheet明细表.py
import streamlit as st

# --- 1. 初始化与配置 ---
from app.utils.session_manager import SessionManager
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

from yield_domain.application.yield_service import YieldAnalysisService
from yield_domain.application.dtos import YieldQueryConfig
from app.components.page_header import extract_cached_funcs, render_page_header

# 引入区块渲染组件
from app.sections.table_details import (
    render_sheet_group_summary_section,
    render_sheet_code_details_section,
    render_sheet_top20_section
)

# --- 2. UI 界面布局 ---
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

# [关键修复]：必须先拿到 Config，再获取 product_dir
active_config = SessionManager.get_active_config()
product_dir = SessionManager.get_product_dir()

YIELD_SHEET_DETAIL_CACHE_SIGNATURE = "yield_sheet_detail_manual_refresh_v1"

# L1 快照刷新参数：仅由页头“刷新数据”按钮触发。
start_dt, end_dt = YieldAnalysisService.get_time_window()
db_manager = DatabaseManager()
yield_query_config = YieldQueryConfig(
    product_code=active_config.data_source.product_code,
    start_date=start_dt.strftime("%Y-%m-%d"),
    end_date=end_dt.strftime("%Y-%m-%d"),
    work_order_types=getattr(active_config.data_source, "work_order_types", []),
    target_defect_groups=getattr(active_config.data_source, "target_defect_groups", []),
)

render_page_header(
    "📈 入库不良率BySheet明细表",
    active_config,
    cached_funcs=extract_cached_funcs(YieldAnalysisService),
    refresh_handlers=[
        lambda: YieldAnalysisService.safe_refresh_snapshots(
            db_manager,
            yield_query_config.model_dump_json(),
        )
    ],
)

# --- 3. 加载数据 ---
# [核心修复] 依赖注入 db_manager + 快照签名感知缓存
all_data = YieldAnalysisService.get_sheet_defect_rates(
    config=active_config, 
    product_dir=product_dir,
    _db_manager=db_manager,
    snapshot_signature=YIELD_SHEET_DETAIL_CACHE_SIGNATURE
)

# --- 4. 页面积木式调度 ---
if all_data:
    # 模块 1：渲染总表并获得在时间/选项范围内的有效 Sheet ID 集合
    valid_sheet_ids = render_sheet_group_summary_section(all_data)
    
    if valid_sheet_ids:
        # 模块 2：手动查询指定 Sheet 的 Code 明细
        render_sheet_code_details_section(all_data, valid_sheet_ids)
        
        # 模块 3：选择 Code，反查 Top 20 严重 Sheet
        render_sheet_top20_section(all_data, valid_sheet_ids)
else:
    st.error("未能从后台加载Sheet数据，请检查后台日志或刷新重试。")
