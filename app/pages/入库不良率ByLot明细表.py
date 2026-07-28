# src/vivo_project/app/pages/入库不良率ByLot明细表.py
import streamlit as st

# --- 1. 初始化与配置 ---
from app.utils.session_manager import SessionManager
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

from yield_domain.application.yield_service import YieldAnalysisService
from app.components.page_header import (
    build_product_cache_signature,
    extract_cached_funcs,
    render_page_header,
)

# 引入区块渲染组件
from app.sections.table_details import (
    render_lot_group_summary_section,
    render_lot_code_details_section,
    render_lot_top20_section
)

# --- 2. UI 界面布局 ---
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

# [关键修复]：必须先拿到 Config，再获取 product_dir
active_config = SessionManager.get_active_config()
product_dir = SessionManager.get_product_dir()

YIELD_LOT_DETAIL_CACHE_SIGNATURE = "yield_lot_detail_manual_refresh_v1"
product_cache_signature = build_product_cache_signature(
    YIELD_LOT_DETAIL_CACHE_SIGNATURE,
    active_config.data_source.product_code,
)

db_manager = DatabaseManager()

render_page_header(
    "📋 入库不良率ByLot明细表",
    active_config,
    cached_funcs=extract_cached_funcs(YieldAnalysisService),
    product_cache_scope=active_config.data_source.product_code,
    refresh_handlers=[
        lambda: YieldAnalysisService.safe_refresh_snapshots(
            db_manager,
            active_config,
        )
    ],
)

# --- 3. 加载数据 ---
# [核心修复] 依赖注入 db_manager + 快照签名感知缓存
all_data = YieldAnalysisService.get_lot_defect_rates(
    config=active_config,
    product_dir=product_dir,
    _db_manager=db_manager,
    snapshot_signature=product_cache_signature
)

# --- 4. 页面积木式调度 ---
if all_data:
    # 模块 1：渲染总表并获得在时间/选项范围内的有效 Lot ID 集合
    valid_lot_ids = render_lot_group_summary_section(all_data)
    
    if valid_lot_ids:
        # 模块 2：手动查询指定 Lot 的 Code 明细
        render_lot_code_details_section(all_data, valid_lot_ids)
        
        # 模块 3：选择 Code，反查 Top 20 严重 Lot
        render_lot_top20_section(all_data, valid_lot_ids)
else:
    st.error("未能从后台加载Lot数据，请检查后台日志或刷新重试。")
