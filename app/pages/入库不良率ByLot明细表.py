from app.sections.yield_domain.data_health import render_yield_data_health
# src/vivo_project/app/pages/入库不良率ByLot明细表.py
import streamlit as st

# --- 1. 初始化与配置 ---
from app.manager.session_manager import SessionManager
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

from yield_domain.application.yield_service import (
    YieldAnalysisService,
    YieldDataLoadError,
    YieldDataModificationError,
    YieldWarningLinesReadError,
)
from src.yield_domain.infrastructure.rate_override_repository import RateOverrideReadError
from app.components.indicator_cache import (
    build_indicator_product_cache_signature,
)

PRODUCT_CACHE_INDICATORS = ("yield_lot_oos",)

from app.components.page_header import (
    extract_cached_funcs,
    render_page_header,
)

# 引入区块渲染组件
from app.sections.yield_domain.table_details import (
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
product_cache_signature = build_indicator_product_cache_signature(
    YIELD_LOT_DETAIL_CACHE_SIGNATURE,
    active_config.data_source.product_code,
    PRODUCT_CACHE_INDICATORS,
)

db_manager = DatabaseManager()
yield_cache_context = YieldAnalysisService.build_cache_context(active_config, product_dir)

render_page_header(
    "📋 入库不良率ByLot明细表",
    active_config,
    cached_funcs=extract_cached_funcs(YieldAnalysisService),
    product_cache_scope=active_config.data_source.product_code,
    product_cache_indicators=PRODUCT_CACHE_INDICATORS,
    refresh_handlers=[
        lambda: YieldAnalysisService.safe_refresh_snapshots(
            db_manager,
            active_config,
        )
    ],
)

# --- 3. 加载数据 ---
# [核心修复] 依赖注入 db_manager + 快照签名感知缓存
health = YieldAnalysisService.get_data_health(
    active_config,
    _db_manager=db_manager,
    snapshot_signature=product_cache_signature,
    analysis_start_date=yield_cache_context["analysis_start_date"],
    analysis_end_date=yield_cache_context["analysis_end_date"],
)
render_yield_data_health(health)

try:
    all_data = YieldAnalysisService.get_lot_defect_rates(
        config=active_config,
        product_dir=product_dir,
        _db_manager=db_manager,
        snapshot_signature=product_cache_signature,
        **yield_cache_context,
    )
except (
    YieldDataLoadError,
    YieldDataModificationError,
    YieldWarningLinesReadError,
    RateOverrideReadError,
):
    st.error("Yield Lot 报表配置或资源读取失败，请检查相关文件后刷新重试。")
    st.stop()

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
    st.info("当前查询窗口没有可供展示的 Lot 数据。")
