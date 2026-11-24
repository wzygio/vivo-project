from app.sections.yield_domain.data_health import render_yield_data_health
# src/vivo_project/app/pages/入库不良率ByLot明细表.py
import streamlit as st

# --- 1. 初始化与配置 ---
from src.shared_kernel.config import ConfigLoader
from app.sections.yield_domain.resource_config import refresh_yield_product
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
    render_lot_details_dashboard,
)

# --- 2. UI 界面布局 ---
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

current_product = render_page_header(
    "📋 入库不良率ByLot明细表",
    cached_funcs=extract_cached_funcs(YieldAnalysisService),
    product_cache_indicators=PRODUCT_CACHE_INDICATORS,
    product_refresh_handler=refresh_yield_product,
)

active_config = ConfigLoader.load_config(current_product)
product_dir = ConfigLoader.get_domain_resource_dir("yield_domain") / current_product

YIELD_LOT_DETAIL_CACHE_SIGNATURE = "yield_lot_detail_manual_refresh_v1"
product_cache_signature = build_indicator_product_cache_signature(
    YIELD_LOT_DETAIL_CACHE_SIGNATURE,
    active_config.data_source.product_code,
    PRODUCT_CACHE_INDICATORS,
)

db_manager = DatabaseManager()
yield_cache_context = YieldAnalysisService.build_cache_context(active_config, product_dir)

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
    render_lot_details_dashboard(all_data)
else:
    st.info("当前查询窗口没有可供展示的 Lot 数据。")
