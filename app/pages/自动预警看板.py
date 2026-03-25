import streamlit as st
import pandas as pd
from datetime import datetime

# ==============================================================================
#  配置与初始化
# ==============================================================================
from app.utils.session_manager import SessionManager
from src.shared_kernel.config import ConfigLoader
from app.components.components import (
    render_page_header, 
    extract_cached_funcs,
    setup_hot_reload
)
from app.components.spc_sections import (
    render_spc_control_panel,
    render_spc_summary_section,
    render_spc_detail_section
)

# --- 2. 引入真实的 SPC 后端 Service 与数据模型 ---
from src.spc_domain.application.spc_service import SpcAnalysisService
from src.spc_domain.infrastructure.data_loader import SpcQueryConfig
from src.yield_domain.infrastructure.db_handler import DatabaseManager

st.set_page_config(page_title="自动预警看板", layout="wide", initial_sidebar_state="collapsed")
setup_hot_reload(enable=True)

st.title("自动预警看板")
with st.expander("数据刷新"):
    # [Refactor] 3. 渲染页头 (动态注入 auto_cached_funcs)
    active_config = SessionManager.get_active_config()
    funcs_to_clear = extract_cached_funcs(SpcAnalysisService)
    render_page_header(
        config=active_config,
        cached_funcs=funcs_to_clear
    )

# ==============================================================================
#  数据加载
# ==============================================================================
with st.spinner("正在全量抽取 SPC 数据 (全产品自动扫描中)..."):
    try:
        # A. 实例化数据库管理器
        db_manager = DatabaseManager() 
        
        # C. 获取后端统一定义的时间窗口
        start_dt, end_dt = SpcAnalysisService.get_time_window()
        
        # D. 构造 Pydantic 查询配置
        # 传入 "ALL" 将触发后端的全产品目录自动探测逻辑
        query_config = SpcQueryConfig(
            prod_code="ALL", 
            start_date=start_dt.strftime("%Y-%m-%d"),
            end_date=end_dt.strftime("%Y-%m-%d")
        )
        
        # E. 发起真实的服务层调用 (严格对齐后端 3 参数签名)
        # 删除了 snapshot_dir_str 参数，由后端 Service 内部自理
        view_model = SpcAnalysisService.get_spc_dashboard_data(
            _db_manager=db_manager,
            query_config_json=query_config.model_dump_json(),
            time_type='MIXED'
        )
    except Exception as e:
        # 如果依然报错，此处会打印出最真实的错误堆栈
        st.error(f"❌ 调用后端 SPC Service 失败: {str(e)}")
        st.stop()
        
# --------------------------------------------------------------------------
# 页面积木组装层 (UI Assembly)
# --------------------------------------------------------------------------
# 3. 提取所有可用的维度供前端过滤使用 (防呆处理：如果为空则返回空列表)
available_products = view_model.detail_df['prod_code'].unique().tolist() if not view_model.detail_df.empty else []
available_factories = view_model.detail_df['factory'].unique().tolist() if not view_model.detail_df.empty else []

# 4. 组装积木: 渲染控制台
filter_state = render_spc_control_panel(available_products, available_factories)

st.divider()

# 5. 组装积木: 渲染全局汇总图表 (传入全球聚合大盘)
render_spc_summary_section(view_model.global_summary_df)

# 6. 组装积木: 渲染明细透视表 (传入多维下钻明细)
render_spc_detail_section(view_model.detail_df, filter_state)