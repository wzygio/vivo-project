# src/vivo_project/app/components/components.py
import pandas as pd
import streamlit as st
import logging, os
from pathlib import Path

# [Refactor] 引入配置模型
from vivo_project.config_model import AppConfig
from vivo_project.utils.session_manager import SessionManager

# [Refactor] 定义默认缓存时间，替代原 CONFIG['application']['cache_ttl_hours']
DEFAULT_CACHE_TTL = 4 * 60 * 60  # 4 Hours

@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def calculate_warning_lines(mwd_code_data):
    """计算所有Code的警戒线值并缓存结果"""
    if mwd_code_data is None:
        return {}
    
    monthly_data = mwd_code_data.get('monthly')
    if monthly_data is None or (isinstance(monthly_data, pd.DataFrame) and monthly_data.empty):
        return {}
    
    warning_lines = {}
    
    # 按Code分组计算警戒线
    for code in monthly_data['defect_desc'].unique():
        code_monthly = monthly_data[monthly_data['defect_desc'] == code]
        monthly_rates = code_monthly.groupby('time_period')['defect_rate'].sum()
        max_monthly_rate = monthly_rates.max()
        warning_lines[code] = max_monthly_rate * 1.35 if max_monthly_rate > 0 else None
    
    return warning_lines


def render_page_header(title: str, config: AppConfig):
    """
    [企业级 Header V2.0]
    集成：标题、产品切换上下文、数据刷新、缓存清理。
    布局：顶部标题 -> 下方控制栏 (Toolbar)
    """
    
    # --- 1. 渲染主标题 ---
    st.title(title)
    
    # --- 2. 准备逻辑与路径 ---
    processing_conf = config.processing
    snapshot_path_str = processing_conf.get('snapshot_path', 'data/panel_details_snapshot.parquet')
    snapshot_path = Path(snapshot_path_str).resolve()
    
    # 定义刷新回调 (仅刷新数据)
    def _refresh_data_callback():
        if snapshot_path.exists():
            try:
                os.remove(snapshot_path)
                logging.info(f"🗑️ [UI] 本地快照已删除: {snapshot_path}")
            except Exception as e:
                logging.error(f"❌ 删除快照失败: {e}")
        st.cache_data.clear()
        # 注意：这里不清除 Session State 中的配置，只清除数据缓存

    # 定义暴力清除回调 (清除所有)
    def _hard_reset_callback():
        st.cache_data.clear()
        st.cache_resource.clear()
        logging.warning("🧨 用户触发暴力缓存清除")

    # --- 3. 渲染控制栏 (Control Toolbar) ---
    # 使用灰色背景容器包裹，形成“工具栏”的视觉效果
    with st.container(border=True):
        # 布局：[产品选择 (2)] [空白占位 (4)] [刷新按钮 (1)] [清除缓存 (1)]
        # 这种比例可以把按钮挤到最右边，产品选择在最左边
        c_prod, c_space, c_refresh, c_clear = st.columns([2, 4, 1.2, 1.2])

        # A. 左侧：产品选择器 (全局上下文)
        with c_prod:
            current_prod = config.data_source.product_code
            available_prods = SessionManager.AVAILABLE_PRODUCTS
            
            # 使用 session state 里的 key 绑定，确保状态同步
            selected_prod = st.selectbox(
                "📦 当前产品型号",
                options=available_prods,
                index=available_prods.index(current_prod) if current_prod in available_prods else 0,
                key=f"header_prod_sel_{title}", # 唯一Key防止冲突
                label_visibility="collapsed" # 隐藏Label，更像工具栏
            )
            
            # 监听切换
            if selected_prod != current_prod:
                SessionManager.load_and_set_config(selected_prod)
                st.rerun()

        # B. 中间：显示当前产品状态 (可选，这里用作占位)
        with c_space:
             # 可以显示最后更新时间，或者单纯留白
             st.write("") 

        # C. 右侧：功能按钮区
        with c_refresh:
            st.button(
                "🔄 刷新数据",
                key=f"btn_refresh_{title}",
                on_click=_refresh_data_callback,
                use_container_width=True,
                help="删除本地快照并重新从数据库拉取数据 (10min)"
            )
            
        with c_clear:
            st.button(
                "🧹 清除缓存",
                key=f"btn_clear_{title}",
                on_click=_hard_reset_callback,
                use_container_width=True,
                help="清除所有内存缓存和资源缓存 (用于Debug配置不生效等问题)"
            )

def create_code_selection_ui(
    source_data: pd.DataFrame | dict,
    key_prefix: str,
    filter_by: str = 'rate',
    rate_threshold: float = 0.0005,
    count_threshold: int = 20
) -> dict:
    """
    (V3.5 - 数据驱动版)
    完全基于 source_data 动态生成筛选器，不再强依赖 target_defect_groups 配置。
    
    [Refactor Note] 此函数逻辑主要依赖传入的 DataFrame 数据，不直接读取全局 CONFIG，
    因此保持原样，仅增强类型提示兼容性。
    """

    # --- 1. 数据聚合 ---
    processed_df = None
    if isinstance(source_data, pd.DataFrame):
        processed_df = source_data.copy()
    elif isinstance(source_data, dict):
        all_dfs = [df for df in source_data.values() if isinstance(df, pd.DataFrame) and not df.empty]
        if all_dfs:
            processed_df = pd.concat(all_dfs, ignore_index=True)

    # --- 2. 动态识别活跃的 Group ---
    active_groups = []
    
    if processed_df is not None and not processed_df.empty:
        # 检查必要列
        if 'defect_group' in processed_df.columns and 'defect_desc' in processed_df.columns:
            # 从数据中提取存在的 Group，并排序
            raw_groups = processed_df['defect_group'].dropna().unique()
            active_groups = sorted([g for g in raw_groups if str(g).strip() != ""])
        else:
            st.error(f"UI组件错误({key_prefix}): 数据源缺少 'defect_group' 或 'defect_desc' 列。")
            return {"group": None, "code": None}

    if not active_groups:
        st.info("当前无有效的不良数据，无法进行 Code 筛选。")
        return {"group": None, "code": None}

    # --- 3. 筛选符合条件的 Code ---
    code_options_by_group = {}
    eligible_series = pd.Series(dtype=float)

    if processed_df is not None and not processed_df.empty:
        if filter_by == 'rate':
            if 'defect_rate' in processed_df.columns:
                metrics = processed_df.groupby(['defect_group', 'defect_desc'])['defect_rate'].mean()
                eligible_series = metrics[metrics > rate_threshold]
        elif filter_by == 'panel_count':
            if 'defect_panel_count' in processed_df.columns:
                metrics = processed_df.groupby(['defect_group', 'defect_desc'])['defect_panel_count'].sum()
                eligible_series = metrics[metrics > count_threshold]
        elif filter_by == 'occurrence':
            metrics = processed_df.groupby(['defect_group', 'defect_desc']).size()
            eligible_series = metrics[metrics > count_threshold]
    
        # 生成选项
        if not eligible_series.empty:
            sorted_series = eligible_series.sort_values(ascending=False)
            for group_name in active_groups:
                # 提取属于该 Group 的 Code
                group_codes_series = sorted_series[sorted_series.index.get_level_values('defect_group') == group_name]
                codes_list = group_codes_series.index.get_level_values('defect_desc').tolist()
                
                if codes_list:
                    code_options_by_group[group_name] = ["---请选择---"] + codes_list
                else:
                    code_options_by_group[group_name] = ["---请选择---"]

    # --- 4. 动态渲染 UI ---
    with st.container():
        # 标题栏：重置按钮
        header_cols = st.columns([0.95, 0.05])
        with header_cols[1]:
            if st.button("🔄", key=f"reset_{key_prefix}", help="重置所有Code选择"):
                for i in range(len(active_groups)):
                    state_key = f"{key_prefix}_g{i}"
                    if state_key in st.session_state:
                         st.session_state[state_key] = "---请选择---"
                st.rerun()

        # 内容栏：动态列数
        cols_count = len(active_groups) if len(active_groups) > 0 else 1
        content_cols = st.columns(cols_count)
        
        for i, col in enumerate(content_cols):
            group_name = active_groups[i]
            key = f"{key_prefix}_g{i}"
            
            # Session State 初始化
            if key not in st.session_state:
                st.session_state[key] = "---请选择---"

            with col:
                st.subheader(f"__{group_name}__")
                st.selectbox(
                    f"选择 {group_name}下的Code:",
                    options=code_options_by_group.get(group_name, ["---请选择---"]),
                    key=key,
                    label_visibility="collapsed"
                )

    # --- 5. 状态读取 ---
    for i, group_name in enumerate(active_groups):
        key = f"{key_prefix}_g{i}"
        if key in st.session_state and st.session_state[key] != "---请选择---":
            return {"group": group_name, "code": st.session_state[key]}

    return {"group": None, "code": None}