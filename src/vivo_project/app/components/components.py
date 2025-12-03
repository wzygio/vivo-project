# src/vivo_project/app/components/components.py
import pandas as pd
import streamlit as st
import logging, os
from pathlib import Path
from  vivo_project.config import CONFIG

@st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
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


def render_page_header(title: str):
    """
    渲染统一的页面头部组件
    布局：[ 标题 (Left) ] --------- [ 强制刷新按钮 (Right) ]
    功能：点击刷新按钮会自动删除本地快照并清除 st.cache，触发重新拉取。
    """
    
    # 1. 获取配置中的快照路径 (逻辑与 PanelRepository 保持一致)
    processing_conf = CONFIG.get('processing', {})
    snapshot_path_str = processing_conf.get('snapshot_path', 'data/panel_details_snapshot.parquet')
    snapshot_path = Path(snapshot_path_str).resolve()

    # 2. 定义刷新回调函数
    def _global_refresh_callback():
        # A. 删除本地快照文件 (核心：逼迫 Repository 发现文件缺失而去查库)
        if snapshot_path.exists():
            try:
                os.remove(snapshot_path)
                logging.info(f"🗑️ [UI] 用户触发强制刷新，本地快照已删除: {snapshot_path}")
            except Exception as e:
                logging.error(f"❌ 删除快照失败: {e}")
        else:
            logging.info("ℹ️ [UI] 本地快照不存在，无需删除。")
        
        # B. 清除 Streamlit 内存缓存 (核心：逼迫 Service 重新运行计算逻辑)
        st.cache_data.clear()
        
        # C. (可选) 如果使用了 st.cache_resource 也需要清除，通常 cache_data 够了
        # st.cache_resource.clear()
        
        # D. 回调结束后，Streamlit 会自动检测到状态变化并 Rerun 整个页面

    # 3. 布局渲染
    # 左侧标题占大头(5)，右侧按钮占小头(1)
    c_title, c_btn = st.columns([5, 1])
    
    with c_title:
        st.title(title)
        
    with c_btn:
        # 增加垂直间距，让按钮在视觉上与标题对齐
        st.write("") 
        st.write("")
        st.button(
            "🔄 刷新数据", 
            key=f"btn_refresh_{title}", # 使用标题作为 key 的一部分，防止不同页面冲突
            on_click=_global_refresh_callback, 
            use_container_width=True,
            help="点击此按钮将删除本地快照缓存，并强制从数据库获取最新数据。"
        )

def create_code_selection_ui(
    source_data: pd.DataFrame | dict,
    target_defect_groups: list,
    key_prefix: str,
    filter_by: str = 'rate',      # <--- [新增] 明确筛选模式 ('rate' 或 'count')
    rate_threshold: float = 0.0005,
    count_threshold: int = 10
) -> dict:
    """
    (V3.4 - 明确筛选逻辑 + 修正 Count 模式)
    1. 智能处理多种数据源。
    2. 通过 filter_by 参数明确指定按 'rate' (平均不良率) 或 'count' (累计不良Panel数) 筛选。
    3. 采用“渲染与读取分离”模式。
    4. 保留“右上角重置按钮”布局。
    """

    # --- 1. 数据准备阶段 ---
    processed_df = None
    if isinstance(source_data, pd.DataFrame):
        processed_df = source_data
    elif isinstance(source_data, dict):
        all_dfs = [df for df in source_data.values() if isinstance(df, pd.DataFrame) and not df.empty]
        if all_dfs:
            processed_df = pd.concat(all_dfs, ignore_index=True)

    code_options_by_group = {}
    eligible_series = pd.Series(dtype=float) # 初始化为空 Series

    if processed_df is not None and not processed_df.empty:
        required_cols = ['defect_group', 'defect_desc']
        if not all(col in processed_df.columns for col in required_cols):
            st.error(f"UI组件错误(key={key_prefix})：传入的数据源缺少必需的列 ('defect_group' or 'defect_desc')。")
            processed_df = None # 标记为无效

        # --- [核心修改] 实现三种筛选模式 ---
        elif filter_by == 'rate':
            if 'defect_rate' in processed_df.columns:
                logging.info(f"CodeSelection ({key_prefix}): 按平均不良率 > {rate_threshold:.4f} 筛选")
                metrics = processed_df.groupby(['defect_group', 'defect_desc'])['defect_rate'].mean()
                eligible_series = metrics[metrics > rate_threshold]
            else:
                logging.warning(f"CodeSelection ({key_prefix}): 请求按 'rate' 筛选，但缺少 'defect_rate' 列。无 Code 可选。")

        elif filter_by == 'panel_count':
            if 'defect_panel_count' in processed_df.columns:
                logging.info(f"CodeSelection ({key_prefix}): 按累计不良 Panel 数 > {count_threshold} 筛选")
                metrics = processed_df.groupby(['defect_group', 'defect_desc'])['defect_panel_count'].sum()
                eligible_series = metrics[metrics > count_threshold]
            else:
                logging.warning(f"CodeSelection ({key_prefix}): 请求按 'panel_count' 筛选，但缺少 'defect_panel_count' 列。无 Code 可选。")

        elif filter_by == 'occurrence':
            logging.info(f"CodeSelection ({key_prefix}): 按出现次数 > {count_threshold} 筛选")
            # --- 使用 .size() ---
            metrics = processed_df.groupby(['defect_group', 'defect_desc']).size()
            eligible_series = metrics[metrics > count_threshold]

        else:
             logging.error(f"CodeSelection ({key_prefix}): 无效的 filter_by 参数 '{filter_by}'。应为 'rate', 'panel_count' 或 'occurrence'。")


        # --- 生成选项列表 (基于筛选结果) ---
        if processed_df is not None and not eligible_series.empty:
             sorted_series = eligible_series.sort_values(ascending=False)
             for group_name in target_defect_groups:
                 # 筛选属于当前 group 的 code
                 group_codes_series = sorted_series[sorted_series.index.get_level_values('defect_group') == group_name]
                 # 获取 code 列表
                 codes_list = group_codes_series.index.get_level_values('defect_desc').tolist()
                 # 添加默认选项
                 code_options_by_group[group_name] = ["---请选择---"] + codes_list
        # else:
             # 如果 processed_df 无效，或筛选后 eligible_series 为空，则 code_options_by_group 会在后面被填充默认值

    # --- 确保所有 Group 都有选项（即使是空的）---
    if not code_options_by_group or len(code_options_by_group) < len(target_defect_groups) or \
       any(not v for v in code_options_by_group.values()): # 检查是否有空列表
        logging.warning(f"CodeSelection ({key_prefix}): 数据准备或筛选后无有效 Code 选项，将使用默认空列表。")
        for group_name in target_defect_groups:
             # 如果某个 group 的选项列表不存在或为空，则设置为默认值
             if group_name not in code_options_by_group or not code_options_by_group[group_name]:
                  code_options_by_group[group_name] = ["---请选择---"]


    # --- 2. UI渲染阶段 (逻辑不变) ---
    with st.container():
        header_cols = st.columns([0.95, 0.05])
        with header_cols[1]:
            if st.button("🔄", key=f"reset_{key_prefix}", help="重置所有Code选择"):
                for i in range(len(target_defect_groups)):
                    # [健壮性] 确保 key 存在于 session_state 中再修改
                    state_key = f"{key_prefix}_g{i}"
                    if state_key in st.session_state:
                         st.session_state[state_key] = "---请选择---"
                st.rerun()

        content_cols = st.columns(3)
        for i, col in enumerate(content_cols):
            group_name = target_defect_groups[i]
            key = f"{key_prefix}_g{i}"
            # 确保 session_state 初始化
            if key not in st.session_state:
                st.session_state[key] = "---请选择---"

            with col:
                st.subheader(f"__{group_name}__")
                st.selectbox(
                    f"选择 {group_name}下的Code:",
                    options=code_options_by_group.get(group_name, ["---请选择---"]), # 使用 .get() 更安全
                    key=key,
                    label_visibility="collapsed"
                )

    # --- 3. 状态读取阶段 (逻辑不变) ---
    for i in range(len(target_defect_groups)):
        key = f"{key_prefix}_g{i}"
        if key in st.session_state and st.session_state[key] != "---请选择---":
            return {"group": target_defect_groups[i], "code": st.session_state[key]}

    return {"group": None, "code": None}