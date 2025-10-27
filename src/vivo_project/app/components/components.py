# 可能位于 src/vivo_project/app/components/components.py 或类似文件
import pandas as pd
import streamlit as st
import logging # <-- 确保导入 logging

# [修改] 移除了 @staticmethod，因为它现在是一个独立函数
# 如果它仍然属于 Components 类，请保留 @staticmethod
def create_code_selection_ui(
    source_data: pd.DataFrame | dict,
    target_defect_groups: list,
    key_prefix: str,
    filter_by: str = 'rate',      # <--- [新增] 明确筛选模式 ('rate' 或 'count')
    rate_threshold: float = 0.001,
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

        # --- [核心修改] 使用 filter_by 参数决定筛选逻辑 ---
        elif filter_by == 'rate':
            if 'defect_rate' in processed_df.columns:
                logging.info(f"CodeSelection ({key_prefix}): 按平均不良率 > {rate_threshold:.4f} 筛选")
                metrics = processed_df.groupby(['defect_group', 'defect_desc'])['defect_rate'].mean()
                eligible_series = metrics[metrics > rate_threshold]
            else:
                logging.warning(f"CodeSelection ({key_prefix}): 请求按 'rate' 筛选，但缺少 'defect_rate' 列。将不进行筛选或返回空列表。")
                # eligible_series 保持为空

        elif filter_by == 'count':
            if 'defect_panel_count' in processed_df.columns:
                logging.info(f"CodeSelection ({key_prefix}): 按累计不良 Panel 数 > {count_threshold} 筛选")
                # --- [核心修改] 使用 sum('defect_panel_count') ---
                metrics = processed_df.groupby(['defect_group', 'defect_desc'])['defect_panel_count'].sum()
                eligible_series = metrics[metrics > count_threshold]
            else:
                logging.warning(f"CodeSelection ({key_prefix}): 请求按 'count' 筛选，但缺少 'defect_panel_count' 列。将不进行筛选或返回空列表。")
                # eligible_series 保持为空
        else:
             logging.error(f"CodeSelection ({key_prefix}): 无效的 filter_by 参数 '{filter_by}'。应为 'rate' 或 'count'。")
             # eligible_series 保持为空


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
