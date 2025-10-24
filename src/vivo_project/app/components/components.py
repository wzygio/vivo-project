import pandas as pd
import streamlit as st

@staticmethod
def create_code_selection_ui(
    source_data: pd.DataFrame | dict, 
    target_defect_groups: list, 
    key_prefix: str,
    rate_threshold: float = 0.001,
    count_threshold: int = 10
) -> dict:
    """
    (V3.3 - 最终健壮版) 
    1. 智能处理多种数据源 (DataFrame/dict)。
    2. 内置按“率”或“数”的筛选排序逻辑。
    3. 采用“渲染与读取分离”模式，彻底解决多选冲突Bug。
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
    if processed_df is not None and not processed_df.empty:
        
        # 检查必需的列
        required_cols = ['defect_group', 'defect_desc']
        if not all(col in processed_df.columns for col in required_cols):
            st.error(f"UI组件错误(key={key_prefix})：传入的数据源缺少必需的列 ('defect_group' or 'defect_desc')。")
            processed_df = None
        
        # 智能切换筛选模式
        elif 'defect_rate' in processed_df.columns:
            metrics = processed_df.groupby(['defect_group', 'defect_desc'])['defect_rate'].mean()
            eligible_series = metrics[metrics > rate_threshold]
        else:
            metrics = processed_df.groupby(['defect_group', 'defect_desc']).size()
            eligible_series = metrics[metrics > count_threshold]
        
        if processed_df is not None:
            sorted_series = eligible_series.sort_values(ascending=False)
            for group_name in target_defect_groups:
                group_codes = sorted_series[sorted_series.index.get_level_values('defect_group') == group_name]
                code_options_by_group[group_name] = ["---请选择---"] + group_codes.index.get_level_values('defect_desc').tolist()
    
    if not code_options_by_group:
        for group_name in target_defect_groups:
            code_options_by_group[group_name] = ["---请选择---"]

    # --- 2. UI渲染阶段 ---
    with st.container():
        header_cols = st.columns([0.95, 0.05])
        with header_cols[1]:
            if st.button("🔄", key=f"reset_{key_prefix}", help="重置所有Code选择"):
                for i in range(len(target_defect_groups)):
                    st.session_state[f"{key_prefix}_g{i}"] = "---请选择---"
                st.rerun()

        content_cols = st.columns(3)
        for i, col in enumerate(content_cols):
            group_name = target_defect_groups[i]
            key = f"{key_prefix}_g{i}"
            if key not in st.session_state:
                st.session_state[key] = "---请选择---" # 确保key被初始化
            
            with col:
                st.subheader(f"__{group_name}__")
                st.selectbox(
                    f"选择 {group_name}下的Code:",
                    options=code_options_by_group.get(group_name, ["---请选择---"]),
                    key=key,
                    label_visibility="collapsed"
                )

    # --- 3. 状态读取阶段 ---
    for i in range(len(target_defect_groups)):
        key = f"{key_prefix}_g{i}"
        if key in st.session_state and st.session_state[key] != "---请选择---":
            return {"group": target_defect_groups[i], "code": st.session_state[key]}
    
    return {"group": None, "code": None}