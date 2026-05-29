# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional

from app.utils.session_manager import SessionManager

def create_code_selection_ui(
    source_data: pd.DataFrame | dict,
    key_prefix: str,
    filter_by: str = 'rate',
    rate_threshold: float = 0.00015,
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

