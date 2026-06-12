# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Union

PLACEHOLDER_OPTION = "---请选择---"


def _prepare_processed_dataframe(
    source_data: Union[pd.DataFrame, Dict[str, pd.DataFrame]]
) -> Optional[pd.DataFrame]:
    """Normalize selector input and preserve MWD dict grain names."""
    if isinstance(source_data, pd.DataFrame):
        return source_data.copy()

    if isinstance(source_data, dict):
        all_dfs: List[pd.DataFrame] = []
        for source_key, df in source_data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                current_df = df.copy()
                current_df["_source_grain"] = str(source_key)
                all_dfs.append(current_df)
        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)

    return None


def _select_monthly_rate_frame(processed_df: pd.DataFrame) -> pd.DataFrame:
    """Return rows that represent monthly rates when the input exposes them."""
    if "_source_grain" in processed_df.columns:
        grain = processed_df["_source_grain"].astype(str).str.lower()
        monthly_df = processed_df[grain == "monthly"].copy()
        if not monthly_df.empty:
            return monthly_df

    if "time_period" in processed_df.columns:
        period = processed_df["time_period"].astype(str)
        monthly_mask = (
            period.str.contains("月", na=False)
            | period.str.fullmatch(r"\d{4}-\d{1,2}", na=False)
            | period.str.fullmatch(r"\d{4}/\d{1,2}", na=False)
        )
        monthly_df = processed_df[monthly_mask].copy()
        if not monthly_df.empty:
            return monthly_df

    return processed_df


def _calculate_rate_metrics(processed_df: pd.DataFrame) -> pd.Series:
    """Calculate Code rate metrics with monthly average as the preferred basis."""
    metric_col = "monthly_avg_rate" if "monthly_avg_rate" in processed_df.columns else "defect_rate"
    if metric_col not in processed_df.columns:
        return pd.Series(dtype=float)

    metric_df = processed_df if metric_col == "monthly_avg_rate" else _select_monthly_rate_frame(processed_df)
    metric_df = metric_df.copy()
    metric_df[metric_col] = pd.to_numeric(metric_df[metric_col], errors="coerce")
    metric_df = metric_df.dropna(subset=["defect_group", "defect_desc", metric_col])
    if metric_df.empty:
        return pd.Series(dtype=float)

    return metric_df.groupby(["defect_group", "defect_desc"])[metric_col].mean()


def _calculate_eligible_series(
    processed_df: Optional[pd.DataFrame],
    filter_by: str,
    rate_threshold: float,
    count_threshold: int,
) -> pd.Series:
    """Calculate eligible Code metrics for the selector."""
    if processed_df is None or processed_df.empty:
        return pd.Series(dtype=float)

    if filter_by == "rate":
        metrics = _calculate_rate_metrics(processed_df)
        return metrics[metrics >= rate_threshold]

    if filter_by == "panel_count" and "defect_panel_count" in processed_df.columns:
        metrics = processed_df.groupby(["defect_group", "defect_desc"])["defect_panel_count"].sum()
        return metrics[metrics > count_threshold]

    if filter_by == "occurrence":
        metrics = processed_df.groupby(["defect_group", "defect_desc"]).size()
        return metrics[metrics > count_threshold]

    return pd.Series(dtype=float)


def _build_code_options_by_group(
    active_groups: List[str],
    eligible_series: pd.Series,
) -> Dict[str, List[str]]:
    """Build selectbox options grouped by defect group."""
    code_options_by_group: Dict[str, List[str]] = {
        group_name: [PLACEHOLDER_OPTION] for group_name in active_groups
    }
    if eligible_series.empty:
        return code_options_by_group

    sorted_series = eligible_series.sort_values(ascending=False)
    for group_name in active_groups:
        group_codes_series = sorted_series[
            sorted_series.index.get_level_values("defect_group") == group_name
        ]
        codes_list = [
            str(code)
            for code in group_codes_series.index.get_level_values("defect_desc").tolist()
            if str(code).strip() != ""
        ]

        if codes_list:
            code_options_by_group[group_name] = [PLACEHOLDER_OPTION] + codes_list

    return code_options_by_group


def _get_default_group(
    active_groups: List[str],
    code_options_by_group: Dict[str, List[str]],
) -> Optional[str]:
    """Choose the first group that still has selectable codes."""
    for group_name in active_groups:
        if len(code_options_by_group.get(group_name, [PLACEHOLDER_OPTION])) > 1:
            return group_name

    return active_groups[0] if active_groups else None


def create_code_selection_ui(
    source_data: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
    key_prefix: str,
    filter_by: str = 'rate',
    rate_threshold: float = 0.0001,
    count_threshold: int = 20
) -> Dict[str, Optional[str]]:
    """
    (V3.5 - 数据驱动版)
    完全基于 source_data 动态生成筛选器，不再强依赖 target_defect_groups 配置。
    
    [Refactor Note] 此函数逻辑主要依赖传入的 DataFrame 数据，不直接读取全局 CONFIG，
    因此保持原样，仅增强类型提示兼容性。
    """

    # --- 1. 数据聚合 ---
    processed_df = _prepare_processed_dataframe(source_data)

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
    eligible_series = _calculate_eligible_series(
        processed_df,
        filter_by=filter_by,
        rate_threshold=rate_threshold,
        count_threshold=count_threshold,
    )
    code_options_by_group = _build_code_options_by_group(active_groups, eligible_series)

    # --- 4. 动态渲染 UI ---
    group_key = f"{key_prefix}_group"
    code_key = f"{key_prefix}_code"
    last_group_key = f"{key_prefix}_last_group"
    default_group = _get_default_group(active_groups, code_options_by_group)

    if default_group is None:
        st.info("当前无可展示的 Group。")
        return {"group": None, "code": None}

    if group_key not in st.session_state or st.session_state[group_key] not in active_groups:
        st.session_state[group_key] = default_group

    selected_group = st.session_state[group_key]
    group_code_options = code_options_by_group.get(selected_group, [PLACEHOLDER_OPTION])
    if code_key not in st.session_state or st.session_state[code_key] not in group_code_options:
        st.session_state[code_key] = PLACEHOLDER_OPTION

    with st.container():
        group_col, code_col, reset_col = st.columns([1.2, 2.6, 0.28])

        with group_col:
            st.selectbox(
                "不良 Group",
                options=active_groups,
                key=group_key,
            )

        selected_group = st.session_state[group_key]
        group_changed = st.session_state.get(last_group_key) != selected_group
        group_code_options = code_options_by_group.get(selected_group, [PLACEHOLDER_OPTION])
        if group_changed or st.session_state.get(code_key) not in group_code_options:
            st.session_state[code_key] = PLACEHOLDER_OPTION
        st.session_state[last_group_key] = selected_group

        with code_col:
            st.selectbox(
                "Defect Code",
                options=group_code_options,
                key=code_key,
                help="仅显示月均不良率达到阈值的 Code。",
            )

        with reset_col:
            st.write("")
            if st.button("🔄", key=f"reset_{key_prefix}", help="重置 Code 选择", use_container_width=True):
                st.session_state[code_key] = PLACEHOLDER_OPTION
                st.rerun()

    # --- 5. 状态读取 ---
    selected_group = st.session_state.get(group_key)
    selected_code = st.session_state.get(code_key)
    selected_options = code_options_by_group.get(selected_group, [PLACEHOLDER_OPTION])
    if (
        selected_group in active_groups
        and selected_code != PLACEHOLDER_OPTION
        and selected_code in selected_options
    ):
        return {"group": selected_group, "code": selected_code}

    return {"group": None, "code": None}

