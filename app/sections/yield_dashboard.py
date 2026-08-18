# src/vivo_project/app/components/view_sections.py
import streamlit as st
import pandas as pd
from functools import partial
from inspect import signature
from typing import Any, Dict, List, Optional, Sequence
import plotly.graph_objects as go

from app.manager.render_gate import RenderGate

# 引入现有的绘图函数
from app.charts.mwd_chart import (
    create_group_trend_chart, 
    create_code_trend_chart,
    slice_recent_data
)
from app.charts.sheet_lot_chart import (
    create_lot_defect_chart, 
    create_sheet_defect_chart,
    create_mapping_heatmap,
    parse_panel_id_to_coords,
    create_sheet_stack_chart
)
from app.constants import COLOR_MAP
from yield_domain.core.mapping.layout import resolve_mapping_layout
from yield_domain.core.mapping.mapping_processor import apply_hotspot_modification_to_matrix

# ==============================================================================
#  1. 宏观分析区 (Group Level)
# ==============================================================================
def render_macro_trend_section(mwd_group_data: dict, group_order: list | None = None):
    """
    渲染 Group 级宏观趋势堆叠柱状图。

    Parameters
    ----------
    mwd_group_data : dict
        包含 'monthly', 'weekly', 'daily' 三个 DataFrame 的趋势数据。
    group_order : list | None
        可选的 Group 规范排序列表（如 YAML 中的 target_defect_groups）。
        若提供，图表中 Group 将按此顺序排列；未在列表中的 Group 追加到末尾。
        若为 None，则按字母序排序（原行为）。
    """
    if not mwd_group_data:
        st.warning("无宏观趋势数据。")
        return

    available_groups = []
    ref_df = mwd_group_data.get('monthly')
    if ref_df is not None and not ref_df.empty:
        raw_groups = ref_df['defect_group'].unique().tolist()
        if group_order:
            # 1) 按配置规范顺序排列
            available_groups = [g for g in group_order if g in raw_groups]
            # 2) 追加数据中存在但配置中未列出的 Group（字母序）
            available_groups += sorted([g for g in raw_groups if g not in group_order])
        else:
            available_groups = sorted(raw_groups)
    
    dynamic_category_orders = {"defect_group": available_groups}

    c1, _, _ = st.columns(3)
    with c1:
        sel_grps_macro = st.multiselect(
            "选择Group (可多选):",
            available_groups,
            default=available_groups,
            key="macro_group_sel"
        )

    # 两阶段渲染：先在 RenderGate 统一 spinner 下构建全部图表，再集中回流渲染，
    # 避免月/周/日三张图随计算进度一张一张跳出。
    gate = RenderGate()
    gate.stage(
        partial(
            _prepare_macro_trend_figures,
            mwd_group_data=mwd_group_data,
            sel_grps_macro=sel_grps_macro,
            dynamic_category_orders=dynamic_category_orders,
        )
    )
    trend_figures = gate.collect()[0]

    gc1, gc2, gc3 = st.columns(3)
    for (title, fig), col in zip(trend_figures, [gc1, gc2, gc3]):
        with col:
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"{title}数据暂无")


def _prepare_macro_trend_figures(
    mwd_group_data: dict,
    sel_grps_macro: list,
    dynamic_category_orders: dict,
) -> List[tuple]:
    """[RenderGate 阶段1] 纯计算：构建 Group 级月/周/日趋势图，禁止触碰 st.*。"""
    df_m = slice_recent_data(mwd_group_data.get('monthly'), 3)
    df_w = slice_recent_data(mwd_group_data.get('weekly'), 3)
    df_d = slice_recent_data(mwd_group_data.get('daily'), 7)

    # 多选过滤: 若用户有选中 Group 则只展示选中项；未选或全不选时展示全部
    if sel_grps_macro:
        filter_func = lambda df: df[df['defect_group'].isin(sel_grps_macro)] if df is not None else None
        df_m, df_w, df_d = map(filter_func, [df_m, df_w, df_d])

    max_rate = 0
    for df in [df_m, df_w, df_d]:
        if df is not None and not df.empty:
            curr_max = df.groupby('time_period')['defect_rate'].sum().max()
            if pd.notna(curr_max): max_rate = max(max_rate, curr_max)
    y_limit = [0, max_rate * 1.2] if max_rate > 0 else [0, 0.1]

    chart_configs = [
        (df_m, "月度趋势", False, True),
        (df_w, "周度趋势", False, False),
        (df_d, "日度趋势", True, False),
    ]
    figures = []
    for df, title, show_slider, show_count in chart_configs:
        if df is None or df.empty:
            figures.append((title, None))
            continue
        figures.append((
            title,
            create_group_trend_chart(
                df, title, show_slider, show_count, y_limit, COLOR_MAP,
                dynamic_category_orders, show_input_count=True
            ),
        ))
    return figures

# ==============================================================================
#  2. 微观分析区 (Row A: Code 级时间趋势)
# ==============================================================================
def render_micro_trend_section(mwd_code_data: dict, curr_code: str, curr_warning: float):
    with st.container(border=True):
        st.markdown("**A. 月周天趋势图**")
        
        cd_m = slice_recent_data(mwd_code_data.get('monthly'), 3)
        cd_w = slice_recent_data(mwd_code_data.get('weekly'), 3)
        cd_d = slice_recent_data(mwd_code_data.get('daily'), 7)
        
        filter_c = lambda df: df[df['defect_desc'] == curr_code] if df is not None else None
        cd_m, cd_w, cd_d = map(filter_c, [cd_m, cd_w, cd_d])

        c_max = 0
        for df in [cd_m, cd_w, cd_d]:
            if df is not None and not df.empty:
                c_max = max(c_max, df['defect_rate'].max())
        c_ylim = [0, c_max * 1.25] if c_max > 0 else [0, 0.01]

        rc1, rc2, rc3 = st.columns(3)
        chart_configs = [(cd_m, "月度", rc1), (cd_w, "周度", rc2), (cd_d, "日度", rc3)]

        for df, title, col in chart_configs:
            with col:
                if df is not None and not df.empty:
                    fig = create_code_trend_chart(df, title, c_ylim, curr_warning)
                    if fig is not None:
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info(f"暂无{title}数据")
                else:
                    st.info(f"暂无{title}数据")

# ==============================================================================
#  3. Lot 集中性 (Row B: 时间趋势)
# ==============================================================================
def render_lot_distribution_section(lot_data: dict, curr_code: str, curr_warning: float) -> str:
    """渲染 Lot 集中性图表，并返回用户点击选中的 target_lot"""
    lot_details = lot_data.get("code_level_details", {})
    if not lot_details:
        with st.container(border=True): st.warning("暂无 Lot 数据。")
        return ""

    df_lot_all = pd.concat(lot_details.values(), ignore_index=True)
    df_lot_all['warehousing_time'] = pd.to_datetime(df_lot_all['warehousing_time'], format='%Y%m%d', errors='coerce')
    df_lot_curr = df_lot_all[df_lot_all['defect_desc'] == curr_code].copy()
    df_lot_curr = df_lot_curr[df_lot_curr['defect_rate'] > 0]

    if df_lot_curr.empty:
        with st.container(border=True): st.warning(f"当前 Code ({curr_code}) 在 Lot 级无不良记录。")
        return ""

    iso_s = df_lot_curr['warehousing_time'].dt.isocalendar()
    df_lot_curr['week_label'] = iso_s.year.astype(str) + '-W' + iso_s.week.map('{:02d}'.format)
    df_lot_curr['month_str'] = df_lot_curr['warehousing_time'].dt.strftime('%Y-%m')

    with st.container(border=True):
        st.markdown("**B. Lot集中性 (点击蓝色柱体可查看 Sheet 分布)**")
        
        lc1, lc2, lc3 = st.columns(3)
        with lc1: l_sort = st.selectbox("排序:", ["按入库时间(默认)", "按阵列投入时间", "按不良率(降序)"], key="u_lot_sort")
        with lc2:
            l_months = sorted(df_lot_curr['month_str'].dropna().unique(), reverse=True)
            l_sel_m = st.selectbox("月别:", ["全部"] + l_months, key="u_lot_m")
        with lc3:
            l_weeks = sorted(df_lot_curr['week_label'].dropna().unique(), reverse=True)
            l_sel_w = st.selectbox("周别:", ["全部"] + l_weeks, key="u_lot_w")

        if l_sel_m != "全部": df_lot_curr = df_lot_curr[df_lot_curr['month_str'] == l_sel_m]
        if l_sel_w != "全部": df_lot_curr = df_lot_curr[df_lot_curr['week_label'] == l_sel_w]

        x_lbl = "Lot ID"
        if l_sort == "按不良率(降序)": df_lot_curr = df_lot_curr.sort_values('defect_rate', ascending=False)
        elif l_sort == "按入库时间(默认)": df_lot_curr = df_lot_curr.sort_values('warehousing_time')
        elif l_sort == "按阵列投入时间": df_lot_curr = df_lot_curr.sort_values('array_input_time')

        if df_lot_curr.empty:
            st.warning("当前筛选条件下无 Lot 数据。")
        else:
            fig_lot = create_lot_defect_chart(df_lot_curr, x_lbl, df_lot_curr['lot_id'].tolist(), curr_warning)
            event = st.plotly_chart(fig_lot, use_container_width=True, on_select="rerun", selection_mode="points")
            
            # Plotly 的空白点击会触发 rerun，但 points 为空；此时必须清除
            # 上一次选择，否则 Sheet 图表会一直沿用旧 Lot。
            if event is not None:
                selection = getattr(event, "selection", None)
                points = selection.get("points", []) if selection else []
                if points:
                    clicked_lot = points[0]["x"]
                    if st.session_state.get("unified_sheet_lot_input") != clicked_lot:
                        st.session_state["unified_sheet_lot_input"] = clicked_lot
                        st.toast(f"已锁定 Lot: {clicked_lot}", icon="🔒")
                        st.rerun()
                else:
                    st.session_state["unified_sheet_lot_input"] = ""
                    st.session_state["sheet_lot_input_box"] = ""

    return st.session_state.get("unified_sheet_lot_input", "")

# ==============================================================================
#  4. Sheet 分布 (Row C: 单片维度)
# ==============================================================================
def render_sheet_distribution_section(sheet_data: dict, target_lot: str, curr_group: str, curr_code: str):
    """处理复杂的数据清洗与 Left Join，然后渲染图表"""
    with st.container(border=True):
        st.markdown("**C. 单片分布 (By Sheet)**")
        sc1, sc2, _ = st.columns([3, 3, 4])
        
        with sc1:
            # 允许用户手动输入或接收上面点击传过来的值
            input_lot = st.text_input("当前分析 Lot ID:", value=target_lot, key="sheet_lot_input_box", help="点击上方柱图自动填充")
            if input_lot != st.session_state.get("unified_sheet_lot_input"):
                st.session_state["unified_sheet_lot_input"] = input_lot

        target_lot = input_lot

        if not target_lot:
            st.info("等待输入 Lot ID 或点击上方图表...")
            return

        group_summary = sheet_data.get("group_level_summary_for_table", pd.DataFrame())
        if group_summary.empty or 'lot_id' not in group_summary.columns:
            st.warning("暂无 Sheet 级明细数据。")
            return

        df_base_sheets = group_summary[group_summary['lot_id'] == target_lot][
            ['sheet_id', 'lot_id', 'warehousing_time', 'array_input_time']
        ].copy()
        
        if df_base_sheets.empty:
            st.warning(f"分析报告：未找到 Lot '{target_lot}' 的任何 Sheet 基础数据。")
            return

        sheet_details_dict = sheet_data.get("code_level_details", {})
        df_sheet_all = sheet_details_dict.get(curr_group, pd.DataFrame())
        
        if not df_sheet_all.empty:
            df_defect_only = df_sheet_all[
                (df_sheet_all['lot_id'] == target_lot) & 
                (df_sheet_all['defect_desc'] == curr_code)
            ][['sheet_id', 'defect_rate', 'defect_panel_count']]
        else:
            df_defect_only = pd.DataFrame(columns=['sheet_id', 'defect_rate', 'defect_panel_count'])
        
        # 数据组装与清洗
        df_sheet = pd.merge(df_base_sheets, df_defect_only, on='sheet_id', how='left')
        df_sheet['defect_rate'] = df_sheet['defect_rate'].fillna(0.0)
        df_sheet['defect_panel_count'] = df_sheet['defect_panel_count'].fillna(0).astype(int)
        df_sheet['warehousing_time'] = pd.to_datetime(df_sheet['warehousing_time'], format='%Y%m%d', errors='coerce')
        
        with sc2:
            s_sort = st.selectbox("Sheet 排序规则:", ["默认(投入时间)", "按不良率(降序)"], key="u_sheet_sort")
        
        if s_sort == "按不良率(降序)":
            df_sheet = df_sheet.sort_values(['defect_rate', 'array_input_time'], ascending=[False, True])
        else:
            df_sheet = df_sheet.sort_values('array_input_time')
        
        fig_sheet = create_sheet_defect_chart(
            df=df_sheet, xaxis_label="Sheet ID", sorted_sheet_ids=df_sheet['sheet_id'].tolist()
        )
        st.plotly_chart(fig_sheet, use_container_width=True)
 
# ==============================================================================
#  5. Mapping (Row D: 空间维度)
# ==============================================================================
def render_mapping_section(
    mapping_data: pd.DataFrame,
    curr_group: str,
    curr_code: str,
    hotspot_scripts: list,
    product_code: Optional[str] = None,
    mapping_layout: Optional[dict] = None,
):
    """处理矩阵变换与热区应用，渲染 Mapping"""
    with st.container(border=True):
        st.markdown("**D. Mapping集中性**")
        
        if mapping_data is None or mapping_data.empty:
            st.warning("Mapping 数据源为空。")
            return

        df_map = mapping_data[
            (mapping_data['defect_group'] == curr_group) & 
            (mapping_data['defect_desc'] == curr_code)
        ]
        
        if df_map.empty:
            st.warning("该 Code 在 Mapping 数据源中无记录。")
            return

        batches = sorted(df_map['batch_no'].unique())
        total_batches = len(batches)
        tab_labels = []
        for b in batches:
            b_data = df_map[df_map['batch_no'] == b]
            total_in = b_data['batch_total_input'].iloc[0] if 'batch_total_input' in b_data.columns else 0
            tab_labels.append(f"{b} ({int(total_in):,})" if total_in else f"{b}")
            
        tabs = st.tabs(tab_labels)
        matrices_cache = {}
        g_max = 0
        resolved_layout = resolve_mapping_layout(mapping_layout)
        
        for i, b in enumerate(batches):
            d = df_map[df_map['batch_no'] == b]
            coords = d['panel_id'].apply(
                lambda panel_id: parse_panel_id_to_coords(
                    panel_id,
                    resolved_layout,
                )
            )
            d_c = d.assign(r=coords.str[0], c=coords.str[1]).dropna(subset=['r','c'])
            d_c[['r','c']] = d_c[['r','c']].astype(int)
            
            mat = pd.pivot_table(d_c, values='panel_id', index='r', columns='c', aggfunc='count', fill_value=0)
            mat = mat.reindex(
                index=range(len(resolved_layout.row_labels)),
                columns=range(len(resolved_layout.column_labels)),
                fill_value=0,
            )
            
            # [修改] 传递数字位置而非字符串：i=0(最旧), i=total-1(最新), i=中间位置
            mat = apply_hotspot_modification_to_matrix(
                heatmap_matrix=mat, batch_no=b, code_desc=curr_code,
                batch_position=i, total_batches=total_batches,
                script_config_list=hotspot_scripts,
                product_code=product_code,
                mapping_layout=resolved_layout,
            )

            matrices_cache[b] = mat
            g_max = max(g_max, mat.max().max())
        
        for i, b in enumerate(batches):
            with tabs[i]:
                fig_map = create_mapping_heatmap(
                    matrices_cache[b],
                    f"批次 {b} 热力图",
                    g_max,
                    mapping_layout=resolved_layout,
                )
                st.plotly_chart(fig_map, use_container_width=True)


# ==============================================================================
#  6. Code 批量展示紧凑布局
# ==============================================================================
def _apply_compact_chart_layout(fig: go.Figure, height: int) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=34, b=26),
        title_font_size=13,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(tickfont=dict(size=10))
    fig.update_yaxes(tickfont=dict(size=10))
    return fig


def _state_key_fragment(*parts: str) -> str:
    return "_".join(
        "".join(ch if ch.isalnum() else "_" for ch in str(part))
        for part in parts
    )


def _get_code_trend_slices(
    mwd_code_data: dict,
    curr_code: str,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, List[float]]:
    cd_m = slice_recent_data(mwd_code_data.get('monthly'), 3)
    cd_w = slice_recent_data(mwd_code_data.get('weekly'), 3)
    cd_d = slice_recent_data(mwd_code_data.get('daily'), 7)

    def _filter_code(df: pd.DataFrame | None) -> pd.DataFrame | None:
        if df is None:
            return None
        return df[df['defect_desc'] == curr_code].copy()

    cd_m, cd_w, cd_d = map(_filter_code, [cd_m, cd_w, cd_d])
    c_max = 0.0
    for df in [cd_m, cd_w, cd_d]:
        if df is not None and not df.empty:
            c_max = max(c_max, float(df['defect_rate'].max()))
    c_ylim = [0, c_max * 1.25] if c_max > 0 else [0, 0.01]
    return cd_m, cd_w, cd_d, c_ylim


def _prepare_compact_trend_figures(
    mwd_code_data: dict,
    curr_code: str,
    curr_warning: float,
) -> List[tuple]:
    """[RenderGate 阶段1] 纯计算：构建单个 Code 的月/周/日趋势图，禁止触碰 st.*。"""
    cd_m, cd_w, cd_d, c_ylim = _get_code_trend_slices(mwd_code_data, curr_code)
    entries: List[tuple] = []
    for df, title in [(cd_m, "月度"), (cd_w, "周度"), (cd_d, "日度")]:
        fig = None
        if df is not None and not df.empty:
            fig = create_code_trend_chart(df, title, c_ylim, curr_warning)
            if fig is not None:
                fig = _apply_compact_chart_layout(fig, 345)
        entries.append((title, fig))
    return entries


def _render_compact_trend_entries(entries: List[tuple], key_fragment: str) -> None:
    """[RenderGate 阶段2] 纯渲染：仅执行 st.* 调用，不做任何重计算。"""
    st.markdown("**A. 月周天趋势图**")
    trend_cols = st.columns(3)
    period_keys = ["monthly", "weekly", "daily"]

    for chart_index, (title, fig) in enumerate(entries):
        with trend_cols[chart_index]:
            if fig is None:
                st.info(f"暂无{title}数据")
                continue
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"compact_micro_trend_{key_fragment}_{period_keys[chart_index]}",
            )


def _render_compact_micro_trends(
    mwd_code_data: dict,
    curr_group: str,
    curr_code: str,
    curr_warning: float,
) -> None:
    entries = _prepare_compact_trend_figures(mwd_code_data, curr_code, curr_warning)
    _render_compact_trend_entries(entries, _state_key_fragment(curr_group, curr_code))


def _prepare_mapping_matrices(
    mapping_data: pd.DataFrame,
    curr_group: str,
    curr_code: str,
    hotspot_scripts: list,
    product_code: Optional[str],
    mapping_layout: Optional[dict] = None,
) -> tuple[List[str], Dict[str, pd.DataFrame], int, List[str]]:
    df_map = mapping_data[
        (mapping_data['defect_group'] == curr_group) &
        (mapping_data['defect_desc'] == curr_code)
    ]
    if df_map.empty:
        return [], {}, 0, []

    batches = sorted(df_map['batch_no'].unique())
    total_batches = len(batches)
    tab_labels: List[str] = []
    matrices_cache: Dict[str, pd.DataFrame] = {}
    global_max = 0
    resolved_layout = resolve_mapping_layout(mapping_layout)

    for i, batch_no in enumerate(batches):
        batch_df = df_map[df_map['batch_no'] == batch_no]
        total_in = batch_df['batch_total_input'].iloc[0] if 'batch_total_input' in batch_df.columns else 0
        tab_labels.append(f"{batch_no} ({int(total_in):,})" if total_in else str(batch_no))

        coords = batch_df['panel_id'].apply(
            lambda panel_id: parse_panel_id_to_coords(
                panel_id,
                resolved_layout,
            )
        )
        coord_df = batch_df.assign(r=coords.str[0], c=coords.str[1]).dropna(subset=['r', 'c'])
        coord_df[['r', 'c']] = coord_df[['r', 'c']].astype(int)

        matrix = pd.pivot_table(
            coord_df,
            values='panel_id',
            index='r',
            columns='c',
            aggfunc='count',
            fill_value=0,
        )
        matrix = matrix.reindex(
            index=range(len(resolved_layout.row_labels)),
            columns=range(len(resolved_layout.column_labels)),
            fill_value=0,
        )
        matrix = apply_hotspot_modification_to_matrix(
            heatmap_matrix=matrix,
            batch_no=batch_no,
            code_desc=curr_code,
            batch_position=i,
            total_batches=total_batches,
            script_config_list=hotspot_scripts,
            product_code=product_code,
            mapping_layout=resolved_layout,
        )
        matrices_cache[batch_no] = matrix
        global_max = max(global_max, int(matrix.max().max()))

    return batches, matrices_cache, global_max, tab_labels


def _prepare_compact_mapping_payload(
    mapping_data: Optional[pd.DataFrame],
    curr_group: str,
    curr_code: str,
    hotspot_scripts: list,
    product_code: Optional[str],
    mapping_layout: Optional[dict] = None,
) -> dict:
    """[RenderGate 阶段1] 纯计算：构建 Mapping 各批次热力图，禁止触碰 st.*。"""
    payload = {
        "curr_group": curr_group,
        "curr_code": curr_code,
        "status": "ok",
        "batches": [],
        "tab_labels": [],
        "default_tab": None,
        "figures": [],
    }
    if mapping_data is None or mapping_data.empty:
        payload["status"] = "empty_source"
        return payload

    resolved_layout = resolve_mapping_layout(mapping_layout)
    batches, matrices_cache, global_max, tab_labels = _prepare_mapping_matrices(
        mapping_data=mapping_data,
        curr_group=curr_group,
        curr_code=curr_code,
        hotspot_scripts=hotspot_scripts,
        product_code=product_code,
        mapping_layout=resolved_layout,
    )
    if not batches:
        payload["status"] = "no_records"
        return payload

    figures = [
        _apply_compact_chart_layout(
            create_mapping_heatmap(
                matrices_cache[batch_no],
                f"批次 {batch_no} 热力图",
                global_max,
                mapping_layout=resolved_layout,
            ),
            345,
        )
        for batch_no in batches
    ]
    payload["batches"] = batches
    payload["tab_labels"] = tab_labels
    payload["default_tab"] = tab_labels[-1] if len(tab_labels) >= 2 else tab_labels[-1]
    payload["figures"] = figures
    return payload


def _tabs_with_optional_default(
    tab_labels: list[str],
    default_tab: str,
) -> Sequence[Any]:
    """Create tabs across Streamlit versions that may not support ``default``."""
    if "default" not in signature(st.tabs).parameters:
        return st.tabs(tab_labels)
    return st.tabs(tab_labels, default=default_tab)


def _render_compact_mapping_payload(payload: dict) -> None:
    """[RenderGate 阶段2] 纯渲染：仅执行 st.* 调用，不做任何重计算。"""
    st.markdown("**B. Mapping集中性**")
    status = payload["status"]
    if status == "empty_source":
        st.warning("Mapping 数据源为空。")
        return
    if status == "no_records":
        st.warning("该 Code 在 Mapping 数据源中无记录。")
        return

    tabs = _tabs_with_optional_default(
        payload["tab_labels"],
        payload["default_tab"],
    )
    key_fragment = _state_key_fragment(payload["curr_group"], payload["curr_code"])
    for i, batch_no in enumerate(payload["batches"]):
        with tabs[i]:
            st.plotly_chart(
                payload["figures"][i],
                use_container_width=True,
                key=f"compact_mapping_{key_fragment}_{_state_key_fragment(batch_no)}",
            )


def _render_compact_mapping_section(
    mapping_data: Optional[pd.DataFrame],
    curr_group: str,
    curr_code: str,
    hotspot_scripts: list,
    product_code: Optional[str],
    mapping_layout: Optional[dict] = None,
) -> None:
    payload = _prepare_compact_mapping_payload(
        mapping_data=mapping_data,
        curr_group=curr_group,
        curr_code=curr_code,
        hotspot_scripts=hotspot_scripts,
        product_code=product_code,
        mapping_layout=mapping_layout,
    )
    _render_compact_mapping_payload(payload)


def _prepare_lot_code_dataframe(lot_data: dict, curr_code: str) -> pd.DataFrame:
    lot_details = lot_data.get("code_level_details", {}) if lot_data else {}
    if not lot_details:
        return pd.DataFrame()

    lot_frames = [df for df in lot_details.values() if isinstance(df, pd.DataFrame) and not df.empty]
    if not lot_frames:
        return pd.DataFrame()

    df_lot_all = pd.concat(lot_frames, ignore_index=True)
    if 'defect_desc' not in df_lot_all.columns:
        return pd.DataFrame()

    df_lot_curr = df_lot_all[df_lot_all['defect_desc'] == curr_code].copy()
    if df_lot_curr.empty:
        return df_lot_curr

    df_lot_curr['warehousing_time'] = pd.to_datetime(
        df_lot_curr['warehousing_time'],
        format='%Y%m%d',
        errors='coerce',
    )
    if 'array_input_time' in df_lot_curr.columns:
        df_lot_curr['array_input_time'] = pd.to_datetime(df_lot_curr['array_input_time'], errors='coerce')
    else:
        df_lot_curr['array_input_time'] = pd.NaT
    if 'defect_panel_count' not in df_lot_curr.columns:
        df_lot_curr['defect_panel_count'] = 0
    df_lot_curr['defect_rate'] = pd.to_numeric(df_lot_curr['defect_rate'], errors='coerce').fillna(0.0)
    df_lot_curr = df_lot_curr[df_lot_curr['defect_rate'] > 0]
    if df_lot_curr.empty:
        return df_lot_curr

    iso_s = df_lot_curr['warehousing_time'].dt.isocalendar()
    df_lot_curr['week_label'] = iso_s.year.astype(str) + '-W' + iso_s.week.map('{:02d}'.format)
    return df_lot_curr.sort_values('warehousing_time')


def _prepare_compact_lot_figure(
    lot_data: dict,
    curr_code: str,
    curr_warning: float,
) -> go.Figure | None:
    """[RenderGate 阶段1] 纯计算：构建 Lot 集中性柱状图，禁止触碰 st.*。"""
    df_lot_curr = _prepare_lot_code_dataframe(lot_data, curr_code)
    if df_lot_curr.empty:
        return None
    return _apply_compact_chart_layout(
        create_lot_defect_chart(
            df_lot_curr,
            "Lot ID",
            df_lot_curr['lot_id'].astype(str).tolist(),
            curr_warning,
        ),
        300,
    )


def _render_compact_lot_payload(lot_payload: dict, key_fragment: str) -> str:
    """[RenderGate 阶段2] 纯渲染 + 选择事件处理，返回当前选中的 target_lot。"""
    st.markdown("**C. Lot集中性**")
    if lot_payload["fig"] is None:
        st.warning(f"当前 Code ({lot_payload['curr_code']}) 在 Lot 级无不良记录。")
        return ""

    selected_lot_key = f"compact_sheet_lot_{key_fragment}"
    event = st.plotly_chart(
        lot_payload["fig"],
        use_container_width=True,
        on_select="rerun",
        selection_mode="points",
        key=f"compact_lot_chart_{key_fragment}",
    )

    # 空白点击会产生 points=[]。显式清除选择，才能让联动的 Sheet 图表消失。
    # event=None 表示普通 rerun，不能误清除当前 Lot。
    if event is not None:
        selection = getattr(event, "selection", None)
        points = selection.get("points", []) if selection else []
        if points:
            clicked_lot = str(points[0]["x"])
            st.session_state[selected_lot_key] = clicked_lot
        else:
            st.session_state[selected_lot_key] = ""

    return str(st.session_state.get(selected_lot_key, ""))


def _render_compact_lot_chart(
    lot_data: dict,
    curr_group: str,
    curr_code: str,
    curr_warning: float,
) -> str:
    fig = _prepare_compact_lot_figure(lot_data, curr_code, curr_warning)
    lot_payload = {"curr_code": curr_code, "fig": fig}
    return _render_compact_lot_payload(lot_payload, _state_key_fragment(curr_group, curr_code))


def _prepare_sheet_chart_dataframe(
    sheet_data: dict,
    target_lot: str,
    curr_group: str,
    curr_code: str,
) -> pd.DataFrame:
    group_summary = sheet_data.get("group_level_summary_for_table", pd.DataFrame()) if sheet_data else pd.DataFrame()
    if group_summary.empty or 'lot_id' not in group_summary.columns:
        return pd.DataFrame()

    base_cols = [
        col for col in ['sheet_id', 'lot_id', 'warehousing_time', 'array_input_time']
        if col in group_summary.columns
    ]
    if 'sheet_id' not in base_cols:
        return pd.DataFrame()

    df_base_sheets = group_summary[group_summary['lot_id'].astype(str) == str(target_lot)][base_cols].copy()
    if df_base_sheets.empty:
        return pd.DataFrame()

    sheet_details_dict = sheet_data.get("code_level_details", {})
    df_sheet_all = sheet_details_dict.get(curr_group, pd.DataFrame())
    if not df_sheet_all.empty:
        df_defect_only = df_sheet_all[
            (df_sheet_all['lot_id'].astype(str) == str(target_lot)) &
            (df_sheet_all['defect_desc'] == curr_code)
        ].copy()
        keep_cols = [col for col in ['sheet_id', 'defect_rate', 'defect_panel_count'] if col in df_defect_only.columns]
        df_defect_only = df_defect_only[keep_cols] if keep_cols else pd.DataFrame(columns=['sheet_id'])
    else:
        df_defect_only = pd.DataFrame(columns=['sheet_id', 'defect_rate', 'defect_panel_count'])

    df_sheet = pd.merge(df_base_sheets, df_defect_only, on='sheet_id', how='left')
    if 'defect_rate' not in df_sheet.columns:
        df_sheet['defect_rate'] = 0.0
    if 'defect_panel_count' not in df_sheet.columns:
        df_sheet['defect_panel_count'] = 0
    df_sheet['defect_rate'] = pd.to_numeric(df_sheet['defect_rate'], errors='coerce').fillna(0.0)
    df_sheet['defect_panel_count'] = pd.to_numeric(
        df_sheet['defect_panel_count'],
        errors='coerce',
    ).fillna(0).astype(int)
    if 'warehousing_time' in df_sheet.columns:
        df_sheet['warehousing_time'] = pd.to_datetime(df_sheet['warehousing_time'], format='%Y%m%d', errors='coerce')
    else:
        df_sheet['warehousing_time'] = pd.NaT
    if 'array_input_time' in df_sheet.columns:
        df_sheet['array_input_time'] = pd.to_datetime(df_sheet['array_input_time'], errors='coerce')
    else:
        df_sheet['array_input_time'] = pd.NaT

    if 'array_input_time' in df_sheet.columns:
        return df_sheet.sort_values('array_input_time')
    return df_sheet.sort_values('sheet_id')


def _prepare_compact_sheet_figure(
    sheet_data: dict,
    target_lot: str,
    curr_group: str,
    curr_code: str,
) -> go.Figure | None:
    """[RenderGate 阶段1] 纯计算：构建 Sheet 分布图，禁止触碰 st.*。"""
    if not target_lot:
        return None
    df_sheet = _prepare_sheet_chart_dataframe(sheet_data, target_lot, curr_group, curr_code)
    if df_sheet.empty:
        return None
    return _apply_compact_chart_layout(
        create_sheet_defect_chart(
            df=df_sheet,
            xaxis_label="Sheet ID",
            sorted_sheet_ids=df_sheet['sheet_id'].astype(str).tolist(),
        ),
        300,
    )


def _render_compact_sheet_chart(
    sheet_data: dict,
    target_lot: str,
    curr_group: str,
    curr_code: str,
) -> None:
    if not target_lot:
        st.caption("点击上方 Lot 柱体后显示 Sheet 分布。")
        return

    st.markdown(f"**D. Sheet分布 | Lot {target_lot}**")
    df_sheet = _prepare_sheet_chart_dataframe(sheet_data, target_lot, curr_group, curr_code)
    if df_sheet.empty:
        st.warning(f"未找到 Lot {target_lot} 的 Sheet 级明细。")
        return

    fig_sheet = create_sheet_defect_chart(
        df=df_sheet,
        xaxis_label="Sheet ID",
        sorted_sheet_ids=df_sheet['sheet_id'].astype(str).tolist(),
    )
    st.plotly_chart(
        _apply_compact_chart_layout(fig_sheet, 300),
        use_container_width=True,
        key=f"compact_sheet_chart_{_state_key_fragment(curr_group, curr_code, target_lot)}",
    )


def _prepare_sheet_detail_table(sheet_data: dict, curr_group: str, curr_code: str) -> pd.DataFrame:
    sheet_details_dict = sheet_data.get("code_level_details", {}) if sheet_data else {}
    df_sheet_all = sheet_details_dict.get(curr_group, pd.DataFrame())
    if df_sheet_all is None or df_sheet_all.empty or 'defect_desc' not in df_sheet_all.columns:
        return pd.DataFrame()

    df_sheet = df_sheet_all[df_sheet_all['defect_desc'] == curr_code].copy()
    if df_sheet.empty:
        return df_sheet

    if 'defect_panel_count' in df_sheet.columns:
        df_sheet['defect_panel_count'] = pd.to_numeric(df_sheet['defect_panel_count'], errors='coerce').fillna(0).astype(int)
        df_sheet = df_sheet[df_sheet['defect_panel_count'] > 0]

    if 'defect_rate' in df_sheet.columns:
        df_sheet['defect_rate'] = pd.to_numeric(df_sheet['defect_rate'], errors='coerce').fillna(0.0)
        sort_cols = [col for col in ['defect_rate', 'lot_id', 'sheet_id'] if col in df_sheet.columns]
        ascending = [False if col == 'defect_rate' else True for col in sort_cols]
        if sort_cols:
            df_sheet = df_sheet.sort_values(sort_cols, ascending=ascending)

    for date_col in ['warehousing_time', 'array_input_time']:
        if date_col in df_sheet.columns:
            df_sheet[date_col] = pd.to_datetime(df_sheet[date_col], errors='coerce')

    return df_sheet.reset_index(drop=True)


def _render_compact_sheet_table(sheet_data: dict, curr_group: str, curr_code: str) -> None:
    st.markdown("**D. Sheet明细**")
    df_sheet = _prepare_sheet_detail_table(sheet_data, curr_group, curr_code)
    if df_sheet.empty:
        st.warning("当前 Code 暂无 Sheet 级不良明细。")
        return

    display_cols = [
        col for col in [
            'lot_id',
            'sheet_id',
            'warehousing_time',
            'array_input_time',
            'defect_panel_count',
            'defect_rate',
            'total_panels',
        ] if col in df_sheet.columns
    ]
    df_display = df_sheet[display_cols].copy()
    if 'defect_rate' in df_display.columns:
        df_display['defect_rate'] = df_display['defect_rate'] * 100

    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        height=300,
        column_config={
            "lot_id": st.column_config.TextColumn("Lot ID", width="medium"),
            "sheet_id": st.column_config.TextColumn("Sheet ID", width="medium"),
            "warehousing_time": st.column_config.DatetimeColumn("入库时间", format="YYYY/MM/DD", width="small"),
            "array_input_time": st.column_config.DatetimeColumn("阵列投入", format="YYYY/MM/DD HH:mm", width="medium"),
            "defect_panel_count": st.column_config.NumberColumn("不良数", format="%d", width="small"),
            "defect_rate": st.column_config.NumberColumn("不良率", format="%.3f%%", width="small"),
            "total_panels": st.column_config.NumberColumn("Panel数", format="%d", width="small"),
        },
    )


def _build_code_expander_label(
    mwd_code_data: dict,
    curr_code: str,
    curr_warning: float,
) -> str:
    monthly_df = mwd_code_data.get('monthly') if mwd_code_data else None
    if monthly_df is None or monthly_df.empty:
        return f"{curr_code} | Spec {curr_warning:.2%}"

    code_monthly = monthly_df[monthly_df['defect_desc'] == curr_code].copy()
    if code_monthly.empty or 'defect_rate' not in code_monthly.columns:
        return f"{curr_code} | Spec {curr_warning:.2%}"

    avg_rate = pd.to_numeric(code_monthly['defect_rate'], errors='coerce').dropna().mean()
    if pd.isna(avg_rate):
        return f"{curr_code} | Spec {curr_warning:.2%}"
    return f"{curr_code} | 月均 {avg_rate:.2%} | Spec {curr_warning:.2%}"


def _build_compact_render_payload(
    mwd_code_data: dict,
    lot_data: dict,
    sheet_data: dict,
    mapping_data: Optional[pd.DataFrame],
    curr_group: str,
    curr_code: str,
    curr_warning: float,
    hotspot_scripts: list,
    product_code: Optional[str] = None,
    mapping_layout: Optional[dict] = None,
    expanded: bool = False,
) -> dict:
    """[RenderGate 阶段1] 纯计算：构建单个 Code expander 的全部图表与表格材料。

    禁止任何 st.* 渲染调用；仅读取 st.session_state 中上一次 rerun 已固化的
    Lot 选择（非渲染调用），用于预建 Sheet 分布图。
    """
    key_fragment = _state_key_fragment(curr_group, curr_code)
    target_lot = str(st.session_state.get(f"compact_sheet_lot_{key_fragment}", ""))
    return {
        "label": _build_code_expander_label(mwd_code_data, curr_code, curr_warning),
        "expanded": expanded,
        "curr_group": curr_group,
        "curr_code": curr_code,
        "key_fragment": key_fragment,
        "trend_entries": _prepare_compact_trend_figures(mwd_code_data, curr_code, curr_warning),
        "mapping": _prepare_compact_mapping_payload(
            mapping_data=mapping_data,
            curr_group=curr_group,
            curr_code=curr_code,
            hotspot_scripts=hotspot_scripts,
            product_code=product_code,
            mapping_layout=mapping_layout,
        ),
        "lot": {
            "curr_code": curr_code,
            "fig": _prepare_compact_lot_figure(lot_data, curr_code, curr_warning),
        },
        "sheet": {
            "target_lot": target_lot,
            "fig": _prepare_compact_sheet_figure(sheet_data, target_lot, curr_group, curr_code),
        },
        # 点击 Lot 的 rerun 中，选择事件在渲染阶段才写入 session_state，
        # 预建 Sheet 图可能过期；保留 sheet_data 引用以便按原路径即时重建。
        "sheet_data": sheet_data,
    }


def _render_compact_sheet_from_payload(payload: dict, target_lot: str) -> None:
    """[RenderGate 阶段2] 纯渲染 Sheet 分布区；选择变化导致预建图过期时回退原路径。"""
    curr_group = payload["curr_group"]
    curr_code = payload["curr_code"]
    sheet_info = payload["sheet"]
    if target_lot != sheet_info["target_lot"]:
        # 本次 rerun 的 Lot 点击刚改写 session_state，预建图对应旧 Lot，
        # 按改造前路径即时重建（仅发生在点击/清除选择的那一次 rerun）。
        _render_compact_sheet_chart(payload["sheet_data"], target_lot, curr_group, curr_code)
        return

    if not target_lot:
        st.caption("点击上方 Lot 柱体后显示 Sheet 分布。")
        return

    st.markdown(f"**D. Sheet分布 | Lot {target_lot}**")
    if sheet_info["fig"] is None:
        st.warning(f"未找到 Lot {target_lot} 的 Sheet 级明细。")
        return

    st.plotly_chart(
        sheet_info["fig"],
        use_container_width=True,
        key=f"compact_sheet_chart_{_state_key_fragment(curr_group, curr_code, target_lot)}",
    )


def _render_compact_payload(payload: dict) -> None:
    """[RenderGate 阶段2] 集中渲染：仅执行 st.* 调用，不做任何重计算。"""
    with st.expander(payload["label"], expanded=payload["expanded"]):
        top_trend_col, top_mapping_col = st.columns([1.35, 1.0])
        with top_trend_col:
            _render_compact_trend_entries(payload["trend_entries"], payload["key_fragment"])
        with top_mapping_col:
            _render_compact_mapping_payload(payload["mapping"])

        target_lot = _render_compact_lot_payload(payload["lot"], payload["key_fragment"])
        _render_compact_sheet_from_payload(payload, target_lot)


def render_code_compact_expander(
    mwd_code_data: dict,
    lot_data: dict,
    sheet_data: dict,
    mapping_data: Optional[pd.DataFrame],
    curr_group: str,
    curr_code: str,
    curr_warning: float,
    hotspot_scripts: list,
    product_code: Optional[str] = None,
    mapping_layout: Optional[dict] = None,
    expanded: bool = False,
) -> None:
    payload = _build_compact_render_payload(
        mwd_code_data=mwd_code_data,
        lot_data=lot_data,
        sheet_data=sheet_data,
        mapping_data=mapping_data,
        curr_group=curr_group,
        curr_code=curr_code,
        curr_warning=curr_warning,
        hotspot_scripts=hotspot_scripts,
        product_code=product_code,
        mapping_layout=mapping_layout,
        expanded=expanded,
    )
    _render_compact_payload(payload)


def render_code_compact_expanders(
    selected_groups: list,
    codes_by_group: dict,
    warning_lines: Optional[dict],
    mwd_code_data: dict,
    lot_data: dict,
    sheet_data: dict,
    mapping_data: Optional[pd.DataFrame],
    hotspot_scripts: list,
    product_code: Optional[str] = None,
    mapping_layout: Optional[dict] = None,
) -> None:
    """按 Group 批量渲染每个 Code 的紧凑 expander。

    两阶段渲染：先在 RenderGate 统一 spinner 下构建全部 Code 的图表，
    再按原顺序集中回流渲染（分组标题、分隔线与 expander 次序不变），
    避免图表随计算进度一张一张跳出导致页面抖动卡顿。
    """
    gate = RenderGate()
    staged_groups: List[tuple] = []
    for group_index, curr_group in enumerate(selected_groups):
        group_codes = codes_by_group.get(curr_group, [])
        if not group_codes:
            continue

        for curr_code in group_codes:
            curr_warning = warning_lines.get(curr_code) if warning_lines else None
            if curr_warning is None:
                curr_warning = {'upper': 0.002, 'lower': 0.0}
            gate.stage(
                partial(
                    _build_compact_render_payload,
                    mwd_code_data=mwd_code_data,
                    lot_data=lot_data,
                    sheet_data=sheet_data,
                    mapping_data=mapping_data,
                    curr_group=str(curr_group),
                    curr_code=str(curr_code),
                    curr_warning=float(curr_warning.get('upper', 0.002)),
                    hotspot_scripts=hotspot_scripts,
                    product_code=product_code,
                    mapping_layout=mapping_layout,
                    expanded=True,
                )
            )
        staged_groups.append((group_index, curr_group, len(group_codes)))

    payloads = gate.collect()
    offset = 0
    for group_index, curr_group, code_count in staged_groups:
        if group_index > 0:
            st.divider()
        st.markdown(f"#### {curr_group} · {code_count} Codes")
        for payload in payloads[offset:offset + code_count]:
            _render_compact_payload(payload)
        offset += code_count
