import streamlit as st
import pandas as pd
import numpy as np
import logging
from streamlit_echarts import st_echarts
from pydantic import BaseModel, Field
from app.charts.spc_chart import get_spc_summary_echarts_option
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode

# --------------------------------------------------------------------------
# 状态模型定义 (Type-Safe Session State)
# --------------------------------------------------------------------------
class SpcFilterState(BaseModel):
    selected_products: list[str] = Field(default_factory=list)
    selected_factories: list[str] = Field(default_factory=list)
    data_type_filter: str = Field(default='SPC', description="监控类型: SPC, CTQ, AOI, ALL")

# --------------------------------------------------------------------------
# UI 渲染区块
# --------------------------------------------------------------------------
def render_spc_control_panel(available_products: list[str], available_factories: list[str]) -> SpcFilterState:
    """
    渲染 SPC 控制面板
    
    [注意] 数据修饰配置已移至主页面使用 compliance_control 模块渲染
    """
    col1, col2, col3 = st.columns(3)
    with col1:
        # [修改] 将基准日期替换为监控类型筛选
        data_type_options = ['SPC', 'CTQ', 'AOI', 'ALL']
        data_type = st.selectbox(
            "监控类型", 
            options=data_type_options, 
            index=0,  # 默认选中 SPC
            help="选择要监控的数据类型: SPC(常规SPC参数), CTQ(关键质量参数), AOI(外观检测参数), ALL(全部)"
        )
    with col2:
        prods = st.multiselect("产品型号", options=available_products, default=available_products)
    with col3:
        facs = st.multiselect("厂别", options=available_factories, default=available_factories)
    
    # [注意] 精细化数据修饰控制面板已移至主页面统一渲染
    # 使用 app.components.compliance_control.render_compliance_control_panel()
    
    return SpcFilterState(selected_products=prods, selected_factories=facs, data_type_filter=data_type)

# =========================================================================
# 大盘汇总图 (Chart)
# =========================================================================
def render_spc_summary_chart(summary_df: pd.DataFrame, data_type_filter: str = 'SPC'):
    if summary_df.empty:
        st.warning("暂无全局汇总数据")
        return
    # [修改] 标题根据监控类型动态显示
    st.markdown(f"#### 📊 {data_type_filter}报警率汇总图")
    
    # [核心修复]: 避开 Categorical 类型强校验引发的 fillna 崩溃
    plot_df = summary_df.copy()
    
    # 1. 解除类别锁定：将 Category 类型的列转回普通字符串，防止填 0 时报错
    if 'time_group' in plot_df.columns:
        plot_df['time_group'] = plot_df['time_group'].astype(str)
        
    # 2. 强制将 NaN 和 Inf 替换为 0，逼迫 Echarts 绘制出 0% 的点和柱子
    plot_df = plot_df.fillna(0).replace([np.inf, -np.inf], 0)
    
    from app.charts.spc_chart import get_spc_summary_echarts_option
    echarts_option = get_spc_summary_echarts_option(plot_df)
    
    from streamlit_echarts import st_echarts
    st_echarts(options=echarts_option, height="450px")

# =========================================================================
# 大盘汇总表 (Table) - 极速直接下钻版
# =========================================================================
def render_spc_summary_table(summary_df: pd.DataFrame, data_type_filter: str = 'SPC'):
    # [安全初始化] 确保 session_state 变量已初始化
    if 'ag_sum_key' not in st.session_state:
        st.session_state.ag_sum_key = 0
    if 'spc_summary_lock' not in st.session_state:
        st.session_state.spc_summary_lock = None
        
    if summary_df.empty:
        return
    
    # [修改] 标题根据监控类型动态显示
    st.markdown(f"#### {data_type_filter}报警汇总表")
    
    view_df = summary_df.copy().set_index('time_group').T

    # [注：复合报警类型已移除]

    def safe_format(val, is_rate=False):
        if pd.isna(val): return "/"
        if is_rate: return f"{val * 100:.2f}%"
        return str(int(val))

    # [企业级优化] 根据数据类型动态调整比率行
    is_aoi = data_type_filter == 'AOI'
    if is_aoi:
        rate_rows = ['OOS', 'OOC']
    else:
        rate_rows = ['OOS', 'SOOS', 'OOC']
    for row_idx in view_df.index:
        is_rate = row_idx in rate_rows
        view_df.loc[row_idx] = view_df.loc[row_idx].apply(lambda x: safe_format(x, is_rate))
    
    view_df = view_df.reset_index().rename(columns={'index': '报警类型'})

    gb = GridOptionsBuilder.from_dataframe(view_df)
    # 开启单选模式，支持再次点击取消选中
    gb.configure_selection(selection_mode="single", use_checkbox=False)
    gb.configure_column("报警类型", pinned="left", width=140, cellStyle={'fontWeight': 'bold', 'backgroundColor': '#f8f9fa'})

    time_cols = [col for col in view_df.columns if col != '报警类型']
    for col in time_cols:
        bg_color = 'rgba(230, 240, 255, 0.6)' if 'M' in col else 'rgba(255, 245, 230, 0.6)' if 'W' in col else 'transparent'
        gb.configure_column(col, cellStyle={
            'backgroundColor': bg_color, 'color': '#1e88e5', 'cursor': 'pointer', 'textDecoration': 'underline'
        })

    grid_response = AgGrid(
        view_df,
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme='streamlit',
        height=260,
        # [核心修改 1] 绑定汇总表的动态 Key
        key=f"ag_summary_table_{st.session_state.ag_sum_key}" 
    )

    selected_rows = grid_response.get("selected_rows")
    if selected_rows is not None and len(selected_rows) > 0:
        row_data = selected_rows.iloc[0].to_dict() if isinstance(selected_rows, pd.DataFrame) else selected_rows[0]
        defect = row_data.get("报警类型")
        
        if defect in rate_rows or '片数' in defect: # type: ignore
            if st.session_state.spc_summary_lock != defect:
                st.session_state.spc_summary_lock = defect
                # [核心修改 2] 传入 source="summary"
                show_drilldown_modal("ALL", "ALL", defect, time_cols, data_type_filter, source="summary")
    else:
        st.session_state.spc_summary_lock = None

def render_spc_summary_section(summary_df: pd.DataFrame, data_type_filter: str = 'SPC'):
    render_spc_summary_chart(summary_df, data_type_filter)
    # st.divider()
    render_spc_summary_table(summary_df, data_type_filter)

# =========================================================================
# 明细多维下钻表 - 极速直接下钻版
# =========================================================================
def render_spc_detail_section(detail_df: pd.DataFrame, filter_state: SpcFilterState):
    # [安全初始化] 确保 session_state 变量已初始化
    if 'ag_det_key' not in st.session_state:
        st.session_state.ag_det_key = 0
    if 'spc_detail_lock' not in st.session_state:
        st.session_state.spc_detail_lock = None
        
    # [修改] 标题根据监控类型动态显示
    st.markdown(f"#### By产品-By工厂{filter_state.data_type_filter}报警明细")
    if detail_df.empty:
        st.info("所选范围内无明细数据。")
        return
        
    filtered_df = detail_df[
        (detail_df['prod_code'].isin(filter_state.selected_products)) & 
        (detail_df['factory'].isin(filter_state.selected_factories))
    ]
    
    view_df = filtered_df.copy()
    
    # [企业级优化] 根据数据类型动态调整显示的列
    # AOI 场景不包含 SOOS 相关列
    is_aoi = filter_state.data_type_filter == 'AOI'
    if is_aoi:
        rate_cols = ['OOS', 'OOC']
        ordered_metrics = ['抽检数', 'OOS片数', 'OOC片数', 'OOS', 'OOC']
    else:
        rate_cols = ['OOS', 'SOOS', 'OOC']
        ordered_metrics = ['抽检数', 'OOS片数', 'SOOS片数', 'OOC片数', 'OOS', 'SOOS', 'OOC']
    
    for col in view_df.columns:
        if col in rate_cols:
            view_df[col] = view_df[col].apply(lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "/")
        elif '片' in col or '量' in col:
            view_df[col] = view_df[col].apply(lambda x: str(int(x)) if pd.notna(x) else "/")

    ordered_time_groups = detail_df['time_group'].unique().tolist()
    view_df['time_group'] = pd.Categorical(view_df['time_group'], categories=ordered_time_groups, ordered=True)

    pivot_df = view_df.pivot_table(index=['prod_code', 'factory'], columns=['time_group'], values=ordered_metrics, aggfunc=lambda x: x.iloc[0], observed=False)
    stacked_df = pivot_df.stack(level=0, dropna=False)
    stacked_df.index.names = ['品名', '工厂', '报警类型']
    stacked_df = stacked_df.reindex(ordered_metrics, level='报警类型')

    flat_df = stacked_df.reset_index()
    time_cols = [col for col in flat_df.columns if col not in ['品名', '工厂', '报警类型']]
    
    is_rate_row = flat_df['报警类型'].isin(rate_cols)
    for col in time_cols:
        flat_df.loc[is_rate_row, col] = flat_df.loc[is_rate_row, col].fillna("0.00%")
        flat_df.loc[~is_rate_row, col] = flat_df.loc[~is_rate_row, col].fillna("0")
    flat_df.columns = flat_df.columns.astype(str)

    gb = GridOptionsBuilder.from_dataframe(flat_df)
    gb.configure_selection(selection_mode="single", use_checkbox=False)
    gb.configure_column("品名", rowGroup=True, hide=True)
    gb.configure_column("工厂", rowGroup=True, hide=True)
    gb.configure_column("报警类型", pinned="left", width=130, cellStyle={'fontWeight': 'bold', 'backgroundColor': '#f8f9fa'})

    for col in time_cols:
        bg_color = 'rgba(230, 240, 255, 0.6)' if 'M' in col else 'rgba(255, 245, 230, 0.6)' if 'W' in col else 'transparent'
        gb.configure_column(col, cellStyle={'backgroundColor': bg_color, 'color': '#1e88e5', 'cursor': 'pointer', 'textDecoration': 'underline'})

    grid_options = gb.build()
    grid_options['groupDefaultExpanded'] = -1 
    grid_options['autoGroupColumnDef'] = {'headerName': '🏭 产品/工厂', 'width': 150, 'pinned': 'left', 'cellRendererParams': {'suppressCount': True}}

    grid_response = AgGrid(
        flat_df,
        gridOptions=grid_options,
        enable_enterprise_modules=True,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme='streamlit',
        height=500,
        # [核心修改 1] 绑定明细表的动态 Key
        key=f"ag_detail_table_{st.session_state.ag_det_key}" 
    )
    
    selected_rows = grid_response.get("selected_rows")
    if selected_rows is not None and len(selected_rows) > 0:
        row_data = selected_rows.iloc[0].to_dict() if isinstance(selected_rows, pd.DataFrame) else selected_rows[0]
        if "报警类型" in row_data and row_data["报警类型"] in rate_cols + ['OOS片数', 'SOOS片数', 'OOC片数']:
            prod = row_data.get("品名", filter_state.selected_products[0])
            factory = row_data.get("工厂", "Unknown")
            defect = row_data.get("报警类型")
            
            current_lock = f"{prod}_{factory}_{defect}"
            if st.session_state.spc_detail_lock != current_lock:
                st.session_state.spc_detail_lock = current_lock
                # [核心修改 2] 传入 source="detail"
                show_drilldown_modal(prod, factory, defect, time_cols, filter_state.data_type_filter, source="detail")
    else:
        st.session_state.spc_detail_lock = None

# =========================================================================
# 悬浮弹窗组件 (注入 CSS 伪装退出魔法)
# =========================================================================
@st.dialog(" ", width="large") # 标题强制留空，为自定义 Header 腾出空间
def show_drilldown_modal(prod: str, factory: str, defect_type: str, available_times: list, data_type_filter: str = 'SPC', source: str = "summary"):
    # 1. 注入 CSS 隐藏原生按钮，调整间距
    st.markdown(
        """
        <style>
        [data-testid="stDialog"] button[aria-label="Close"] { display: none !important; }
        [data-testid="stDialog"] div[data-testid="stVerticalBlock"] { gap: 0.5rem; }
        </style>
        """,
        unsafe_allow_html=True
    )

    # 2. 渲染自定义 Header (伪装的退出按钮)
    header_col1, header_col2 = st.columns([10, 1])
    with header_col1:
        st.markdown(f"### {data_type_filter}报警明细 - {defect_type}")
    with header_col2:
        if st.button("✖", key=f"close_btn_{prod}_{factory}_{defect_type}", use_container_width=True, help="关闭并释放图表状态"):
            # 根据调用方来源，精确释放对应的锁和重置对应的 Key
            if source == "summary":
                st.session_state.spc_summary_lock = None
                st.session_state.ag_sum_key += 1
            else:
                st.session_state.spc_detail_lock = None
                st.session_state.ag_det_key += 1
            st.rerun() # 立刻重载，强行刷新前端画布

    st.divider()
    
    # 3. 业务数据调取与渲染逻辑
    # [新增] 从配置文件获取当前组合的修饰配置
    from app.components.compliance_config import get_compliance_config
    force_compliant = get_compliance_config(data_type_filter, prod, factory)
    
    selected_time = st.segmented_control(
        "选择追溯时间段:",
        options=available_times,
        default=available_times[-1] if available_times else None,
        key=f"drill_time_{prod}_{factory}_{defect_type}" # 加上 defect 保证 Key 的绝对唯一
    )
    
    if selected_time:
        with st.spinner(f"正在从底层快照极速调取 {selected_time} 的 {defect_type} 明细..."):
            try:
                from src.spc_domain.application.spc_service import SpcAnalysisService
                from src.spc_domain.infrastructure.data_loader import SpcQueryConfig
                from shared_kernel.infrastructure.db_handler import DatabaseManager

                start_dt, end_dt = SpcAnalysisService.get_time_window()
                core_defect_type = defect_type.replace("片数", "").strip()

                query_config = SpcQueryConfig(
                    prod_code=prod,
                    start_date=start_dt.strftime("%Y-%m-%d"),
                    end_date=end_dt.strftime("%Y-%m-%d")
                )
                db_manager = DatabaseManager()

                real_df = SpcAnalysisService.get_spc_defect_details(
                    _db_manager=db_manager,
                    query_config_json=query_config.model_dump_json(),
                    time_group=selected_time,
                    defect_type=core_defect_type,
                    time_type='MIXED',
                    force_compliant=force_compliant,  # [核心修复] 传递当前组合修饰配置
                    data_type_filter=data_type_filter  # ✅ 传入监控类型
                )

                if real_df.empty:
                    st.info(f"💡 {selected_time} 期间，未追溯到具体的 **{defect_type}** 拦截明细。")
                else:
                    if 'factory' in real_df.columns and factory != "ALL":
                        real_df = real_df[real_df['factory'] == factory]

                    if real_df.empty:
                        st.info(f"💡 该时段内，属于 **{factory}** 工厂的 {defect_type} 明细为空。")
                    else:
                        st.success(f"✅ 钻取成功！共捕获 **{len(real_df)}** 片底层追溯数据。")
                        st.dataframe(real_df, use_container_width=True, hide_index=True, height=400)
                        
            except Exception as e:
                import traceback
                st.error(f"❌ 钻取请求失败: {str(e)}")
                with st.expander("查看详细错误日志"):
                    st.code(traceback.format_exc())

# =========================================================================
# 数据联动处理引擎 (Data Binding Engine)
# =========================================================================
def filter_and_rollup_spc_data(
    detail_df: pd.DataFrame, 
    global_summary_df: pd.DataFrame, 
    station_detail_df: pd.DataFrame,
    filter_state: SpcFilterState
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    [前端动态联动核心] 
    根据用户的下拉框选择过滤明细数据，并动态向上卷起重算大盘汇总数据。
    彻底解耦：在不重新请求后端的情况下，实现图表和表格的实时物理联动。
    """
    if detail_df.empty:
        return global_summary_df, detail_df

    # 1. 过滤明细表
    filtered_detail_df = detail_df[
        (detail_df['prod_code'].isin(filter_state.selected_products)) & 
        (detail_df['factory'].isin(filter_state.selected_factories))
    ].copy()
    
    # 2. 动态重算汇总表 (Roll-up)
    if not filtered_detail_df.empty and not global_summary_df.empty:
        sum_cols = ['抽检数', 'OOS片数', 'SOOS片数', 'OOC片数']
        sum_cols = [c for c in sum_cols if c in filtered_detail_df.columns]
        
        # 按 time_group 聚合绝对数值
        agg_df = filtered_detail_df.groupby('time_group', as_index=False)[sum_cols].sum()
        
        # 重新计算比率
        if 'OOS片数' in agg_df.columns:
            agg_df['OOS'] = agg_df['OOS片数'] / agg_df['抽检数']
        if 'OOC片数' in agg_df.columns:
            agg_df['OOC'] = agg_df['OOC片数'] / agg_df['抽检数']
        if 'SOOS片数' in agg_df.columns:  # SPC/CTQ 场景
            agg_df['SOOS'] = agg_df['SOOS片数'] / agg_df['抽检数']
            
        # 强制对齐原始时间轴的排序
        ordered_times = global_summary_df['time_group'].tolist() if 'time_group' in global_summary_df.columns else []
        if ordered_times:
            agg_df['time_group'] = pd.Categorical(agg_df['time_group'], categories=ordered_times, ordered=True)
            
        filtered_summary_df = agg_df.sort_values('time_group').reset_index(drop=True)
    else:
        # 过滤后没数据，返回空壳
        filtered_summary_df = pd.DataFrame(columns=global_summary_df.columns)
        
    # =========================================================
    # [重构] 处理 Top 10 站点数据，仅做物理过滤，将完整维度交给 UI 降维
    # =========================================================
    if station_detail_df is not None and not station_detail_df.empty:
        # 严格响应前端【产品】与【厂别】的下拉框筛选联动
        filtered_station_df = station_detail_df[
            (station_detail_df['prod_code'].isin(filter_state.selected_products)) & 
            (station_detail_df['factory'].isin(filter_state.selected_factories))
        ].copy()
    else:
        filtered_station_df = pd.DataFrame()
        
    # [恢复极简] 仅返回 3 个过滤后的数据源
    return filtered_summary_df, filtered_detail_df, filtered_station_df


# =========================================================================
# 🏆 Top 10 异常站点分析模块 (Top 10 Station Section)
# =========================================================================
# =========================================================================
# 🏆 Top 10 异常站点分析模块 (Top 10 Station Section)
# =========================================================================
def render_station_top10_section(filtered_station_df: pd.DataFrame):
    """渲染 Top 10 异常站点图表、汇总(转置)与明细表(产品折叠)"""
    
    if 'ag_top10_sum_key' not in st.session_state: 
        st.session_state.ag_top10_sum_key = 0
    if 'ag_top10_det_key' not in st.session_state: 
        st.session_state.ag_top10_det_key = 0

    if filtered_station_df.empty:
        st.success("🎉 当前监控下无任何超规报警站点！")
        return

    base_cols = ['OOS', 'SOOS', 'OOC']
    actual_count_cols = [f"{c}片数" for c in base_cols if f"{c}片数" in filtered_station_df.columns]
    
    if not actual_count_cols:
         st.success("🎉 当前监控下，无有效的报警数据列！")
         return

    # [核心修复 1]：动态识别并纳入“抽检数”进行多维聚合
    has_sample_size = '抽检数' in filtered_station_df.columns
    agg_cols = actual_count_cols + (['抽检数'] if has_sample_size else [])

    # --- 核心视图逻辑：内部提炼 Top 10 站点 ---
    agg_station_df = filtered_station_df.groupby('step_id', as_index=False)[agg_cols].sum()
    
    # [核心修复 2]：计算异常总数 (Total) 仅用于找出 Top 10 瓶颈，绝对不能把抽检数加进去！
    agg_station_df['Total'] = agg_station_df[actual_count_cols].sum(axis=1)
    
    top_station_df = agg_station_df[agg_station_df['Total'] > 0].sort_values('Total', ascending=False).head(10)
    
    if top_station_df.empty:
        st.success("🎉 当前监控下无任何超规报警站点！")
        return
        
    top10_stations_list = top_station_df['step_id'].tolist()

    # ==========================================
    # 0. Echarts 垂直堆叠柱状图
    # ==========================================
    st.markdown("#### 🏆 Top 10 异常站点分布图")
    
    chart_df = top_station_df.copy()
    x_data = chart_df['step_id'].tolist()
    
    option = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "legend": {"data": ["OOC片数", "SOOS片数", "OOS片数"], "bottom": 0},
        "grid": {"left": "3%", "right": "3%", "bottom": "15%", "containLabel": True},
        "xAxis": {
            "type": "category",
            "data": x_data,
            "axisLabel": {"interval": 0, "fontWeight": "bold"}
        },
        "yAxis": {"type": "value", "name": "报警总片数"},
        "series": []
    }

    if 'OOC片数' in actual_count_cols:
        option["series"].append({
            "name": "OOC片数", "type": "bar", "stack": "总量", "barMaxWidth": 80,
            "itemStyle": {"color": "#F9D976"},
            "data": chart_df['OOC片数'].tolist()
        })
    if 'SOOS片数' in actual_count_cols:
        option["series"].append({
            "name": "SOOS片数", "type": "bar", "stack": "总量", "barMaxWidth": 80,
            "itemStyle": {"color": "#81D8D0"},
            "data": chart_df['SOOS片数'].tolist()
        })
    if 'OOS片数' in actual_count_cols:
        option["series"].append({
            "name": "OOS片数", "type": "bar", "stack": "总量", "barMaxWidth": 80,
            "itemStyle": {"color": "#7B9CE1"},
            "data": chart_df['OOS片数'].tolist()
        })

    st_echarts(option, height="450px")
    st.divider()

    # ==========================================
    # 1. 汇总表 (Summary Table): 转置矩阵
    # ==========================================
    st.markdown("#### 📊 Top 10 异常站点汇总表")
    sum_view = top_station_df.copy()
    
    # [核心修复 3]：表格第一行展示真正的“抽检数”
    ordered_metrics = ['抽检数'] if has_sample_size else ['报警总数']
    
    for c in base_cols:
        col_name = f"{c}片数"
        if col_name in actual_count_cols:
            # [核心修复 4]：使用抽检数作为分母，计算真正的报警率(不良率)
            denominator = sum_view['抽检数'] if has_sample_size else sum_view['Total']
            ratio = np.where(denominator > 0, sum_view[col_name] / denominator, 0)
            
            sum_view[f'{c}占比'] = [f"{x * 100:.2f}%" for x in ratio]
            sum_view[col_name] = sum_view[col_name].astype(str)
            ordered_metrics.extend([col_name, f"{c}占比"])
            
    if has_sample_size:
        sum_view['抽检数'] = sum_view['抽检数'].astype(int).astype(str)
    else:
        sum_view['报警总数'] = sum_view['Total'].astype(str)
        
    sum_view = sum_view.set_index('step_id')[ordered_metrics]
    view_df = sum_view.T.reset_index().rename(columns={'index': '统计维度'})
    
    gb_sum = GridOptionsBuilder.from_dataframe(view_df)
    gb_sum.configure_selection(selection_mode="single", use_checkbox=False)
    gb_sum.configure_column("统计维度", pinned="left", width=95, cellStyle={'fontWeight': 'bold', 'backgroundColor': '#f8f9fa'})
    
    for col in top10_stations_list:
        gb_sum.configure_column(col, cellStyle={'backgroundColor': 'transparent'})
        
    grid_options_sum = gb_sum.build()
    grid_options_sum['getRowStyle'] = JsCode("""
    function(params) {
        if (params.data && params.data['统计维度'] && params.data['统计维度'].includes('占比')) {
            return {'backgroundColor': 'rgba(230, 240, 255, 0.4)'};
        }
        return null;
    }
    """)
    
    AgGrid(
        view_df,
        gridOptions=grid_options_sum,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme='streamlit',
        height=280,
        allow_unsafe_jscode=True,
        key=f"ag_top10_summary_{st.session_state.ag_top10_sum_key}"
    )

    st.divider()

    # ==========================================
    # 2. 明细表 (Detail Table): By 产品折叠 + 占比补全
    # ==========================================
    st.markdown("#### 📑 By产品 - Top 10 站点报警明细")
    
    filtered_det = filtered_station_df[filtered_station_df['step_id'].isin(top10_stations_list)].copy()
    
    # 细粒度聚合，同样要带上抽检数
    agg_det = filtered_det.groupby(['prod_code', 'step_id'])[agg_cols].sum()
    
    ordered_metrics_det = ['抽检数'] if has_sample_size else ['报警总数']
    
    for c in base_cols:
        col_name = f"{c}片数"
        if col_name in actual_count_cols:
            if has_sample_size:
                denominator = agg_det['抽检数']
            else:
                denominator = agg_det[actual_count_cols].sum(axis=1)
                
            ratio = np.where(denominator > 0, agg_det[col_name] / denominator, 0)
            agg_det[f'{c}占比'] = ratio 
            ordered_metrics_det.extend([col_name, f"{c}占比"])
            
    if not has_sample_size:
        agg_det['报警总数'] = agg_det[actual_count_cols].sum(axis=1)
    
    pivot_df = agg_det.unstack(level='step_id', fill_value=0)
    stacked_df = pivot_df.stack(level=0, dropna=False)
    stacked_df.index.names = ['品名', '报警类型']
    stacked_df = stacked_df.reindex(ordered_metrics_det, level='报警类型')
    
    flat_df = stacked_df.reset_index()
    available_stations = [s for s in top10_stations_list if s in flat_df.columns]
    
    for col in available_stations:
        is_ratio = flat_df['报警类型'].str.contains('占比')
        flat_df[col] = np.where(
            is_ratio,
            (flat_df[col].fillna(0).astype(float) * 100).map("{:.2f}%".format),
            flat_df[col].fillna(0).astype(int).astype(str)
        )

    flat_df = flat_df[['品名', '报警类型'] + available_stations]
    
    gb_det = GridOptionsBuilder.from_dataframe(flat_df)
    gb_det.configure_selection(selection_mode="single", use_checkbox=False)
    gb_det.configure_column("品名", rowGroup=True, hide=True)
    gb_det.configure_column("报警类型", pinned="left", width=95, cellStyle={'fontWeight': 'bold', 'backgroundColor': '#f8f9fa'})
    
    grid_options_det = gb_det.build()
    grid_options_det['groupDefaultExpanded'] = -1 
    grid_options_det['autoGroupColumnDef'] = {
        'headerName': '📦 产品型号', 
        'width': 130, 
        'pinned': 'left', 
        'cellRendererParams': {'suppressCount': True}
    }
    
    grid_options_det['getRowStyle'] = JsCode("""
    function(params) {
        if (params.data && params.data['报警类型'] && params.data['报警类型'].includes('占比')) {
            return {'backgroundColor': 'rgba(230, 240, 255, 0.4)'};
        }
        return null;
    }
    """)
    
    AgGrid(
        flat_df,
        gridOptions=grid_options_det,
        enable_enterprise_modules=True,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme='streamlit',
        height=380,
        allow_unsafe_jscode=True,
        key=f"ag_top10_detail_{st.session_state.ag_top10_det_key}"
    )