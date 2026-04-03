import streamlit as st
import pandas as pd
import numpy as np
from streamlit_echarts import st_echarts
from pydantic import BaseModel, Field
from app.charts.spc_chart import get_spc_summary_echarts_option
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# --------------------------------------------------------------------------
# 状态模型定义 (Type-Safe Session State)
# --------------------------------------------------------------------------
class SpcFilterState(BaseModel):
    selected_products: list[str] = Field(default_factory=list)
    selected_factories: list[str] = Field(default_factory=list)
    data_type_filter: str = Field(default='SPC', description="监控类型: SPC, CTQ, AOI, ALL")

# 初始化弹窗防死循环锁
if 'spc_summary_lock' not in st.session_state: st.session_state.spc_summary_lock = None
if 'spc_detail_lock' not in st.session_state: st.session_state.spc_detail_lock = None

# [新增] 初始化前端 AgGrid 强刷失忆动态 Key
if 'ag_sum_key' not in st.session_state: st.session_state.ag_sum_key = 0
if 'ag_det_key' not in st.session_state: st.session_state.ag_det_key = 0

# --------------------------------------------------------------------------
# UI 渲染区块
# --------------------------------------------------------------------------
def render_spc_control_panel(available_products: list[str], available_factories: list[str]) -> SpcFilterState:
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
    
    # [核心修改]: 强制将 NaN 和 Inf 替换为 0，逼迫 Echarts 绘制出 0% 的点和柱子
    plot_df = summary_df.copy().fillna(0).replace([np.inf, -np.inf], 0)
    
    echarts_option = get_spc_summary_echarts_option(plot_df)
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

    # ==========================================================
    # [新增前端屏蔽]: 在转置后，把这两行数据从 view_df 中丢弃
    # ==========================================================
    # AOI 场景下这些列不会存在，但仍保留防呆处理
    hidden_metrics = ['OOS+SOOS', 'OOS+SOOS+OOC']
    view_df = view_df.drop(index=[m for m in hidden_metrics if m in view_df.index])

    def safe_format(val, is_rate=False):
        if pd.isna(val): return "/"
        if is_rate: return f"{val * 100:.2f}%"
        return str(int(val))

    # [企业级优化] 根据数据类型动态调整比率行
    is_aoi = data_type_filter == 'AOI'
    if is_aoi:
        rate_rows = ['OOS', 'OOC', 'OOS+OOC']
    else:
        rate_rows = ['OOS', 'SOOS', 'OOC', 'OOS+SOOS', 'OOS+SOOS+OOC']
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
        rate_cols = ['OOS', 'OOC', 'OOS+OOC']
        ordered_metrics = ['抽检数', 'OOS片数', 'OOC片数', 'OOS', 'OOC']
    else:
        rate_cols = ['OOS', 'SOOS', 'OOC', 'OOS+SOOS', 'OOS+SOOS+OOC']
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
    query_params = st.query_params
    is_admin = query_params.get("admin") == "true"
    force_compliant = not is_admin 
    
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
                    force_compliant=force_compliant
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