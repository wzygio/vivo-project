import streamlit as st
import pandas as pd
import numpy as np
import logging
from functools import partial
from streamlit_echarts import st_echarts

# [Phase 1] 调试追踪专用 Logger
trace_logger = logging.getLogger("trace")
from pydantic import BaseModel, Field
from app.charts.inline_domain.monitor_chart import get_spc_summary_echarts_option
from app.manager.compliance_manager import get_compliance_file_signature
from app.manager.render_gate import RenderGate
from app.utils.step_labels import format_step_label
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode

from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.composition import build_monitor_repository
from src.inline_domain.core.monitor.monitor_calculator import sanitize_to_compliant
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

ALARM_DETAIL_MONITOR_TYPES = ["SPC", "CTQ", "AOI", "报废"]
ALARM_DETAIL_STATUS_OPTIONS = ["OOC", "OOS"]


def _get_compliance_file_signature() -> str:
    """Return the active compliance workbook cache signature."""
    return get_compliance_file_signature()


# --------------------------------------------------------------------------
# 状态模型定义 (Type-Safe Session State)
# --------------------------------------------------------------------------
class MonitorFilterState(BaseModel):
    selected_products: list[str] = Field(default_factory=list)
    selected_factories: list[str] = Field(default_factory=list)
    data_type_filter: str = Field(default='ALL', description="监控类型: ALL, SPC, CTQ, AOI, 报废")

# --------------------------------------------------------------------------
# UI 渲染区块
# --------------------------------------------------------------------------
def render_monitor_control_panel(available_products: list[str], available_factories: list[str]) -> MonitorFilterState:
    """
    渲染 SPC 控制面板
    
    [注意] 数据修饰配置已移至主页面使用 compliance_control 模块渲染
    """
    col1, col2, col3 = st.columns(3)
    with col1:
        # [修改] 将基准日期替换为监控类型筛选
        data_type_options = ['ALL', 'SPC', 'CTQ', 'AOI', '报废']
        data_type = st.selectbox(
            "监控类型", 
            options=data_type_options, 
            index=0,  # 默认选中 ALL，避免切换时重复加载
            key="spc_data_type_filter",
            help="选择要监控的数据类型: SPC(常规SPC参数), CTQ(关键质量参数), AOI(外观检测参数), 报废(报废数据), ALL(全部)"
        )
    with col2:
        prods = st.multiselect("产品型号", options=available_products, default=available_products)
    with col3:
        facs = st.multiselect("厂别", options=available_factories, default=available_factories)
    
    # [注意] 精细化数据修饰控制面板已移至主页面统一渲染
    # 使用 app.components.compliance_control.render_compliance_control_panel()
    
    return MonitorFilterState(selected_products=prods, selected_factories=facs, data_type_filter=data_type)


# --------------------------------------------------------------------------
# 「查询」门控（2026-09-03 需求：页面打开不自动全量加载）
# --------------------------------------------------------------------------
# 已提交筛选签名的 session_state 键；perform_hard_reset 阶段 4 与
# _refresh_data_callback（app/components/page_header.py）负责清除。
MONITOR_QUERY_SIGNATURE_KEY = "monitor_query_signature"


def monitor_filter_signature(filter_state: MonitorFilterState) -> tuple:
    """当前筛选条件的确定性签名：任一维度变化即产生新签名。"""
    return (
        str(filter_state.data_type_filter),
        tuple(sorted(str(prod) for prod in filter_state.selected_products)),
        tuple(sorted(str(factory) for factory in filter_state.selected_factories)),
    )


def _submit_monitor_query(signature: tuple) -> None:
    st.session_state[MONITOR_QUERY_SIGNATURE_KEY] = signature


def render_monitor_query_gate(filter_state: MonitorFilterState) -> bool:
    """渲染「查询」主按钮并判定当前筛选签名是否已提交。

    未提交（含筛选变更后签名过期）时静默返回 False——不渲染任何说明文案，
    门控语义由「查询」按钮本身承担（2026-09-03 UI 优化轮次：本页渲染面
    禁用 st.info 提醒条）；调用方不得执行签名预算与数据加载，避免展示与
    筛选不一致的旧数据（仿 Q-Time 页签名过期模式）。已提交状态普通 rerun 保持。
    """
    signature = monitor_filter_signature(filter_state)
    st.button(
        "🔍 查询",
        type="primary",
        key="btn_monitor_query_submit",
        on_click=_submit_monitor_query,
        args=(signature,),
        help="按当前筛选条件加载超规片自动预警数据（签名预算 + 全量监控数据）。",
    )
    stored = st.session_state.get(MONITOR_QUERY_SIGNATURE_KEY)
    return stored == signature

# =========================================================================
# 大盘汇总图 (Chart)
# =========================================================================
def _build_monitor_summary_chart_payload(summary_df: pd.DataFrame, data_type_filter: str) -> dict[str, object]:
    """[RenderGate 阶段1] 纯计算：整形数据并组装 Echarts option，禁止触碰 st.*。"""
    # [修改] 标题根据监控类型动态显示
    chart_title = f"{data_type_filter}报废率汇总图" if data_type_filter == '报废' else f"{data_type_filter}报警率汇总图"

    # [核心修复]: 避开 Categorical 类型强校验引发的 fillna 崩溃
    plot_df = summary_df.copy()

    # 1. 解除类别锁定：将 Category 类型的列转回普通字符串，防止填 0 时报错
    if 'time_group' in plot_df.columns:
        plot_df['time_group'] = plot_df['time_group'].astype(str)

    # 2. 强制将 NaN 和 Inf 替换为 0，逼迫 Echarts 绘制出 0% 的点和柱子
    plot_df = plot_df.fillna(0).replace([np.inf, -np.inf], 0)

    echarts_option = get_spc_summary_echarts_option(plot_df)
    return {"chart_title": chart_title, "echarts_option": echarts_option}


def _render_monitor_summary_chart_payload(payload: dict[str, object]) -> None:
    """[RenderGate 阶段2] 集中渲染：仅执行 st.* 调用，不做任何重计算。"""
    st.markdown(f"#### 📊 {payload['chart_title']}")
    st_echarts(options=payload["echarts_option"], height="450px")


def render_monitor_summary_chart(summary_df: pd.DataFrame, data_type_filter: str = 'SPC'):
    if summary_df.empty:
        st.warning("暂无全局汇总数据")
        return

    # 两阶段渲染：先在 RenderGate 统一 spinner 下完成 option 组装，再集中回流渲染，
    # 避免重计算夹在 st.* 推送之间造成图表阶段式跳出。
    gate = RenderGate()
    gate.stage(partial(_build_monitor_summary_chart_payload, summary_df, data_type_filter))
    for payload in gate.collect():
        _render_monitor_summary_chart_payload(payload)

# =========================================================================
# 大盘汇总表 (Table) - 极速直接下钻版
# =========================================================================
def render_monitor_summary_table(summary_df: pd.DataFrame, data_type_filter: str = 'SPC', is_admin: bool = False):
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
    is_scrap = data_type_filter == '报废'
    if is_scrap:
        rate_rows = ['OOC']
    elif is_aoi:
        rate_rows = ['OOS', 'OOC']
    else:
        rate_rows = ['OOS', 'SOOS', 'OOC']
    for row_idx in view_df.index:
        is_rate = row_idx in rate_rows
        view_df.loc[row_idx] = view_df.loc[row_idx].apply(lambda x: safe_format(x, is_rate))
    
    view_df = view_df.reset_index().rename(columns={'index': '报警类型'})
    
    # [报废类型] 前端文案替换：OOC → 报废
    if is_scrap:
        view_df['报警类型'] = view_df['报警类型'].replace({
            'OOC片数': '报废片数',
            'OOC': '报废率'
        })

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
            # 🛑 [核心修改]: 在弹出弹窗前，判断是否为管理员
            if is_admin:
                if st.session_state.spc_summary_lock != defect:
                    st.session_state.spc_summary_lock = defect
                    show_drilldown_modal("ALL", "ALL", defect, time_cols, data_type_filter, source="summary")
            else:
                 # 重置选中状态，避免一直显示警告
                 st.session_state.spc_summary_lock = None 
    else:
        st.session_state.spc_summary_lock = None

def render_monitor_summary_section(summary_df: pd.DataFrame, data_type_filter: str = 'SPC', is_admin: bool = False):
    render_monitor_summary_chart(summary_df, data_type_filter)
    # st.divider()
    render_monitor_summary_table(summary_df, data_type_filter, is_admin)

# =========================================================================
# 明细多维下钻表 - 极速直接下钻版
# =========================================================================
def render_monitor_detail_section(detail_df: pd.DataFrame, filter_state: MonitorFilterState, is_admin: bool = False):
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
    # AOI 场景不包含 SOOS 相关列；报废场景只保留 OOC（伪装）
    is_aoi = filter_state.data_type_filter == 'AOI'
    is_scrap = filter_state.data_type_filter == '报废'
    if is_scrap:
        rate_cols = ['OOC']
        ordered_metrics = ['抽检数', 'OOC片数', 'OOC']
    elif is_aoi:
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
    
    # [报废类型] 前端文案替换
    if is_scrap:
        flat_df['报警类型'] = flat_df['报警类型'].replace({
            'OOC片数': '报废片数',
            'OOC': '报废率'
        })
    
    time_cols = [col for col in flat_df.columns if col not in ['品名', '工厂', '报警类型']]
    
    is_rate_row = flat_df['报警类型'].isin(rate_cols) if not is_scrap else flat_df['报警类型'] == '报废率'
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
            
            # 🛑 [核心修改]: 在弹出弹窗前，判断是否为管理员
            if is_admin:
                if st.session_state.spc_detail_lock != current_lock:
                    st.session_state.spc_detail_lock = current_lock
                    show_drilldown_modal(prod, factory, defect, time_cols, filter_state.data_type_filter, source="detail")
            else:
                st.session_state.spc_detail_lock = None
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
        if st.button("✖", key=f"close_btn_{prod}_{factory}_{defect_type}", width="stretch", help="关闭并释放图表状态"):
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
    selected_time = "ALL"

    if selected_time:
        with st.spinner(f"正在从底层快照极速提取近 3 个月内所有的 {defect_type} 物理明细..."):
            try:
                start_dt, end_dt = MonitorAnalysisService.get_time_window()
                core_defect_type = defect_type.replace("片数", "").strip()

                query_config = SpcQueryConfig(
                    prod_code=prod,
                    start_date=start_dt.strftime("%Y-%m-%d"),
                    end_date=end_dt.strftime("%Y-%m-%d")
                )
                db_manager = DatabaseManager()

                real_df = MonitorAnalysisService.get_monitor_defect_details(
                    _repository_factory=partial(build_monitor_repository, db_manager),
                    query_config_json=query_config.model_dump_json(),
                    time_group=selected_time,
                    defect_type=core_defect_type,
                    time_type='MIXED',
                    force_compliant=True,
                    data_type_filter=data_type_filter  # ✅ 传入监控类型
                )

                if real_df.empty:
                    st.info(f"💡 近 3 个月内，未追溯到具体的 **{defect_type}** 拦截明细。")
                else:
                    if 'factory' in real_df.columns and factory != "ALL":
                        real_df = real_df[real_df['factory'] == factory]

                    if real_df.empty:
                        st.info(f"💡 该时段内，属于 **{factory}** 工厂的 {defect_type} 明细为空。")
                    else:
                        st.success(f"✅ 钻取成功！共捕获 **{len(real_df)}** 片真实的底层追溯数据。")
                        # 现在的 st.dataframe 自带极强的列过滤和排序功能，几百上千行数据一眼看穿！
                        st.dataframe(real_df, width="stretch", hide_index=True, height=400)
                        
            except Exception as e:
                import traceback
                st.error(f"❌ 钻取请求失败: {str(e)}")
                with st.expander("查看详细错误日志"):
                    st.code(traceback.format_exc())


# =========================================================================
# 管理员报警明细表 (Cached Alarm Details)
# =========================================================================
def _apply_compliance_visibility_filter(detail_df: pd.DataFrame) -> pd.DataFrame:
    """Hide rows that the shared compliance engine marks as modified."""
    if detail_df is None or detail_df.empty:
        return pd.DataFrame()

    original_columns = detail_df.columns.tolist()
    compliant_df = sanitize_to_compliant(detail_df, add_tag=True)
    if "is_compliant_modified" not in compliant_df.columns:
        return compliant_df[original_columns].copy()

    visible_df = compliant_df[~compliant_df["is_compliant_modified"].fillna(False).astype(bool)].copy()
    return visible_df[[col for col in original_columns if col in visible_df.columns]].copy()


def _normalise_alarm_detail_frame(df: pd.DataFrame, monitor_type: str, alarm_type: str) -> pd.DataFrame:
    if df.empty:
        return df

    normalised = df.copy()
    if "data_type" not in normalised.columns:
        normalised["data_type"] = monitor_type
    if "spc_status" not in normalised.columns and "status" not in normalised.columns:
        normalised["spc_status"] = alarm_type
    return normalised


def _stable_cache_key_fragment(mapping: dict | None) -> str:
    """把 prod -> 签名（可嵌套 scope -> 签名）映射序列化为排序后的稳定字符串，
    作为 st.cache_data 缓存键的一部分；dict 内容变化即产生不同的键。"""
    if not mapping:
        return ""
    parts = []
    for key in sorted(mapping):
        value = mapping[key]
        if isinstance(value, dict):
            value = _stable_cache_key_fragment(value)
        parts.append(f"{key}={value}")
    return ";".join(parts)


@st.cache_data(
    show_spinner=False,
    max_entries=4,
    ttl=ConfigLoader.get_service_cache_ttl_seconds(
        "inline_monitor_alarm_details", default_hours=12
    ),
)
def get_cached_alarm_detail_tables(
    _db_manager: DatabaseManager,
    query_config_json: str,
    time_type: str,
    snapshot_signature: str,
    compliance_signature: str,
    revision_signature: str = "",
    decision_signature: str = "",
) -> dict[str, pd.DataFrame]:
    """Build cached physical alarm details for the admin table.

    TTL 由 config/global.yaml 的 service_cache.ttl_hours.inline_monitor_alarm_details
    配置（默认 12h）；缓存键除查询参数外还包含 snapshot/compliance 签名、
    产品 revision 与决策签名（``revision_signature``/``decision_signature``，
    由页面把 product_revisions/decision_signatures 序列化为稳定字符串后传入），
    用户编辑 __flags 或刷新缓存换 revision 时触发缓存 miss 与明细重建。
    """
    del snapshot_signature, compliance_signature, revision_signature, decision_signature

    alarm_frames: list[pd.DataFrame] = []
    for monitor_type in ALARM_DETAIL_MONITOR_TYPES:
        for alarm_type in ALARM_DETAIL_STATUS_OPTIONS:
            try:
                real_df = MonitorAnalysisService.get_monitor_defect_details(
                    _repository_factory=partial(build_monitor_repository, _db_manager),
                    query_config_json=query_config_json,
                    time_group="ALL",
                    defect_type=alarm_type,
                    time_type=time_type,
                    force_compliant=False,
                    data_type_filter=monitor_type,
                )
            except Exception as e:
                logging.error(
                    "[SPC] 报警明细缓存构建失败: monitor_type=%s alarm_type=%s error=%s",
                    monitor_type,
                    alarm_type,
                    e,
                    exc_info=True,
                )
                continue

            if real_df.empty:
                continue

            visible_df = _apply_compliance_visibility_filter(real_df)
            if visible_df.empty:
                continue

            alarm_frames.append(_normalise_alarm_detail_frame(visible_df, monitor_type, alarm_type))

    if not alarm_frames:
        return {monitor_type: pd.DataFrame() for monitor_type in ALARM_DETAIL_MONITOR_TYPES}

    combined_df = pd.concat(alarm_frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
    if "data_type" not in combined_df.columns:
        return {"ALL": combined_df}

    return {
        monitor_type: combined_df[
            combined_df["data_type"].astype(str).str.upper() == monitor_type.upper()
        ].copy()
        for monitor_type in ALARM_DETAIL_MONITOR_TYPES
    }


def _selected_alarm_monitor_types(data_type_filter: str) -> list[str]:
    if data_type_filter == "ALL":
        return ALARM_DETAIL_MONITOR_TYPES
    return [data_type_filter] if data_type_filter in ALARM_DETAIL_MONITOR_TYPES else []


def _filter_alarm_detail_by_state(df: pd.DataFrame, filter_state: MonitorFilterState) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df.copy()
    if filter_state.selected_products and "prod_code" in filtered.columns:
        filtered = filtered[filtered["prod_code"].isin(filter_state.selected_products)]
    if filter_state.selected_factories and "factory" in filtered.columns:
        filtered = filtered[filtered["factory"].isin(filter_state.selected_factories)]
    return filtered


def _filter_alarm_detail_by_status(df: pd.DataFrame, selected_statuses: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    if not selected_statuses:
        return df.iloc[0:0].copy()

    status_col = "spc_status" if "spc_status" in df.columns else "status" if "status" in df.columns else None
    if status_col is None:
        return df

    selected = {status.upper() for status in selected_statuses}
    return df[df[status_col].astype(str).str.upper().isin(selected)].copy()


def render_alarm_detail_tables(
    db_manager: DatabaseManager,
    query_config_json: str,
    filter_state: MonitorFilterState,
    snapshot_signature: str,
    is_admin: bool = False,
    product_revisions: dict | None = None,
    decision_signatures: dict | None = None,
) -> None:
    """Render cached alarm detail tables, grouped by monitor type.

    ``product_revisions``/``decision_signatures`` 为页面按产品（及 scope）预算的
    revision 与决策签名映射；序列化为稳定字符串后进入缓存键，签名变化即重建明细。
    """
    if not is_admin:
        return

    st.markdown("#### 报警明细表")
    with st.spinner("正在加载缓存报警明细..."):
        detail_tables = get_cached_alarm_detail_tables(
            db_manager,
            query_config_json,
            "MIXED",
            snapshot_signature,
            _get_compliance_file_signature(),
            _stable_cache_key_fragment(product_revisions),
            _stable_cache_key_fragment(decision_signatures),
        )

    monitor_types = _selected_alarm_monitor_types(filter_state.data_type_filter)
    if not monitor_types:
        st.caption("当前监控类型暂无报警明细。")
        return

    has_any = False
    for monitor_type in monitor_types:
        type_df = _filter_alarm_detail_by_state(
            detail_tables.get(monitor_type, pd.DataFrame()),
            filter_state,
        )
        has_any = has_any or not type_df.empty

        with st.expander(f"{monitor_type} 报警明细（{len(type_df)}）", expanded=not type_df.empty):
            selected_statuses = st.multiselect(
                "报警类型",
                options=ALARM_DETAIL_STATUS_OPTIONS,
                default=ALARM_DETAIL_STATUS_OPTIONS,
                key=f"alarm_detail_status_filter_{monitor_type}",
            )
            view_df = _filter_alarm_detail_by_status(type_df, selected_statuses)

            if view_df.empty:
                st.caption("当前筛选条件下无报警明细。")
                continue

            st.dataframe(
                view_df,
                width="stretch",
                hide_index=True,
                height=520,
            )

    if not has_any:
        st.caption("当前产品、厂别和监控类型下无可展示的报警明细。")

# =========================================================================
# 数据联动处理引擎 (Data Binding Engine)
# =========================================================================
def _filter_spc_data_type(df: pd.DataFrame, data_type_filter: str) -> pd.DataFrame:
    """从 ALL 缓存结果中按监控类型切片。"""
    if df is None or df.empty or data_type_filter == 'ALL' or 'data_type' not in df.columns:
        return df.copy() if df is not None else pd.DataFrame()

    target = str(data_type_filter).upper()
    source = df['data_type'].astype(str).str.upper()
    return df[source == target].copy()


def _recompute_spc_rates(df: pd.DataFrame) -> pd.DataFrame:
    """按聚合后的绝对片数重算报警率。"""
    if df.empty or '抽检数' not in df.columns:
        return df

    denominator = df['抽检数'].replace(0, np.nan)
    if 'OOS片数' in df.columns:
        df['OOS'] = df['OOS片数'] / denominator
    if 'OOC片数' in df.columns:
        df['OOC'] = df['OOC片数'] / denominator
    if 'SOOS片数' in df.columns:
        df['SOOS'] = df['SOOS片数'] / denominator
    return df


def _rollup_metric_rows(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """把 ALL 缓存中的 data_type 维度卷回页面展示粒度。"""
    if df.empty:
        return df

    metric_cols = ['抽检数', 'OOS片数', 'SOOS片数', 'OOC片数']
    metric_cols = [col for col in metric_cols if col in df.columns]
    if not metric_cols:
        return df

    rolled = df.groupby(group_cols, as_index=False, observed=False)[metric_cols].sum()
    return _recompute_spc_rates(rolled)


def filter_and_rollup_monitor_data(
    detail_df: pd.DataFrame, 
    global_summary_df: pd.DataFrame, 
    station_detail_df: pd.DataFrame,
    filter_state: MonitorFilterState
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    [前端动态联动核心] 
    根据用户的下拉框选择过滤明细数据，并动态向上卷起重算大盘汇总数据。
    彻底解耦：在不重新请求后端的情况下，实现图表和表格的实时物理联动。
    """
    trace_logger.info(f"🚧 [ScrapTrace][UI-L1] filter_and_rollup 输入: detail_df={len(detail_df)} 条, global={len(global_summary_df)} 条, station={len(station_detail_df)} 条")
    trace_logger.info(f"🚧 [ScrapTrace][UI-L1] filter_state: products={filter_state.selected_products}, factories={filter_state.selected_factories}, type={filter_state.data_type_filter}")
    
    if detail_df.empty:
        trace_logger.info("🚧 [ScrapTrace][UI-L2] detail_df 为空，直接透传")
        return global_summary_df, detail_df, station_detail_df

    # 1. 从 ALL 缓存结果中按监控类型、产品、厂别过滤明细表
    typed_detail_df = _filter_spc_data_type(detail_df, filter_state.data_type_filter)
    trace_logger.info(f"🚧 [ScrapTrace][UI-L3] detail_df 列: {detail_df.columns.tolist()}, factory唯一值: {detail_df['factory'].unique().tolist() if 'factory' in detail_df.columns else 'N/A'}")
    filtered_detail_df = typed_detail_df[
        (typed_detail_df['prod_code'].isin(filter_state.selected_products)) & 
        (typed_detail_df['factory'].isin(filter_state.selected_factories))
    ].copy()
    filtered_detail_df = _rollup_metric_rows(
        filtered_detail_df,
        ['time_group', 'prod_code', 'factory'],
    )
    trace_logger.info(f"🚧 [ScrapTrace][UI-L3] 过滤并卷积后 filtered_detail_df: {len(filtered_detail_df)} 条")
    
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
        ordered_times = global_summary_df['time_group'].drop_duplicates().tolist() if 'time_group' in global_summary_df.columns else []
        if ordered_times:
            agg_df['time_group'] = pd.Categorical(agg_df['time_group'], categories=ordered_times, ordered=True)
            
        filtered_summary_df = agg_df.sort_values('time_group').reset_index(drop=True)
    else:
        # 过滤后没数据，返回空壳
        filtered_summary_df = pd.DataFrame(columns=global_summary_df.columns)
        
    # =========================================================
    # 处理 Top 10 站点数据，前端根据用户交互进行二次切片
    # =========================================================
    if station_detail_df is not None and not station_detail_df.empty:
        # 1. 物理过滤：严格响应前端【监控类型】、【产品】与【厂别】的下拉框
        typed_station_df = _filter_spc_data_type(station_detail_df, filter_state.data_type_filter)
        filtered_station = typed_station_df[
            (typed_station_df['prod_code'].isin(filter_state.selected_products)) & 
            (typed_station_df['factory'].isin(filter_state.selected_factories))
        ].copy()
        filtered_station = _rollup_metric_rows(
            filtered_station,
            ['prod_code', 'factory', 'step_id'],
        )

        # 🚨 [关键探针 C] 前端联动后计数
        ooc_frontend = filtered_station['OOC片数'].sum() if 'OOC片数' in filtered_station.columns else 0
        logging.info(f"📊 [UI] 前端过滤后站点 OOC 总数: {ooc_frontend}")
        
        if not filtered_station.empty:
            # 2. 统计所有异常列的总和，找出 Top 10 站点的名称 (step_id)
            err_cols = [c for c in ['OOS片数', 'SOOS片数', 'OOC片数'] if c in filtered_station.columns]
            
            if err_cols:
                # 先按站点把异常数揉在一起，纯粹为了排序找 Top 10
                step_errors = filtered_station.groupby('step_id')[err_cols].sum().sum(axis=1)
                top10_step_ids = step_errors.sort_values(ascending=False).head(10).index.tolist()
                
                # 3. [核心修复] 从切片后的完整数据中，仅提取这 10 个站点的行。
                # 这样做完美保留了 prod_code 和 抽检数 等所有维度，供下游图表自由 groupby 和堆叠！
                top_station_df = filtered_station[filtered_station['step_id'].isin(top10_step_ids)].copy()
            else:
                top_station_df = pd.DataFrame()
        else:
            top_station_df = pd.DataFrame()
    else:
        top_station_df = pd.DataFrame()

    return filtered_summary_df, filtered_detail_df, top_station_df

# =========================================================================
# 🏆 Top 10 异常站点分析模块 (Top 10 Station Section)
# =========================================================================
def _build_station_top10_payload(
    filtered_station_df: pd.DataFrame,
    data_type_filter: str,
    show_tables: bool,
    step_desc_map: dict[str, str] | None = None,
) -> dict[str, object]:
    """[RenderGate 阶段1] 纯计算：聚合 Top 10 站点并组装图表与表格材料，禁止触碰 st.*。"""
    is_scrap = data_type_filter == '报废'
    if is_scrap:
        base_cols = ['OOC']
        actual_count_cols = [f"{c}片数" for c in base_cols if f"{c}片数" in filtered_station_df.columns]
    else:
        base_cols = ['OOS', 'SOOS', 'OOC']
        actual_count_cols = [f"{c}片数" for c in base_cols if f"{c}片数" in filtered_station_df.columns]

    # [核心修复 1]：动态识别并纳入“抽检数”进行多维聚合
    has_sample_size = '抽检数' in filtered_station_df.columns
    agg_cols = actual_count_cols + (['抽检数'] if has_sample_size else [])

    # --- 核心视图逻辑：内部提炼 Top 10 站点 ---
    agg_station_df = filtered_station_df.groupby('step_id', as_index=False)[agg_cols].sum()

    # [核心修复 2]：计算异常总数 (Total) 仅用于找出 Top 10 瓶颈，绝对不能把抽检数加进去！
    agg_station_df['Total'] = agg_station_df[actual_count_cols].sum(axis=1)

    top_station_df = agg_station_df[agg_station_df['Total'] > 0].sort_values('Total', ascending=False).head(10)

    if top_station_df.empty:
        return {"status": "no_alarm_station"}

    top10_stations_list = top_station_df['step_id'].tolist()

    # ==========================================
    # 0. Echarts 垂直堆叠柱状图
    # ==========================================
    chart_df = top_station_df.copy()
    x_data = [format_step_label(step, step_desc_map) for step in chart_df['step_id'].tolist()]

    option = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "legend": {"data": ["报废片数"] if is_scrap else ["OOC片数", "SOOS片数", "OOS片数"], "bottom": 0},
        "grid": {"left": "3%", "right": "3%", "bottom": "15%", "containLabel": True},
        "xAxis": {
            "type": "category",
            "data": x_data,
            "axisLabel": {"interval": 0, "fontWeight": "bold"}
        },
        "yAxis": {"type": "value", "name": "报废总片数" if is_scrap else "报警总片数"},
        "series": []
    }

    if 'OOC片数' in actual_count_cols:
        option["series"].append({
            "name": "报废片数" if is_scrap else "OOC片数", "type": "bar", "stack": "总量", "barMaxWidth": 80,
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

    payload: dict[str, object] = {
        "status": "ok",
        "option": option,
    }

    if not show_tables:
        return payload

    # ==========================================
    # 1. 汇总表 (Summary Table): 转置矩阵
    # ==========================================
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
    sum_view.index = [format_step_label(step, step_desc_map) for step in sum_view.index]
    view_df = sum_view.T.reset_index().rename(columns={'index': '统计维度'})

    # [报废类型] 前端文案替换
    if is_scrap:
        view_df['统计维度'] = view_df['统计维度'].replace({
            'OOC片数': '报废片数',
            'OOC占比': '报废率'
        })

    gb_sum = GridOptionsBuilder.from_dataframe(view_df)
    gb_sum.configure_selection(selection_mode="single", use_checkbox=False)
    gb_sum.configure_column("统计维度", pinned="left", width=95, cellStyle={'fontWeight': 'bold', 'backgroundColor': '#f8f9fa'})

    for col in [format_step_label(step, step_desc_map) for step in top10_stations_list]:
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

    # ==========================================
    # 2. 明细表 (Detail Table): By 产品折叠 + 占比补全
    # ==========================================
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

    # [报废类型] 前端文案替换
    if is_scrap:
        flat_df['报警类型'] = flat_df['报警类型'].replace({
            'OOC片数': '报废片数',
            'OOC占比': '报废率'
        })

    available_stations = [s for s in top10_stations_list if s in flat_df.columns]

    for col in available_stations:
        is_ratio = flat_df['报警类型'].str.contains('占比') if not is_scrap else flat_df['报警类型'] == '报废率'
        flat_df[col] = np.where(
            is_ratio,
            (flat_df[col].fillna(0).astype(float) * 100).map("{:.2f}%".format),
            flat_df[col].fillna(0).astype(int).astype(str)
        )

    flat_df = flat_df[['品名', '报警类型'] + available_stations]
    station_labels = {step: format_step_label(step, step_desc_map) for step in available_stations}
    flat_df = flat_df.rename(columns=station_labels)

    gb_det = GridOptionsBuilder.from_dataframe(flat_df)
    gb_det.configure_selection(selection_mode="single", use_checkbox=False)
    gb_det.configure_column("品名", rowGroup=True, hide=True)
    gb_det.configure_column("报警类型", pinned="left", width=95, cellStyle={'fontWeight': 'bold', 'backgroundColor': '#f8f9fa'})

    # [核心修改] 为站点列添加下钻样式（蓝色下划线 + 手型光标）
    for col in station_labels.values():
        gb_det.configure_column(col, cellStyle={
            'color': '#1e88e5', 'cursor': 'pointer', 'textDecoration': 'underline'
        })

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

    payload.update({
        "sum_view_df": view_df,
        "grid_options_sum": grid_options_sum,
        "det_flat_df": flat_df,
        "grid_options_det": grid_options_det,
        "available_stations": available_stations,
    })
    return payload


def _render_station_top10_payload(
    payload: dict[str, object],
    data_type_filter: str,
    is_admin: bool,
    show_tables: bool,
) -> None:
    """[RenderGate 阶段2] 集中渲染：仅执行 st.* 调用与交互回读，不做任何重计算。"""
    if payload["status"] == "no_alarm_station":
        st.success("🎉 当前监控下无任何超规报警站点！")
        return

    is_scrap = data_type_filter == '报废'

    # ==========================================
    # 0. Echarts 垂直堆叠柱状图
    # ==========================================
    st.markdown("#### 🏆 Top 10 异常站点分布图")
    st_echarts(payload["option"], height="450px")
    if not show_tables:
        return
    st.divider()

    # ==========================================
    # 1. 汇总表 (Summary Table): 转置矩阵
    # ==========================================
    st.markdown("#### 📊 Top 10 异常站点汇总表")
    AgGrid(
        payload["sum_view_df"],
        gridOptions=payload["grid_options_sum"],
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
    grid_response = AgGrid(
        payload["det_flat_df"],
        gridOptions=payload["grid_options_det"],
        enable_enterprise_modules=True,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme='streamlit',
        height=380,
        allow_unsafe_jscode=True,
        key=f"ag_top10_detail_{st.session_state.ag_top10_det_key}"
    )

    # =========================================================================
    # [核心新增] Top 10 站点明细表下钻逻辑
    # =========================================================================
    available_stations = payload["available_stations"]
    selected_rows = grid_response.get("selected_rows")
    if selected_rows is not None and len(selected_rows) > 0:
        row_data = selected_rows.iloc[0].to_dict() if isinstance(selected_rows, pd.DataFrame) else selected_rows[0]
        if "报警类型" in row_data:
            defect = row_data.get("报警类型")
            # 支持下钻的报警类型：片数行、占比行、或纯比率行
            drillable_types = ['OOS', 'SOOS', 'OOC', '报废率'] if is_scrap else ['OOS', 'SOOS', 'OOC']
            if '片数' in defect or '占比' in defect or defect in drillable_types: # type: ignore
                prod = row_data.get("品名", "ALL")
                core_defect = defect.replace("片数", "").replace("占比", "").strip()

                current_lock = f"{prod}_{core_defect}"

                # 🛑 [核心修改]: 在弹出弹窗前，判断是否为管理员
                if is_admin:
                    if st.session_state.spc_station_top10_lock != current_lock:
                        st.session_state.spc_station_top10_lock = current_lock
                        show_drilldown_modal(prod, "ALL", core_defect, available_stations, data_type_filter, source="station_top10")
                else:
                    st.session_state.spc_station_top10_lock = None
    else:
        st.session_state.spc_station_top10_lock = None


def render_station_top10_section(
    filtered_station_df: pd.DataFrame,
    data_type_filter: str = 'SPC',
    is_admin: bool = False,
    show_tables: bool = True,
    step_desc_map: dict[str, str] | None = None,
):
    """渲染 Top 10 异常站点图表、汇总(转置)与明细表(产品折叠)

    两阶段渲染：先在 RenderGate 统一 spinner 下完成 Top 10 聚合与 option/表格材料组装，
    再集中回流渲染，避免重计算与 st.* 推送交错导致图表阶段式跳出。
    """
    if 'ag_top10_sum_key' not in st.session_state:
        st.session_state.ag_top10_sum_key = 0
    if 'ag_top10_det_key' not in st.session_state:
        st.session_state.ag_top10_det_key = 0
    if 'spc_station_top10_lock' not in st.session_state:
        st.session_state.spc_station_top10_lock = None

    if filtered_station_df.empty:
        st.success("🎉 当前监控下无任何超规报警站点！")
        return

    is_scrap = data_type_filter == '报废'
    if is_scrap:
        base_cols = ['OOC']
        actual_count_cols = [f"{c}片数" for c in base_cols if f"{c}片数" in filtered_station_df.columns]
    else:
        base_cols = ['OOS', 'SOOS', 'OOC']
        actual_count_cols = [f"{c}片数" for c in base_cols if f"{c}片数" in filtered_station_df.columns]

    if not actual_count_cols:
         st.success("🎉 当前监控下，无有效的报警数据列！")
         return

    gate = RenderGate()
    gate.stage(partial(
        _build_station_top10_payload,
        filtered_station_df,
        data_type_filter,
        show_tables,
        step_desc_map,
    ))
    for payload in gate.collect():
        _render_station_top10_payload(payload, data_type_filter, is_admin, show_tables)
