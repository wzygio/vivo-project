# src/vivo_project/app/components/components.py
import pandas as pd
import streamlit as st
import logging, os, io
from pathlib import Path
from typing import Dict, Optional, Callable, List

# [Refactor] 引入配置模型
from src.shared_kernel.config_model import AppConfig
from app.utils.session_manager import SessionManager

# [Refactor] 定义默认缓存时间，替代原 CONFIG['application']['cache_ttl_hours']
DEFAULT_CACHE_TTL = 4 * 60 * 60  # 4 Hours

# --- 常量 ---
COLOR_MAP = {
    'Array_Line': "#1930ff",  # Plotly默认的蓝色
    'OLED_Mura': "#ff2828",   # Plotly默认的红色
    'Array_Pixel': "#6fb9ff",   # Plotly默认的浅蓝色
    'array_Line_rate': "#1930ff",  
    'oled_mura_rate': "#ff2828",   
    'array_pixel_rate': "#6fb9ff"   
}

def render_lot_spec_alert(lot_data: dict, warning_lines: Dict[str, dict], time_period: int = 30):
    """
    [企业级预警组件 V3.0] 扫描并展示近 30 天内良损超规的 Lot，包含多维度追溯明细表。
    
    :param lot_data: YieldAnalysisService 返回的 lot 级数据字典
    :param warning_lines: {code_desc: upper_limit} 的警戒线字典
    """
    total_recent_lots = 0
    oos_records = [] # 用于存储超规明细列表
    
    if lot_data and 'code_level_details' in lot_data:
        # 1. 提取所有 Code 级明细并合并
        all_dfs: list[pd.DataFrame] = []
        for df in lot_data['code_level_details'].values():
            if not df.empty and 'lot_id' in df.columns and 'warehousing_time' in df.columns:
                all_dfs.append(df)
        
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            
            # 2. 时间过滤 (近 30 天)
            # 注意：这里的 warehousing_time 被转为了 datetime 对象
            combined_df['warehousing_time'] = pd.to_datetime(
                combined_df['warehousing_time'], format='%Y%m%d', errors='coerce'
            )
            max_date = combined_df['warehousing_time'].max()
            
            if pd.notna(max_date):
                threshold_date = max_date - pd.Timedelta(days=time_period)
                recent_df = combined_df[combined_df['warehousing_time'] >= threshold_date].copy()
                
                # 3. 统计近 30 天总 Lot 数
                total_recent_lots = recent_df['lot_id'].nunique()
                
                # 4. 核心逻辑：遍历并收集超规明细 (新增时间与数量字段)
                for _, row in recent_df.iterrows():
                    code = str(row.get('defect_desc', '')).strip()
                    rate = row.get('defect_rate', 0.0)
                    
                    # [防呆修复] 安全获取警戒线字典
                    spec_dict = warning_lines.get(code)
                    if not spec_dict:
                        continue
                        
                    # 安全提取上限，如果配置缺失默认 1.0 (100%)
                    spec_limit = spec_dict.get('upper', 1.0)
                    
                    if rate > spec_limit:
                        # 处理入库时间格式化 (之前已被转为 datetime 对象)
                        w_time = row.get('warehousing_time')
                        w_time_str = w_time.strftime('%Y/%m/%d') if pd.notna(w_time) else "-"
                        
                        # 处理阵列投入时间格式化 (安全转换并格式化)
                        a_time = pd.to_datetime(row.get('array_input_time'), errors='coerce')
                        a_time_str = a_time.strftime('%Y/%m/%d') if pd.notna(a_time) else "-"
                        
                        # 获取不良 panel 数 (安全转为整数)
                        defect_count = int(row.get('defect_panel_count', 0)) if pd.notna(row.get('defect_panel_count')) else 0

                        oos_records.append({
                            "超规 Lot ID": row.get('lot_id', 'Unknown'),
                            "异常 Code": code,
                            "管控规格线": f"{(spec_limit or 0) * 100:.2f}%" if spec_limit < 1.0 else "无限制",
                            "实际不良率": f"{(rate or 0) * 100:.2f}%",
                            "不良panel数": defect_count,
                            "入库时间": w_time_str,
                            "阵列投入时间": a_time_str,
                        })

    # --- 数据计算完毕，开始渲染 U
    oos_df = pd.DataFrame(oos_records)
    oos_count = oos_df['超规 Lot ID'].nunique() if not oos_df.empty else 0
    oos_rate = f"{(oos_count / total_recent_lots * 100):.1f}%" if total_recent_lots > 0 else "0.0%"
    has_alert = oos_count > 0
    
    with st.expander(f"🛡️ Lot级良损超规预警（近{time_period}天）", expanded=has_alert):
        if has_alert:
            st.error(f"⚠️ 发现 {oos_count} 个近期 Lot 存在至少一项缺陷超规！")
        else:
            st.success("✅ 系统监测正常：近一个月未发现超规 Lot。")

        # 1. 渲染顶部核心指标
        c1, c2, c3 = st.columns(3)
        c1.metric(f"Lot 总数（近{time_period}天）", f"{total_recent_lots}")
        c2.metric(
            "超规个数(Out of Spec)", 
            f"{oos_count}", 
        )
        c3.metric(
            "超规率", 
            oos_rate, 
        )
        
        # 2. 渲染底部独立明细表 (含新字段)
        if has_alert:
            st.divider()
            st.markdown("##### 🚨 异常 Lot 追溯明细")
            
            st.dataframe(
                oos_df,
                use_container_width=True, # 关键：让表格拉伸至容器总宽度
                hide_index=True,
                # 列配置：移除 width 参数，释放宽度锁定，让算法自动平摊宽度
                column_config={
                    "超规 Lot ID": st.column_config.TextColumn("超规 Lot ID"),
                    "入库时间": st.column_config.TextColumn("入库时间"),
                    "阵列投入时间": st.column_config.TextColumn("阵列投入时间"),
                    "异常 Code": st.column_config.TextColumn("异常 Code"),
                    "不良panel数": st.column_config.NumberColumn("不良panel数", format="%d"),
                    "管控规格线": st.column_config.TextColumn("管控规格线"),
                    "实际不良率": st.column_config.TextColumn("实际不良率"),
                }
            )

def render_page_header(
    title: Optional[str] = None, 
    config: AppConfig = None, 
    cached_funcs: list = None,
    refresh_handlers: list = None # [重构] 接收无参闭包，期待返回 bool
):
    # =========================================================================
    # [新增] Admin 隐身模式 (Stealth Mode)
    # =========================================================================
    # 只要 URL 中没有 ?admin=true，就利用 CSS 抹除侧边栏中的特定页面
    if st.query_params.get("admin") != "true":
        st.markdown(
            """
            <style>
            /* 使用属性选择器精准狙击 href 包含特定名称的 <a> 标签 */
            /* 兼容明文中文和 URL Encode 编码格式 */
            [data-testid="stSidebarNav"] a[href*=""],
            [data-testid="stSidebarNav"] a[href*="%E8%87%AA%E5%8A%A8%E9%A2%84%E8%AD%A6%E7%9C%8B%E6%9D%BF"] {
                display: none !important;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

    if title:
        st.title(title)
    
    # [重构] 具备失败拦截与安全回退的刷新回调
    def _refresh_data_callback():
        all_success = True
        
        if refresh_handlers:
            for handler in refresh_handlers:
                if callable(handler):
                    # 尝试执行后端的安全刷新动作
                    is_success = handler()
                    # 如果后端明确返回 False，说明该模块更新失败
                    if is_success is False:
                        all_success = False
                        
            # [核心容错逻辑] 如果任何一个服务更新失败
            if not all_success:
                st.toast("❌ 数据库连接或快照更新失败，已为您保留旧版本数据。", icon="🚨")
                # 关键：直接 Return，绝对不执行下方的 clear()，保护当前内存里的旧数据依然可用
                return 

        # 只有在全部更新成功的情况下，才清空内存并重载
        if cached_funcs:
            for func in cached_funcs:
                if hasattr(func, "clear"):
                    func.clear()
            st.toast("✅ 数据刷新成功，已加载最新快照！", icon="🎉")
            logging.info("🔄 [UI] 单页数据刷新完毕")
        else:
            st.cache_data.clear()

    # [保留] 精准清除内存回调 (纯清内存，不动硬盘)
    def _hard_reset_callback():
        if cached_funcs:
            for func in cached_funcs:
                if hasattr(func, "clear"):
                    func.clear()
            st.toast("🧹 内存缓存已清除", icon="✅")
        else:
            st.cache_data.clear()
            st.cache_resource.clear()

    # --- 渲染控制栏 (UI 保持不变) ---
    with st.container(border=True):
        c_prod, c_space, c_refresh, c_clear = st.columns([2, 4, 1.2, 1.2])

        with c_prod:
            current_prod = config.data_source.product_code
            available_prods = SessionManager.AVAILABLE_PRODUCTS
            selected_prod = st.selectbox(
                "📦 当前产品型号",
                options=available_prods,
                index=available_prods.index(current_prod) if current_prod in available_prods else 0,
                key=f"header_prod_sel_{title}", 
                label_visibility="collapsed" 
            )
            if selected_prod != current_prod:
                SessionManager.load_and_set_config(selected_prod)
                st.rerun()

        with c_space:
             st.write("") 

        with c_refresh:
            st.button(
                "🔄 刷新数据",
                key=f"btn_refresh_{title}",
                on_click=_refresh_data_callback,
                use_container_width=True,
                help="强制从数据库获取最新数据。若失败则自动回退并保留当前视图。"
            )
            
        with c_clear:
            st.button(
                "🧹 清除缓存",
                key=f"btn_clear_{title}",
                on_click=_hard_reset_callback,
                use_container_width=True,
                help="仅清除当前报表的内存缓存 (极速重载视图)"
            )

def extract_cached_funcs(*services) -> list:
    """
    [企业级工具] 自动探测并提取传入的 Service 类中所有的 Streamlit 缓存函数。
    支持传入多个 Service 类，合并返回。
    """
    auto_cached_funcs = []
    
    for service in services:
        for attr_name in dir(service):
            # 1. 过滤掉 Python 的内置双下划线属性和私有方法
            if attr_name.startswith("_"):
                continue
                
            attr = getattr(service, attr_name)
            
            # 2. 严格三重校验
            if hasattr(attr, "clear") and callable(attr) and hasattr(attr, "__name__"):
                auto_cached_funcs.append(attr)
                
    return auto_cached_funcs

def setup_hot_reload(enable: bool = True):
    """
    [企业级工具] 底层代码热重载守卫。
    用于在开发态下监控深层依赖模块的变化，一旦发现代码哈希变动，
    立即强制清空 sys.modules，实现后端代码修改后的无缝热生效。
    """
    if not enable:
        return

    try:
        from app.utils.reloader import deep_reload_modules, get_project_revision
        from src.shared_kernel.config import ConfigLoader
        
        # 1. 计算当前代码目录的真实哈希指纹
        project_root = ConfigLoader.get_project_root()
        current_rev = get_project_revision(project_root)
        
        # 2. 从 session_state 获取上一次的指纹
        last_rev = st.session_state.get('last_code_revision')
        
        # 3. 只有当代码指纹发生变化时，才执行暴力的模块卸载
        if last_rev is not None and last_rev != current_rev:
            import logging
            logging.info("♻️ 探测到后端底层代码库变更，触发 Deep Reload...")
            deep_reload_modules()
            
            # [关键联动]：代码都变了，旧的代码生成的缓存数据大概率也不能用了，直接顺手清空数据缓存！
            st.cache_data.clear()
            st.cache_resource.clear()
            
        # 4. 更新指纹
        st.session_state['last_code_revision'] = current_rev
        
    except ImportError as e:
        import logging
        logging.warning(f"⚠️ 热重载模块依赖缺失，已跳过: {e}")

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

def render_trend_override_uploader(config: AppConfig, product_dir: Path):
    """
    [企业级后台组件] 渲染开发者专属的配置文件与覆盖数据管理中心。
    使用 st.tabs 支持多个 YAML 配置文件的上传与无缝重载。
    """
    with st.expander("🛠️ 开发者后台：配置与数据覆写管理", expanded=False):
        
        # 建立多标签页视图
        tab1, tab2, tab3 = st.tabs(["📈 趋势图数据修正", "⚠️ 预警规格线配置", "🎯 Lot不良覆写配置"])
        
        # --- Tab 1: 趋势图人工修正 ---
        with tab1:
            _render_file_manager_tab(
                config=config, 
                product_dir=product_dir, 
                config_key='mwd_override_config',
                template_dfs={
                    'Group级': pd.DataFrame(columns=['目标名称', '周期类型', '时间标签', '期望不良率']),
                    'Code级': pd.DataFrame(columns=['目标名称', '周期类型', '时间标签', '期望不良率'])
                }
            )
            
        # --- Tab 2: 预警规格线 ---
        with tab2:
            # 根据 yield_service.py 解析要求，B列(索引1)是Code，F列(索引5)是预警线
            _render_file_manager_tab(
                config=config, 
                product_dir=product_dir, 
                config_key='static_warning_lines',
                template_dfs={
                    'Sheet1': pd.DataFrame(columns=['序号', 'Code', '缺陷大类', '工段', '机台', '预警线'])
                }
            )
            
        # --- Tab 3: Lot 级覆盖数据 ---
        with tab3:
            _render_file_manager_tab(
                config=config, 
                product_dir=product_dir, 
                config_key='rate_override_config',
                template_dfs={
                    'Sheet1': pd.DataFrame(columns=['lot_id', 'sheet_id', 'override_rate', 'defect_desc'])
                }
            )

def _render_file_manager_tab(config: AppConfig, product_dir: Path, config_key: str, template_dfs: dict):
    """
    内部子组件：处理单一配置文件的下载、生成、覆写和缓存清除流水线。
    """
    override_res = config.paths.get(config_key)
    
    if not override_res:
        st.warning(f"当前产品尚未在 YAML 中配置 `{config_key}`，无法使用此管理功能。")
        return
    
    file_name = override_res.file_name
    target_path = product_dir / file_name

    col1, col2 = st.columns([1, 1])
    
    # ---------------- 📥 步骤 1: 下载逻辑 ----------------
    with col1:
        st.markdown(f"#### 📥 步骤 1: 下载配置表")
        st.caption("您可以下载当前的配置表进行修改。如果服务器当前无配置，将下载标准模板。")  
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if target_path.exists():
                try:
                    # 如果已有文件，提供现存文件下载
                    existing_xls = pd.read_excel(target_path, sheet_name=None, engine='openpyxl')
                    for sheet_name, df in existing_xls.items():
                        df.to_excel(writer, index=False, sheet_name=sheet_name)
                except Exception as e:
                    st.error(f"读取现有配置文件失败: {e}")
                    return
            else:
                # 针对不同的 Key 下发对应的智能模板
                for sheet_name, df_template in template_dfs.items():
                    df_template.to_excel(writer, index=False, sheet_name=sheet_name)
        
        st.download_button(
            label=f"⬇️ 下载 {file_name}",
            data=output.getvalue(),
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_{config_key}" # 必须加前缀保证不同 Tab 间 Key 唯一
        )
        
    # ---------------- 📤 步骤 2: 上传覆写逻辑 ----------------
    with col2:
        st.markdown("#### 📤 步骤 2: 上传覆盖文件")
        uploaded_file = st.file_uploader(f"请上传填好的 Excel 文件", type=['xlsx'], key=f"up_{config_key}")
        
        if uploaded_file is not None:
            if st.button(f"🚀 确认覆盖并刷新 ({file_name})", type="primary", use_container_width=True, key=f"btn_{config_key}"):
                try:
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    if target_path.exists():
                        try:
                            target_path.unlink()  
                            logging.info(f"已成功删除旧的配置文件: {target_path.name}")
                        except PermissionError:
                            st.error("❌ 无法删除旧文件，它可能正被其他程序（如 Excel）打开，请关闭后重试。")
                            return
                    
                    with open(target_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    
                    st.success(f"✅ 成功覆盖文件: {file_name}")
                    
                    st.cache_data.clear()
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"保存文件失败: {e}")