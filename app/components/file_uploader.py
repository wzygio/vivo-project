# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import logging, io
from pathlib import Path

from src.shared_kernel.config_model import AppConfig

def render_trend_override_uploader(config: AppConfig, product_dir: Path):
    """
    [企业级后台组件] 渲染开发者专属的配置文件与覆盖数据管理中心。
    使用 st.tabs 支持多个 YAML 配置文件的上传与无缝重载。
    """
    with st.expander("🛠️ 开发者后台：配置与数据覆写管理", expanded=False):
        
        # 建立多标签页视图
        tab1, tab2, tab3 = st.tabs(["📈 趋势图数据修正", "⚠️ 预警规格线配置", "🎯 Sheet不良率覆写"])
        
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