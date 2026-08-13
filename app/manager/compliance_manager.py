"""
数据修饰配置文件管理模块 (Compliance Config Manager)

功能：
1. 从共享 Excel 文件读取四维修饰配置
2. 提供只读界面展示当前配置
3. 支持下载/上传配置文件（管理员）
4. 统一运行时配置路径与缓存失效签名
"""

import logging
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

from src.shared_kernel.compliance_config_excel import (
    build_compliance_config_dataframe,
    compliance_rule_matches,
    compliance_config_to_xlsx_bytes,
    load_compliance_config_from_xlsx,
    write_compliance_config_to_xlsx,
)
from src.shared_kernel.config import ConfigLoader

# 管理界面与运行时修饰引擎共用唯一配置文件。
CONFIG_PATH = ConfigLoader.get_compliance_config_path()

# 报废Sheet路径
SCRAP_SHEET_PATH = Path("resources/scrap_sheets.xlsx")


def _ensure_config_exists():
    """Create an empty four-column workbook when no configuration exists."""
    if CONFIG_PATH.exists():
        return
    write_compliance_config_to_xlsx({"rules": []}, CONFIG_PATH)
    logging.info("[ComplianceConfig] 创建空配置文件: %s", CONFIG_PATH)


def load_compliance_config() -> dict:
    """Load the shared four-dimension compliance configuration."""
    _ensure_config_exists()
    try:
        return load_compliance_config_from_xlsx(CONFIG_PATH)
    except Exception as error:
        logging.error("[ComplianceConfig] 加载配置失败: %s", error, exc_info=True)
        return {"rules": []}


def get_compliance_config(
    factory: str,
    prod_code: str,
    data_type: str,
    month: int | str | None,
) -> bool:
    """Return True when any enabled four-dimension row matches the context."""
    return any(
        compliance_rule_matches(
            rule,
            factory=factory,
            prod_code=prod_code,
            data_type=data_type,
            month=month,
        )
        for rule in load_compliance_config().get("rules", [])
    )


def save_compliance_config(config: dict) -> bool:
    """Persist the shared four-column workbook."""
    try:
        write_compliance_config_to_xlsx(config, CONFIG_PATH)
        logging.info("[ComplianceConfig] 配置已保存: %s", CONFIG_PATH)
        return True
    except Exception as error:
        logging.error("[ComplianceConfig] 保存配置失败: %s", error, exc_info=True)
        return False


def get_compliance_file_signature() -> str:
    """Return the active workbook signature used to invalidate Streamlit caches."""
    try:
        stat = CONFIG_PATH.stat()
        return f"{stat.st_mtime_ns}:{stat.st_size}"
    except OSError:
        return "missing"


def _filter_rules_for_selection(
    config: dict,
    data_type: str,
    selected_products: list[str],
    selected_factories: list[str],
) -> pd.DataFrame:
    rules_df = build_compliance_config_dataframe(config)
    if rules_df.empty:
        return rules_df

    product_mask = rules_df["产品型号"].eq("ALL") | rules_df["产品型号"].isin(selected_products)
    factory_mask = rules_df["厂别"].eq("ALL") | rules_df["厂别"].isin(selected_factories)
    type_mask = True if data_type == "ALL" else rules_df["监控类型"].isin(["ALL", data_type])
    return rules_df[product_mask & factory_mask & type_mask].reset_index(drop=True)


def render_compliance_config_panel(
    data_type: str,
    selected_products: list,
    selected_factories: list
):
    """
    渲染修饰配置面板（只读展示 + 文件管理）
    
    此面板仅用于：
    1. 展示当前配置状态
    2. 管理员下载/上传配置文件
    3. 上传成功后清除页面数据缓存
    """
    query_params = st.query_params
    is_admin = query_params.get("admin") == "true"
    
    config = load_compliance_config()
    
    with st.expander("🔧 数据修饰配置", expanded=False):
        st.info("配置来源：`resources/compliance_config.xlsx`。每一行都是启用规则，支持 ALL。")
        visible_rules = _filter_rules_for_selection(
            config,
            data_type,
            selected_products,
            selected_factories,
        )
        if visible_rules.empty:
            st.info("当前筛选范围没有启用的数据修饰规则。")
        else:
            st.dataframe(visible_rules, hide_index=True, width="stretch")
        
        # 管理员功能：下载/上传配置文件
        if is_admin:
            st.divider()
            st.markdown("#### 🛠️ 管理员操作")
            st.warning("修改配置文件后，请刷新页面使配置生效")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # 下载当前配置
                st.download_button(
                    label="📥 下载配置文件",
                    data=compliance_config_to_xlsx_bytes(config),
                    file_name="compliance_config.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="下载当前配置文件到本地，修改后上传"
                )
            
            with col2:
                # 上传新配置
                uploaded_file = st.file_uploader(
                    "📤 上传配置文件",
                    type=['xlsx'],
                    help="上传修改后的配置文件（将覆盖原文件）"
                )
                
                if uploaded_file is not None:
                    try:
                        # 验证 xlsx 格式
                        new_config = load_compliance_config_from_xlsx(uploaded_file)
                        if save_compliance_config(new_config):
                            st.cache_data.clear()
                            st.success("✅ 配置已更新，缓存已清除")
                    except Exception as e:
                        st.error(f"配置文件解析失败: {e}")
            
            st.divider()
            render_scrap_sheet_uploader()
            
        else:
            st.divider()
            st.info("💡 管理员可通过添加 `?admin=true` 参数到 URL 来获取配置文件管理权限")


def render_scrap_sheet_uploader():
    """
    报废Sheet覆写面板
    
    参考 render_trend_override_uploader 的交互样式：
    - 左列：步骤1 下载标准模板（现有文件或空模板）
    - 右列：步骤2 上传覆盖文件
    """
    st.markdown("**📊 报废Sheet覆写**")
    
    file_exists = SCRAP_SHEET_PATH.exists()
    
    col_dl, col_up = st.columns(2)
    
    # ---------------- 📥 步骤 1: 下载标准模板 ----------------
    with col_dl:
        st.markdown("**📥 步骤 1: 下载标准模板**")
        
        output = BytesIO()
        if file_exists:
            try:
                # 读取当前生效的报废清单
                with open(SCRAP_SHEET_PATH, "rb") as f:
                    output.write(f.read())
                st.download_button(
                    label="⬇️ 下载当前报废清单",
                    data=output.getvalue(),
                    file_name="scrap_sheets.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_scrap_sheet"
                )
            except Exception as e:
                logging.error(f"[ScrapSheet] 读取现有文件失败: {e}")
                st.error(f"读取现有文件失败: {e}")
        else:
            # 生成标准模板
            template_df = pd.DataFrame(columns=['产品型号', 'Sheet_ID', '报废时间', '报废站点'])
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                template_df.to_excel(writer, index=False, sheet_name='报废数据')
            st.download_button(
                label="⬇️ 下载标准模板",
                data=output.getvalue(),
                file_name="scrap_sheets_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_scrap_tpl"
            )
            st.info("当前无报废数据，已生成标准模板")
    
    # ---------------- 📤 步骤 2: 上传覆盖文件 ----------------
    with col_up:
        st.markdown("**📤 步骤 2: 上传覆盖文件**")
        uploaded = st.file_uploader(
            "请上传填好的 Excel 文件",
            type=['xlsx'],
            key="up_scrap_sheet"
        )
        
        if uploaded is not None:
            if st.button("🚀 确认覆盖并刷新", type="primary", width="stretch", key="btn_scrap_sheet"):
                try:
                    # 确保目标文件夹存在
                    SCRAP_SHEET_PATH.parent.mkdir(parents=True, exist_ok=True)
                    
                    # 如果旧文件存在，尝试删除
                    if SCRAP_SHEET_PATH.exists():
                        try:
                            SCRAP_SHEET_PATH.unlink()
                            logging.info(f"[ScrapSheet] 已删除旧文件: {SCRAP_SHEET_PATH}")
                        except PermissionError:
                            st.error("❌ 无法删除旧文件，它可能正被 Excel 打开，请关闭后重试。")
                            return
                    
                    # 保存上传的文件
                    with open(SCRAP_SHEET_PATH, "wb") as f:
                        f.write(uploaded.getbuffer())
                    
                    st.success("✅ 报废Sheet已更新，正在刷新页面...")
                    logging.info(f"[ScrapSheet] 成功覆写文件: {SCRAP_SHEET_PATH}")
                    
                    # 清除缓存并刷新
                    st.cache_data.clear()
                    
                    # 🆕 [核心修复] 清除 st.session_state 中报废类型的 view_model 缓存
                    # 否则旧的空 view_model 会继续被使用，导致新上传的数据不显示
                    keys_to_remove = [k for k in st.session_state.keys() if k.startswith("spc_view_model_报废")]
                    for k in keys_to_remove:
                        del st.session_state[k]
                        logging.info(f"[ScrapSheet] 已清除 session_state 缓存: {k}")
                    
                    st.rerun()
                    
                except Exception as e:
                    logging.error(f"[ScrapSheet] 保存文件失败: {e}")
                    st.error(f"保存文件失败: {e}")
