"""
数据修饰配置文件管理模块 (Compliance Config Manager)

功能：
1. 从本地 YAML 文件读取修饰配置
2. 提供只读界面展示当前配置
3. 支持下载/上传配置文件（管理员）
4. 彻底避开 Streamlit 状态同步问题
"""

import streamlit as st
import yaml
import logging
import pandas as pd
from io import BytesIO
from pathlib import Path
from typing import Dict, Tuple, Optional, List
from datetime import datetime

from src.shared_kernel.compliance_config_excel import (
    compliance_config_to_xlsx_bytes,
    load_compliance_config_from_xlsx,
    write_compliance_config_to_xlsx,
)

# 配置文件路径
CONFIG_PATH = Path("config/compliance_config.xlsx")
LEGACY_YAML_CONFIG_PATH = Path("config/compliance_config.yaml")

# 报废Sheet路径
SCRAP_SHEET_PATH = Path("resources/scrap_sheets.xlsx")


def _ensure_config_exists():
    """确保配置文件存在，不存在则创建默认配置"""
    if CONFIG_PATH.exists():
        return

    default_config = {
        "default": False,
        "rules": {}
    }

    if CONFIG_PATH.suffix.lower() in {".yaml", ".yml"}:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            yaml.dump(default_config, f, allow_unicode=True, default_flow_style=False)
        logging.info(f"[ComplianceConfig] 创建默认 YAML 配置文件: {CONFIG_PATH}")
        return

    if LEGACY_YAML_CONFIG_PATH.exists():
        try:
            with open(LEGACY_YAML_CONFIG_PATH, 'r', encoding='utf-8') as f:
                legacy_config = yaml.safe_load(f) or default_config
            write_compliance_config_to_xlsx(legacy_config, CONFIG_PATH)
            logging.info(f"[ComplianceConfig] 已从 YAML 迁移到 xlsx: {CONFIG_PATH}")
            return
        except Exception as e:
            logging.error(f"[ComplianceConfig] YAML 迁移 xlsx 失败: {e}", exc_info=True)

    write_compliance_config_to_xlsx(default_config, CONFIG_PATH)
    logging.info(f"[ComplianceConfig] 创建默认 xlsx 配置文件: {CONFIG_PATH}")


def load_compliance_config() -> Dict:
    """
    加载修饰配置文件
    
    Returns:
        dict: 配置内容 {default: bool, rules: dict}
    """
    _ensure_config_exists()
    
    try:
        if CONFIG_PATH.suffix.lower() == ".xlsx":
            return load_compliance_config_from_xlsx(CONFIG_PATH)

        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
            if not isinstance(config, dict):
                config = {}
            return {
                "default": config.get("default", False),
                "rules": config.get("rules") or {}
            }
    except Exception as e:
        logging.error(f"[ComplianceConfig] 加载配置失败: {e}")
        return {"default": False, "rules": {}}


def _rule_matches_context(
    rule_key: str,
    data_type: str,
    prod_code: str,
    factory: str,
    month: Optional[int] = None,
    week: Optional[int] = None,
) -> bool:
    """Return whether a 1-5 segment rule key matches the supplied context."""
    parts = [part.strip().upper() for part in rule_key.split("-") if part.strip()]
    if not 1 <= len(parts) <= 5:
        return False

    context = [
        str(data_type).upper(),
        str(prod_code).upper(),
        str(factory).upper(),
    ]

    if len(parts) >= 4:
        if month is None:
            return False
        context.append(f"M{month:02d}")

    if len(parts) >= 5:
        if week is None:
            return False
        context.append(f"W{week:02d}")

    for index, rule_part in enumerate(parts):
        if rule_part == "ALL":
            continue
        if index >= len(context) or rule_part != context[index]:
            return False

    return True


def _rule_priority(rule_key: str, order_index: int) -> tuple[int, int, int]:
    """Rank matching rules: deeper keys, fewer wildcards, later entries win."""
    parts = [part.strip().upper() for part in rule_key.split("-") if part.strip()]
    specific_parts = sum(1 for part in parts if part != "ALL")
    return (len(parts), specific_parts, order_index)


def get_compliance_config(
    data_type: str,
    prod_code: str = "ALL",
    factory: str = "ALL",
    month: Optional[int] = None,
    week: Optional[int] = None
) -> bool:
    """
    获取指定组合的修饰状态（1-5段键级联查找）
    
    Args:
        data_type: 监控类型 (SPC/CTQ/AOI/ALL)
        prod_code: 产品型号 (默认 ALL)
        factory: 厂别 (默认 ALL)
        month: ISO 月份 (可选，1-12)
        week: ISO 周号 (可选，1-53)
    
    Returns:
        bool: True = 显示修饰数据, False = 显示真实数据
    
    优先级: 5段 > 4段 > 3段 > 2段 > 1段 > default
    """
    config = load_compliance_config()
    rules = config.get("rules") or {}
    default_value = bool(config.get("default", False))

    best_match: Optional[tuple[tuple[int, int, int], bool]] = None
    for order_index, (rule_key, is_enabled) in enumerate(rules.items()):
        if not _rule_matches_context(rule_key, data_type, prod_code, factory, month, week):
            continue

        priority = _rule_priority(rule_key, order_index)
        if best_match is None or priority > best_match[0]:
            best_match = (priority, bool(is_enabled))

    if best_match is not None:
        return best_match[1]

    return default_value


def save_compliance_config(config: Dict):
    """保存配置到文件"""
    try:
        if CONFIG_PATH.suffix.lower() == ".xlsx":
            write_compliance_config_to_xlsx(config, CONFIG_PATH)
            logging.info(f"[ComplianceConfig] xlsx 配置已保存")
            return True

        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        logging.info(f"[ComplianceConfig] 配置已保存")
        return True
    except Exception as e:
        logging.error(f"[ComplianceConfig] 保存配置失败: {e}")
        return False


def compute_global_compliance_status(
    data_type: str,
    selected_products: list,
    selected_factories: list
) -> bool:
    """
    计算全局修饰状态（1-5段键通配感知）
    
    策略：对每个已选 type * prod * factory 组合，遍历所有规则键。
    段0(type) 必须匹配 data_type（或为 ALL），段1(prod) 匹配产品（或为 ALL/不存在），
    段2(fac) 匹配厂别（或为 ALL/不存在），月/周段忽略。任一命中即返回 True。
    """
    config = load_compliance_config()
    
    for prod in selected_products:
        for factory in selected_factories:
            for rule_key, is_enabled in config["rules"].items():
                if not is_enabled:
                    continue
                parts = rule_key.split("-")
                
                # 段 0: type 必须匹配 data_type (或为 ALL)
                if parts[0].upper() not in (data_type.upper(), "ALL"):
                    continue
                
                # 段 1: prod 必须匹配 (或为 ALL，或规则只有 1 段 = 通配 prod)
                if len(parts) >= 2 and parts[1].upper() not in (prod.upper(), "ALL"):
                    continue
                
                # 段 2: factory 必须匹配 (或为 ALL，或规则只有 <=2 段 = 通配 fac)
                if len(parts) >= 3 and parts[2].upper() not in (factory.upper(), "ALL"):
                    continue
                
                # 段 3+(月/周): 忽略，不影响全局状态判定
                return True
    
    return config["default"]


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
    3. 不涉及任何状态修改操作
    """
    query_params = st.query_params
    is_admin = query_params.get("admin") == "true"
    
    config = load_compliance_config()
    
    with st.expander("🔧 数据修饰配置", expanded=False):
        st.info("当前配置从 `config/compliance_config.xlsx` 加载，刷新页面后生效")
        
        # 显示默认配置
        default_status = "✅ 启用" if config["default"] else "❌ 禁用"
        st.write(f"**默认配置**: {default_status}（当特定组合未配置时使用）")
        
        st.divider()
        
        # 显示当前选中的组合的详细配置
        st.write("**当前选中组合的配置：**")
        
        if selected_products and selected_factories:
            data = []
            for prod in selected_products:
                for factory in selected_factories:
                    key = f"{data_type}-{prod}-{factory}"
                    value = config["rules"].get(key, config["default"])
                    status = "✅ 启用" if value else "❌ 禁用"
                    data.append({
                        "组合": f"{data_type} | {prod} | {factory}",
                        "配置键": key,
                        "状态": status,
                        "修饰数据": value
                    })
            
            st.dataframe(
                data,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "修饰数据": st.column_config.CheckboxColumn(
                        "修饰数据",
                        help="True = 显示修饰后的合规数据",
                        disabled=True  # 只读
                    )
                }
            )
        else:
            st.warning("未选择产品型号或厂别")
        
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
                        if "default" not in new_config or "rules" not in new_config:
                            st.error("配置文件格式错误：必须包含默认配置和规则配置")
                        else:
                            # 保存上传的文件
                            save_compliance_config(new_config)
                            st.success("✅ 配置已更新，请刷新页面生效")
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
            if st.button("🚀 确认覆盖并刷新", type="primary", use_container_width=True, key="btn_scrap_sheet"):
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


def export_config_template() -> str:
    """导出配置模板"""
    template = """# 数据修饰配置文件

# 默认配置（当特定组合未配置时使用）
default: false

# 精细化配置 (1-5段键通用模型，顺序固定: 监控类型-产品型号-厂别-月份-周别)
# 每段支持 ALL 通配；段数越多优先级越高
rules:
  # 1段键: 匹配该类型下任意筛选条件
  # CTQ: true

  # 2段键: 匹配指定类型+产品
  # CTQ-Z571: true

  # 3段键: 匹配指定类型+产品+厂别
  SPC-M626-ARRAY: false
  SPC-M626-OLED: true
  CTQ-M678-ARRAY: false

  # 4段键: 匹配指定类型+产品+厂别+ISO月份
  # CTQ-Z571-OLED-M05: true   # 仅修饰5月数据

  # 5段键: 匹配指定类型+产品+厂别+ISO月份+ISO周
  # CTQ-Z571-OLED-M05-W21: true   # 仅修饰5月第21周数据
  # CTQ-Z571-OLED-M05-W22: false  # 5月第22周显示真实数据

  # ALL 通配示例
  # ALL-Z571-OLED: true   # 任意监控类型下的 Z571 OLED
  # CTQ-ALL-OLED: true    # CTQ 下任意产品的 OLED
"""
    return template
