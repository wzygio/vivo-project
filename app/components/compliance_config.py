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
from pathlib import Path
from typing import Dict, Tuple, Optional
from datetime import datetime


# 配置文件路径
CONFIG_PATH = Path("config/compliance_config.yaml")


def _ensure_config_exists():
    """确保配置文件存在，不存在则创建默认配置"""
    if not CONFIG_PATH.exists():
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        default_config = {
            "default": False,
            "rules": {}
        }
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            yaml.dump(default_config, f, allow_unicode=True, default_flow_style=False)
        logging.info(f"[ComplianceConfig] 创建默认配置文件: {CONFIG_PATH}")


def load_compliance_config() -> Dict:
    """
    加载修饰配置文件
    
    Returns:
        dict: 配置内容 {default: bool, rules: dict}
    """
    _ensure_config_exists()
    
    try:
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


def get_compliance_config(data_type: str, prod_code: str, factory: str) -> bool:
    """
    获取指定组合的修饰状态
    
    Args:
        data_type: 监控类型 (SPC/CTQ/AOI/ALL)
        prod_code: 产品型号
        factory: 厂别
    
    Returns:
        bool: True = 显示修饰数据, False = 显示真实数据
    """
    config = load_compliance_config()
    key = f"{data_type}-{prod_code}-{factory}"
    
    # 优先从 rules 中查找，找不到使用 default
    return config["rules"].get(key, config["default"])


def save_compliance_config(config: Dict):
    """保存配置到文件"""
    try:
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
    计算全局修饰状态
    
    策略：任一选中的组合启用了修饰，则返回 True（保守策略）
    """
    config = load_compliance_config()
    
    for prod in selected_products:
        for factory in selected_factories:
            key = f"{data_type}-{prod}-{factory}"
            if config["rules"].get(key, config["default"]):
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
        st.info("当前配置从 `config/compliance_config.yaml` 加载，刷新页面后生效")
        
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
                with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                    config_content = f.read()
                
                st.download_button(
                    label="📥 下载配置文件",
                    data=config_content,
                    file_name="compliance_config.yaml",
                    mime="text/yaml",
                    help="下载当前配置文件到本地，修改后上传"
                )
            
            with col2:
                # 上传新配置
                uploaded_file = st.file_uploader(
                    "📤 上传配置文件",
                    type=['yaml', 'yml'],
                    help="上传修改后的配置文件（将覆盖原文件）"
                )
                
                if uploaded_file is not None:
                    try:
                        # 验证 YAML 格式
                        new_config = yaml.safe_load(uploaded_file)
                        if "default" not in new_config or "rules" not in new_config:
                            st.error("配置文件格式错误：必须包含 'default' 和 'rules' 字段")
                        else:
                            # 保存上传的文件
                            save_compliance_config(new_config)
                            st.success("✅ 配置已更新，请刷新页面生效")
                    except Exception as e:
                        st.error(f"配置文件解析失败: {e}")
        else:
            st.divider()
            st.info("💡 管理员可通过添加 `?admin=true` 参数到 URL 来获取配置文件管理权限")


def export_config_template() -> str:
    """导出配置模板"""
    template = """# 数据修饰配置文件

# 默认配置（当特定组合未配置时使用）
default: false

# 精细化配置
rules:
  # 格式: {监控类型}-{产品型号}-{厂别}: true/false
  SPC-M626-ARRAY: false
  SPC-M626-OLED: true
  CTQ-M678-ARRAY: false
  # ... 添加更多配置
"""
    return template
