# app/components/compliance_control.py
"""
数据修饰控制组件 (Compliance Control Component)

功能：
1. 提供精细化数据修饰开关控制（监控类型-产品型号-厂别）
2. 状态持久化到本地JSON文件，页面刷新不丢失
3. 支持导入/导出配置
"""

import streamlit as st
import json
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ComplianceKey:
    """合规配置键"""
    data_type: str
    prod_code: str
    factory: str
    
    def to_tuple(self) -> Tuple[str, str, str]:
        return (self.data_type, self.prod_code, self.factory)
    
    def to_string(self) -> str:
        return f"{self.data_type}-{self.prod_code}-{self.factory}"


class ComplianceStateManager:
    """
    合规状态管理器
    
    负责：
    - 从本地文件加载/保存配置
    - 提供默认配置
    - 管理配置版本
    """
    
    CONFIG_FILE = Path("config/compliance_state.json")
    VERSION = "1.0"
    
    def __init__(self):
        self._ensure_config_dir()
        self._state_cache: Dict[str, bool] = {}
        self._load_state()
    
    def _ensure_config_dir(self):
        """确保配置目录存在"""
        self.CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    def _load_state(self):
        """从文件加载状态"""
        if self.CONFIG_FILE.exists():
            try:
                with open(self.CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 验证版本兼容性
                    if data.get('version') == self.VERSION:
                        self._state_cache = data.get('states', {})
                        logging.info(f"[ComplianceState] 已加载 {len(self._state_cache)} 条配置")
                    else:
                        logging.warning("[ComplianceState] 配置文件版本不兼容，使用默认配置")
                        self._state_cache = {}
            except Exception as e:
                logging.error(f"[ComplianceState] 加载配置失败: {e}")
                self._state_cache = {}
        else:
            logging.info("[ComplianceState] 配置文件不存在，创建新配置")
            self._state_cache = {}
    
    def _save_state(self):
        """保存状态到文件"""
        try:
            data = {
                'version': self.VERSION,
                'last_updated': datetime.now().isoformat(),
                'states': self._state_cache
            }
            with open(self.CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"[ComplianceState] 保存配置失败: {e}")
    
    def get(self, data_type: str, prod_code: str, factory: str) -> bool:
        """
        获取指定组合的修饰状态
        
        Returns:
            bool: True = 显示修饰数据, False = 显示真实数据
        """
        key = ComplianceKey(data_type, prod_code, factory).to_string()
        
        # 如果未配置，使用默认策略：非管理员默认修饰
        if key not in self._state_cache:
            query_params = st.query_params
            is_admin = query_params.get("admin") == "true"
            return not is_admin  # 非管理员默认启用修饰
        
        return self._state_cache[key]
    
    def set(self, data_type: str, prod_code: str, factory: str, value: bool):
        """设置指定组合的修饰状态"""
        key = ComplianceKey(data_type, prod_code, factory).to_string()
        self._state_cache[key] = value
        self._save_state()  # 立即持久化
        logging.info(f"[ComplianceState] 设置 {key} = {value}")
    
    def get_all_states(self) -> Dict[str, bool]:
        """获取所有状态（用于导出）"""
        return self._state_cache.copy()
    
    def import_states(self, states: Dict[str, bool]):
        """导入配置（用于批量恢复）"""
        self._state_cache.update(states)
        self._save_state()
    
    def clear_all(self):
        """清空所有配置"""
        self._state_cache = {}
        self._save_state()
        logging.info("[ComplianceState] 已清空所有配置")


# 全局单例
_compliance_manager: Optional[ComplianceStateManager] = None


def get_compliance_manager() -> ComplianceStateManager:
    """获取合规状态管理器单例"""
    global _compliance_manager
    if _compliance_manager is None:
        _compliance_manager = ComplianceStateManager()
    return _compliance_manager


def get_compliance_config(data_type: str, prod_code: str, factory: str) -> bool:
    """
    获取指定组合的合规修饰配置（便捷函数）
    
    Returns:
        bool: True = 显示修饰数据, False = 显示真实数据
    """
    manager = get_compliance_manager()
    return manager.get(data_type, prod_code, factory)


def render_compliance_control_panel(
    data_type: str,
    selected_products: list,
    selected_factories: list,
    key_prefix: str = ""
) -> Dict[Tuple[str, str, str], bool]:
    """
    渲染数据修饰控制面板
    
    Args:
        data_type: 当前监控类型 (SPC/CTQ/AOI/ALL)
        selected_products: 选中的产品型号列表
        selected_factories: 选中的厂别列表
        key_prefix: Streamlit 组件 key 前缀
    
    Returns:
        Dict: 当前所有组合的配置状态
    """
    manager = get_compliance_manager()
    
    # 检查是否为管理员
    query_params = st.query_params
    is_admin = query_params.get("admin") == "true"
    
    # 如果不是管理员，不显示控制面板但返回配置
    if not is_admin:
        # 为所有组合返回默认配置
        configs = {}
        for prod in selected_products:
            for factory in selected_factories:
                key = (data_type, prod, factory)
                configs[key] = manager.get(data_type, prod, factory)
        return configs
    
    # 管理员：显示控制面板
    with st.expander("🔧 数据修饰配置 (按监控类型-产品-厂别)", expanded=False):
        
        # 批量操作按钮
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("全部启用修饰", key=f"{key_prefix}enable_all"):
                for prod in selected_products:
                    for factory in selected_factories:
                        manager.set(data_type, prod, factory, True)
                st.rerun()
        with col2:
            if st.button("全部禁用修饰", key=f"{key_prefix}disable_all"):
                for prod in selected_products:
                    for factory in selected_factories:
                        manager.set(data_type, prod, factory, False)
                st.rerun()
        with col3:
            if st.button("重置为默认", key=f"{key_prefix}reset_all"):
                manager.clear_all()
                st.rerun()
        
        st.divider()
        
        # 为每个组合显示开关
        configs = {}
        for prod in selected_products:
            for factory in selected_factories:
                key = (data_type, prod, factory)
                config_key = f"{key_prefix}compliance_{data_type}_{prod}_{factory}"
                
                # 获取当前值（从持久化存储）
                current_value = manager.get(data_type, prod, factory)
                
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.text(f"{data_type} | {prod} | {factory}")
                with col_b:
                    new_value = st.toggle(
                        "修饰数据",
                        value=current_value,
                        key=config_key,
                        help="开启: 显示修饰后的合规数据, 关闭: 显示真实原始数据"
                    )
                    
                    # 如果值变化，更新持久化存储
                    if new_value != current_value:
                        manager.set(data_type, prod, factory, new_value)
                        st.rerun()
                
                configs[key] = new_value
    
    return configs


def compute_global_compliance_status(
    configs: Dict[Tuple[str, str, str], bool]
) -> bool:
    """
    计算全局修饰状态
    
    策略：如果任一组合启用了修饰，则返回 True（保守策略）
    
    Args:
        configs: 所有组合的配置状态
    
    Returns:
        bool: 是否全局启用修饰
    """
    if not configs:
        # 默认从 URL 参数获取
        query_params = st.query_params
        is_admin = query_params.get("admin") == "true"
        return not is_admin
    
    return any(configs.values())


def export_compliance_config() -> str:
    """
    导出配置为JSON字符串
    
    Returns:
        str: JSON 格式的配置
    """
    manager = get_compliance_manager()
    data = {
        'version': ComplianceStateManager.VERSION,
        'exported_at': datetime.now().isoformat(),
        'states': manager.get_all_states()
    }
    return json.dumps(data, ensure_ascii=False, indent=2)


def import_compliance_config(json_str: str) -> bool:
    """
    从JSON字符串导入配置
    
    Args:
        json_str: JSON 格式的配置
    
    Returns:
        bool: 是否导入成功
    """
    try:
        data = json.loads(json_str)
        if data.get('version') != ComplianceStateManager.VERSION:
            logging.warning("导入的配置文件版本不兼容")
            return False
        
        manager = get_compliance_manager()
        manager.import_states(data.get('states', {}))
        return True
    except Exception as e:
        logging.error(f"导入配置失败: {e}")
        return False
