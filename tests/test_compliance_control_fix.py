"""
测试程序：验证 Compliance Control 批量操作后 toggle 状态同步问题

运行方式：streamlit run tests/test_compliance_control_fix.py
"""

import streamlit as st
import json
from pathlib import Path
from datetime import datetime

# ==============================================================================
# 模拟的 ComplianceStateManager（简化版）
# ==============================================================================
class MockComplianceManager:
    """模拟的状态管理器"""
    
    def __init__(self):
        self._cache = {}
        self._load()
    
    def _load(self):
        """从 session state 加载（模拟文件加载）"""
        if 'mock_compliance_config' not in st.session_state:
            st.session_state.mock_compliance_config = {}
        self._cache = st.session_state.mock_compliance_config
    
    def _save(self):
        """保存到 session state"""
        st.session_state.mock_compliance_config = self._cache
    
    def get(self, key):
        return self._cache.get(key, False)
    
    def set(self, key, value):
        self._cache[key] = value
        self._save()
    
    def clear(self):
        self._cache = {}
        self._save()


# ==============================================================================
# 方案 A：当前实现（有问题）
# ==============================================================================
def render_problematic_version(data_type, products, factories):
    """当前有问题的实现 - 用于复现 bug"""
    st.markdown("### 方案 A：当前实现（有问题）")
    st.warning("点击'全部启用'后，下方 toggle 不会立即改变")
    
    manager = MockComplianceManager()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("全部启用", key="prob_enable"):
            for prod in products:
                for factory in factories:
                    manager.set(f"{data_type}_{prod}_{factory}", True)
            st.rerun()
    
    with col2:
        if st.button("全部禁用", key="prob_disable"):
            for prod in products:
                for factory in factories:
                    manager.set(f"{data_type}_{prod}_{factory}", False)
            st.rerun()
    
    with col3:
        if st.button("重置", key="prob_reset"):
            manager.clear()
            st.rerun()
    
    st.divider()
    
    # 显示 toggle
    for prod in products:
        for factory in factories:
            key = f"{data_type}_{prod}_{factory}"
            config_key = f"prob_toggle_{key}"
            
            # 从文件/session state 读取值
            current_value = manager.get(key)
            
            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.text(f"{data_type} | {prod} | {factory}")
            with col_b:
                # 问题：value=current_value 在 rerun 后不会生效
                # 因为 toggle 会优先使用 session state 中保存的旧值
                new_value = st.toggle(
                    "修饰数据",
                    value=current_value,
                    key=config_key
                )
                if new_value != current_value:
                    manager.set(key, new_value)
                    st.rerun()
            
            # 显示实际值（用于调试）
            st.caption(f"实际值: {current_value} | 组件返回值: {new_value}")


# ==============================================================================
# 方案 B：修复版本 - 使用 session_state 强制同步
# ==============================================================================
def render_fixed_version(data_type, products, factories):
    """修复后的实现"""
    st.markdown("### 方案 B：修复版本（推荐）")
    st.success("点击'全部启用'后，下方 toggle 会立即同步")
    
    manager = MockComplianceManager()
    
    # 初始化组件状态：将文件值同步到 session_state
    for prod in products:
        for factory in factories:
            key = f"{data_type}_{prod}_{factory}"
            config_key = f"fixed_toggle_{key}"
            
            # 如果 session state 中没有该 key，或需要强制刷新
            if config_key not in st.session_state:
                st.session_state[config_key] = manager.get(key)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("全部启用", key="fix_enable"):
            for prod in products:
                for factory in factories:
                    key = f"{data_type}_{prod}_{factory}"
                    config_key = f"fixed_toggle_{key}"
                    # 同时更新持久化存储和 session state
                    manager.set(key, True)
                    st.session_state[config_key] = True
            st.rerun()
    
    with col2:
        if st.button("全部禁用", key="fix_disable"):
            for prod in products:
                for factory in factories:
                    key = f"{data_type}_{prod}_{factory}"
                    config_key = f"fixed_toggle_{key}"
                    manager.set(key, False)
                    st.session_state[config_key] = False
            st.rerun()
    
    with col3:
        if st.button("重置", key="fix_reset"):
            manager.clear()
            # 清空所有相关的 session state
            for key in list(st.session_state.keys()):
                if key.startswith("fixed_toggle_"):
                    del st.session_state[key]
            st.rerun()
    
    st.divider()
    
    # 显示 toggle
    for prod in products:
        for factory in factories:
            key = f"{data_type}_{prod}_{factory}"
            config_key = f"fixed_toggle_{key}"
            
            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.text(f"{data_type} | {prod} | {factory}")
            with col_b:
                # 使用 session_state 中的值
                # 注意：这里用 key 而不是 value，让 Streamlit 从 session state 读取
                new_value = st.toggle(
                    "修饰数据",
                    key=config_key  # 通过 key 从 session state 自动读取值
                )
                
                # 同步回持久化存储
                if new_value != manager.get(key):
                    manager.set(key, new_value)
                    st.rerun()
            
            # 显示实际值（用于调试）
            actual_value = manager.get(key)
            session_value = st.session_state.get(config_key, "N/A")
            st.caption(f"持久化值: {actual_value} | Session值: {session_value}")


# ==============================================================================
# 方案 C：修复版本 - 使用动态 key 强制刷新
# ==============================================================================
def render_dynamic_key_version(data_type, products, factories):
    """使用动态 key 的修复方案"""
    st.markdown("### 方案 C：动态 Key 版本（备选）")
    st.info("通过改变 key 强制重新创建 toggle 组件")
    
    manager = MockComplianceManager()
    
    # 使用版本号来强制刷新
    if 'key_version' not in st.session_state:
        st.session_state.key_version = 0
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("全部启用", key="dyn_enable"):
            for prod in products:
                for factory in factories:
                    manager.set(f"{data_type}_{prod}_{factory}", True)
            st.session_state.key_version += 1
            st.rerun()
    
    with col2:
        if st.button("全部禁用", key="dyn_disable"):
            for prod in products:
                for factory in factories:
                    manager.set(f"{data_type}_{prod}_{factory}", False)
            st.session_state.key_version += 1
            st.rerun()
    
    with col3:
        if st.button("重置", key="dyn_reset"):
            manager.clear()
            st.session_state.key_version += 1
            st.rerun()
    
    st.divider()
    
    # 显示 toggle - 使用动态 key
    version = st.session_state.key_version
    for prod in products:
        for factory in factories:
            key = f"{data_type}_{prod}_{factory}"
            # key 包含版本号，每次版本变化都会重新创建组件
            config_key = f"dyn_toggle_{key}_v{version}"
            
            current_value = manager.get(key)
            
            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.text(f"{data_type} | {prod} | {factory}")
            with col_b:
                new_value = st.toggle(
                    "修饰数据",
                    value=current_value,
                    key=config_key
                )
                if new_value != current_value:
                    manager.set(key, new_value)
                    st.rerun()
            
            st.caption(f"实际值: {current_value}")


# ==============================================================================
# 主程序
# ==============================================================================
def main():
    st.set_page_config(page_title="Compliance Control 修复测试", layout="wide")
    st.title("🔧 Compliance Control Toggle 同步问题测试")
    
    st.markdown("""
    ## 问题描述
    点击"全部启用/禁用"后，下方的 toggle 开关状态不会立即改变，需要手动刷新页面才能看到变化。
    
    ## 测试数据
    - 监控类型: SPC
    - 产品型号: M626, M678
    - 厂别: ARRAY, OLED
    
    ## 测试步骤
    1. 展开每个方案的折叠面板
    2. 点击"全部启用"或"全部禁用"
    3. 观察下方 toggle 是否立即同步变化
    """)
    
    # 测试数据
    data_type = "SPC"
    products = ["M626", "M678"]
    factories = ["ARRAY", "OLED"]
    
    # 方案 A：问题版本
    with st.expander("❌ 方案 A：当前有问题的实现", expanded=True):
        render_problematic_version(data_type, products, factories)
    
    st.divider()
    
    # 方案 B：修复版本（推荐）
    with st.expander("✅ 方案 B：使用 Session State 同步（推荐）", expanded=True):
        render_fixed_version(data_type, products, factories)
    
    st.divider()
    
    # 方案 C：动态 key 版本
    with st.expander("ℹ️ 方案 C：使用动态 Key（备选）", expanded=False):
        render_dynamic_key_version(data_type, products, factories)
    
    st.divider()
    st.markdown("""
    ## 修复建议
    
    **推荐方案 B**，原因：
    1. 使用 session state 同步，不会丢失用户手动设置的状态
    2. 不需要重新创建组件，性能好
    3. 代码改动小，易于维护
    
    核心修改：
    ```python
    # 批量操作时，同时更新持久化存储和 session state
    if st.button("全部启用"):
        for key in all_keys:
            manager.set(key, True)  # 更新文件
            st.session_state[f"toggle_{key}"] = True  # 更新 session state
        st.rerun()
    
    # toggle 使用 key 而不是 value，自动读取 session state
    new_value = st.toggle("修饰数据", key=f"toggle_{key}")
    ```
    """)


if __name__ == "__main__":
    main()
