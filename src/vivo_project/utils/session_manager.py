# src/vivo_project/utils/session_manager.py
import streamlit as st
from pathlib import Path
from vivo_project.config import ConfigLoader
from vivo_project.config_model import AppConfig

class SessionManager:
    """
    负责管理 Streamlit Session State 中的全局状态：
    1. 当前选中的产品 (Current Product)
    2. 当前生效的配置对象 (Active Config)
    """
    
    KEY_PRODUCT = "selected_product_code"
    KEY_CONFIG = "active_app_config"
    
    # 定义可用产品列表 (后续可以扫描 config/products 目录自动生成)
    AVAILABLE_PRODUCTS = ["M678", "M626"] 

    @staticmethod
    def get_active_config() -> AppConfig:
        """
        获取当前 Session 中的配置对象。
        如果尚未初始化，则加载默认产品。
        """
        if SessionManager.KEY_CONFIG not in st.session_state:
            # 默认初始化
            default_product = SessionManager.AVAILABLE_PRODUCTS[0]
            SessionManager.load_and_set_config(default_product)
            
        return st.session_state[SessionManager.KEY_CONFIG]

    @staticmethod
    def load_and_set_config(product_code: str):
        """加载指定产品的配置并存入 Session"""
        try:
            config = ConfigLoader.load_config(product_code)
            st.session_state[SessionManager.KEY_PRODUCT] = product_code
            st.session_state[SessionManager.KEY_CONFIG] = config
        except Exception as e:
            st.error(f"加载产品 {product_code} 配置失败: {e}")
            st.stop()

    @staticmethod
    def render_product_selector_sidebar():
        """
        在侧边栏渲染产品切换器。
        """
        current_prod = st.session_state.get(SessionManager.KEY_PRODUCT, SessionManager.AVAILABLE_PRODUCTS[0])
        
        selected_prod = st.sidebar.selectbox(
            "📦 选择产品型号",
            options=SessionManager.AVAILABLE_PRODUCTS,
            index=SessionManager.AVAILABLE_PRODUCTS.index(current_prod)
        )
        
        # 如果用户切换了产品，重新加载配置并刷新页面
        if selected_prod != current_prod:
            SessionManager.load_and_set_config(selected_prod)
            st.cache_data.clear() # 清除旧产品的缓存 (可选，视内存情况而定)
            st.rerun()
            
    @staticmethod
    def get_resource_dir() -> Path:
        """获取资源目录路径"""
        return ConfigLoader.get_project_root() / "resources"