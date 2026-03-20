# src/vivo_project/utils/session_manager.py
import streamlit as st
import logging
from pathlib import Path
import time
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.config_model import AppConfig

class SessionManager:
    """
    [Session 管理器 V2.0 - 智能热重载版]
    负责管理 Streamlit Session State 中的全局状态。
    新增能力：自动检测配置文件变化并热加载。
    """
    
    KEY_PRODUCT = "selected_product_code"
    KEY_CONFIG = "active_app_config"
    KEY_CONFIG_MTIME = "config_file_mtime" # 记录配置文件的最后修改时间
    
    # 定义可用产品列表
    AVAILABLE_PRODUCTS = ConfigLoader.get_enabled_products()

    @staticmethod
    def get_active_config() -> AppConfig:
        """
        获取当前 Session 中的配置对象。
        [增强] 每次调用都会检查硬盘上的配置文件是否更新，如果更新则自动重载。
        """
        # 1. 确保有默认产品
        if SessionManager.KEY_PRODUCT not in st.session_state:
            st.session_state[SessionManager.KEY_PRODUCT] = SessionManager.AVAILABLE_PRODUCTS[0]
            
        current_product = st.session_state[SessionManager.KEY_PRODUCT]
        
        # 2. 检查是否需要(重)加载
        # 情况A: Session里完全没有Config
        # 情况B: 硬盘上的配置文件被修改了 (Hot Reload)
        if SessionManager._needs_reload(current_product):
            SessionManager.load_and_set_config(current_product)
            
        return st.session_state[SessionManager.KEY_CONFIG]

    @staticmethod
    def _needs_reload(product_code: str) -> bool:
        """检查配置文件是否需要重新加载"""
        # 如果内存里没配置，肯定要加载
        if SessionManager.KEY_CONFIG not in st.session_state:
            return True
            
        # 获取硬盘文件的最新修改时间
        try:
            root = ConfigLoader.get_project_root()
            config_file = root / "config" / "products" / f"{product_code}.yaml"
            if not config_file.exists():
                return False # 文件都没了，保持现状吧
            
            current_mtime = config_file.stat().st_mtime
            
            # 获取上次加载时记录的时间
            last_mtime = st.session_state.get(SessionManager.KEY_CONFIG_MTIME, 0)
            
            # 如果硬盘文件比内存里的新，说明用户改了配置
            if current_mtime > last_mtime:
                logging.info(f"⚡ 检测到配置文件变动 ({product_code}.yaml)，触发热重载...")
                return True
                
        except Exception as e:
            logging.warning(f"检查配置文件更新失败: {e}")
            
        return False

    @staticmethod
    def load_and_set_config(product_code: str):
        """加载指定产品的配置并存入 Session，同时更新时间戳"""
        try:
            # 1. 加载配置
            config = ConfigLoader.load_config(product_code)
            
            # 2. 获取文件时间戳
            root = ConfigLoader.get_project_root()
            config_file = root / "config" / "products" / f"{product_code}.yaml"
            mtime = config_file.stat().st_mtime if config_file.exists() else time.time()
            
            # 3. 更新 Session
            st.session_state[SessionManager.KEY_PRODUCT] = product_code
            st.session_state[SessionManager.KEY_CONFIG] = config
            st.session_state[SessionManager.KEY_CONFIG_MTIME] = mtime
            
            # 4. [关键] 配置变了，旧的 Service 缓存(基于旧config对象)也应该失效
            # 虽然 Streamlit 会因为参数(config对象)变化而自动重算，
            # 但显式打印一条日志有助于调试
            logging.info(f"✅ 配置已更新并加载到 Session: {product_code}")
            
        except Exception as e:
            st.error(f"加载产品 {product_code} 配置失败: {e}")
            st.stop()

            
    @staticmethod
    def get_resource_dir() -> Path:
        """获取资源目录路径"""
        return ConfigLoader.get_project_root() / "resources"
    
    @staticmethod
    def get_product_dir() -> Path:
        """获取当前选中的产品代码"""
        return ConfigLoader.get_project_root() / "resources" / st.session_state[SessionManager.KEY_PRODUCT]