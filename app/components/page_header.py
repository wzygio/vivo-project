# -*- coding: utf-8 -*-
import streamlit as st
import os
from pathlib import Path
from typing import Optional

from src.shared_kernel.config_model import AppConfig
from app.utils.session_manager import SessionManager

DEFAULT_CACHE_TTL = 4 * 60 * 60  # 4 Hours

def render_page_header(
    title: Optional[str] = None, 
    config: AppConfig = None, 
    cached_funcs: list = None,
    refresh_handlers: list = None
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
        
        # [核心修复] 强制页面重新加载，确保用户立即看到刷新后的数据
        st.rerun()

    # [企业级] 精准 "清除缓存 + 模块重载" 回调
    #
    # 执行顺序必须严格保持以下链条：
    #   1. func.clear()         ─ 旧模块函数尚存活，.clear() 正常生效
    #   2. deep_reload_modules()─ 卸载 sys.modules 中的旧模块
    #   3. 清除 composite_key   ─ 强制下次重算 project_rev
    #   4. st.rerun()           ─ 重新 import → 加载新代码 → 新 project_rev → 新 composite_key
    def _hard_reset_callback():
        # ---- 阶段 1: 清除数据缓存 (旧模块函数仍有效) ----
        if cached_funcs:
            for func in cached_funcs:
                if hasattr(func, "clear"):
                    func.clear()
        else:
            st.cache_data.clear()
            st.cache_resource.clear()
        
        # ---- 阶段 2: 清理前端 session_state 视图缓存 ----
        for key in list(st.session_state.keys()):
            if "view_model" in key: # type: ignore
                del st.session_state[key]
        
        # ---- 阶段 3: 卸载旧模块，强制下次 import 读取磁盘新代码 ----
        try:
            from app.utils.reloader import deep_reload_modules
            deep_reload_modules()
            logging.info("♻️ [Hard Reset] 已卸载所有后端模块，下次 import 将加载最新代码。")
        except ImportError:
            logging.warning("⚠️ 模块重载依赖缺失，跳过 (仅清除缓存)。")
        
        # ---- 阶段 4: 清除 composite_key 签名缓存，强制重算 ----
        # 新 Session 重算时 project_rev 会包含新代码的 mtime → key 不同 → 缓存自动 miss
        for key in list(st.session_state.keys()):
            key_str = str(key)
            if key_str.startswith("yield_composite_key_") or key_str.startswith("yield_snapshot_sig_") or key_str.startswith("spc_snapshot_sig_"):
                del st.session_state[key]
        
        st.toast("🧹 缓存已清除 · 模块已重载", icon="✅")
        st.rerun()

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
        from app.utils.reloader import deep_reload_modules, get_project_revision, get_project_revision
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
            
            # [核心修复] 同步清除 yield 域的 composite_key 缓存，强制下次重新计算签名
            for key in list(st.session_state.keys()):
                if key.startswith("yield_composite_key_") or key.startswith("yield_snapshot_sig_"):
                    del st.session_state[key]
            
        # 4. 更新指纹
        st.session_state['last_code_revision'] = current_rev
        
    except ImportError as e:
        import logging
        logging.warning(f"⚠️ 热重载模块依赖缺失，已跳过: {e}")

