# -*- coding: utf-8 -*-
import hashlib
import logging
from pathlib import Path
from typing import Optional
from uuid import uuid4
import streamlit as st
from src.shared_kernel.config_model import AppConfig
from app.manager.session_manager import SessionManager

DEFAULT_CACHE_TTL = 4 * 60 * 60  # 4 Hours
PRODUCT_CACHE_REVISION_DIR = Path("output") / "tmp" / "product_cache_revisions"


def _product_cache_revision_path(
    product_code: str,
    revision_dir: Path = PRODUCT_CACHE_REVISION_DIR,
) -> Path:
    normalized_product = str(product_code).strip().upper()
    product_digest = hashlib.sha256(normalized_product.encode("utf-8")).hexdigest()[:16]
    return revision_dir / f"{product_digest}.revision"


def get_product_cache_revision(
    product_code: str,
    *,
    revision_dir: Path = PRODUCT_CACHE_REVISION_DIR,
) -> str:
    """Return the shared invalidation revision for one product."""
    revision_path = _product_cache_revision_path(product_code, revision_dir)
    try:
        revision = revision_path.read_text(encoding="utf-8").strip()
    except (FileNotFoundError, OSError):
        return "0"
    return revision or "0"


def bump_product_cache_revision(
    product_code: str,
    *,
    revision_dir: Path = PRODUCT_CACHE_REVISION_DIR,
) -> str:
    """Invalidate one product's versioned cache keys without touching other products."""
    revision_path = _product_cache_revision_path(product_code, revision_dir)
    revision_path.parent.mkdir(parents=True, exist_ok=True)
    revision = uuid4().hex
    temporary_path = revision_path.with_name(f"{revision_path.name}.{revision}.tmp")
    temporary_path.write_text(revision, encoding="utf-8")
    temporary_path.replace(revision_path)
    return revision


def build_product_cache_signature(
    base_signature: str,
    product_code: str,
    *,
    revision_dir: Path = PRODUCT_CACHE_REVISION_DIR,
) -> str:
    """Attach the selected product's shared revision to a page cache signature."""
    normalized_product = str(product_code).strip().upper()
    revision = get_product_cache_revision(
        normalized_product,
        revision_dir=revision_dir,
    )
    return f"{base_signature}|product={normalized_product}|revision={revision}"


def invalidate_page_cache(
    cached_funcs: list | None = None,
    *,
    product_code: str | None = None,
) -> str:
    """Invalidate one product when scoped, otherwise preserve legacy global clearing."""
    if product_code:
        bump_product_cache_revision(product_code)
        return "product"

    if cached_funcs:
        for func in cached_funcs:
            if hasattr(func, "clear"):
                func.clear()
    else:
        st.cache_data.clear()
        st.cache_resource.clear()
    return "global"


def perform_hard_reset(
    cached_funcs: list | None = None,
    product_cache_scope: str | None = None,
) -> None:
    """执行「刷新缓存」的完整硬重置流程，可被页头按钮或独立页面按钮复用。"""
    # ---- 阶段 1: 优先仅失效当前产品；无产品作用域时保留旧的全量清理 ----
    cache_scope = invalidate_page_cache(
        cached_funcs,
        product_code=product_cache_scope,
    )

    # ---- 阶段 2: 清理前端 session_state 视图缓存 ----
    for key in list(st.session_state.keys()):
        if "view_model" in key: # type: ignore
            del st.session_state[key]

    # ---- 阶段 3: 手动热重载 = 代码重载 + 配置重读（总是执行） ----
    # 自动热重载已降级为被动检测（detect_project_changes 只置提示标记），
    # 代码与配置的统一生效全部收敛到本按钮。
    try:
        from app.utils.reloader import deep_reload_modules
        deep_reload_modules()
        logging.info("♻️ [Hard Reset] 已卸载所有后端模块，下次 import 将加载最新代码。")
    except ImportError:
        logging.warning("⚠️ 模块重载依赖缺失，跳过 (仅刷新缓存)。")

    try:
        current_product = st.session_state.get(SessionManager.KEY_PRODUCT)
        if current_product:
            SessionManager.load_and_set_config(current_product)
            logging.info(f"♻️ [Hard Reset] 配置已强制重读: {current_product}")
    except Exception as exc:
        logging.warning(f"⚠️ [Hard Reset] 配置重读失败，保留现有配置: {exc}")

    st.session_state.pop("code_update_pending", None)

    # ---- 阶段 4: 清除页面级签名/视图状态，按钮点击后才允许缓存失效 ----
    for key in list(st.session_state.keys()):
        key_str = str(key)
        if (
            key_str.startswith("yield_composite_key_")
            or key_str.startswith("yield_snapshot_sig_")
            or key_str.startswith("spc_snapshot_sig_")
            or key_str == "parts_baseline_sig"
        ):
            del st.session_state[key]

    if cache_scope == "product":
        st.toast(f"🔄 {product_cache_scope} 缓存已刷新 · 代码与配置已重载", icon="✅")
    else:
        st.toast("🔄 缓存已刷新 · 代码与配置已重载", icon="✅")


def render_page_header(
    title: Optional[str] = None,
    config: AppConfig = None,
    cached_funcs: list = None,
    refresh_handlers: list = None,
    product_cache_scope: str | None = None,
) -> None:
    # 每个报表页面都会经过统一页头；在渲染或查询数据前完成项目变更的被动检测。
    # 检测只置位提示标记，绝不打断当前 run；代码/配置/缓存的统一生效由"刷新缓存"手动触发。
    detect_project_changes()

    is_admin = st.query_params.get("admin") == "true"

    # Admin 隐身模式 (Stealth Mode)
    # 只要 URL 中没有 ?admin=true，就利用 CSS 抹除侧边栏中的特定页面
    if not is_admin:
        st.markdown(
            """
            <style>
            /* 使用属性选择器精准狙击 href 包含特定名称的 <a> 标签 */
            /* 兼容明文中文和 URL Encode 编码格式 */
            [data-testid="stSidebarNav"] a[href*="自动预警看板"],
            [data-testid="stSidebarNav"] a[href*="%E8%87%AA%E5%8A%A8%E9%A2%84%E8%AD%A6%E7%9C%8B%E6%9D%BF"] {
                display: none !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

    if title:
        st.title(title)

    # [L1+L2] 刷新底层数据快照，成功后同步失效当前页面的 L2 缓存。
    # 模块卸载与配置重读仍只属于「刷新缓存」(_hard_reset_callback)。
    def _refresh_data_callback():
        if not refresh_handlers:
            st.toast("当前页面没有独立的数据快照刷新任务。", icon="ℹ️")
            return

        all_success = True
        for handler in refresh_handlers:
            if callable(handler):
                try:
                    is_success = handler()
                except Exception:
                    # handler 抛异常同样视为失败：不推进 revision、不清 L2。
                    logging.exception("❌ [UI] L1 数据快照刷新任务执行异常。")
                    is_success = False
                if is_success is False:
                    all_success = False

        if not all_success:
            st.toast("❌ 数据库连接或快照更新失败，已保留当前缓存视图。", icon="🚨")
            return

        # 快照全部刷新成功后失效 L2：产品页面仅推进当前产品的共享 revision；
        # 无产品作用域但有缓存函数时保留旧的全量 func.clear() 语义。
        if product_cache_scope:
            invalidate_page_cache(cached_funcs, product_code=product_cache_scope)
        elif cached_funcs:
            invalidate_page_cache(cached_funcs)

        # 清理前端 memo 化的视图模型缓存（与 _hard_reset_callback 阶段 2 一致）。
        for key in list(st.session_state.keys()):
            if "view_model" in key: # type: ignore
                del st.session_state[key]

        st.toast("✅ L1 快照与 L2 缓存已刷新。", icon="🎉")
        logging.info("🔄 [UI] L1 数据快照刷新完毕，已同步失效 L2 页面缓存。")

    # 产品页面通过共享版本键仅失效当前产品；聚合/无产品页面保留旧的
    # func.clear() + 模块重载行为。
    def _hard_reset_callback():
        perform_hard_reset(cached_funcs, product_cache_scope)

    # 产品筛选与管理员操作使用独立边框分组，避免把常规筛选误认为维护操作。
    product_column, admin_column = st.columns(
        [2, 4],
        vertical_alignment="bottom",
    )
    with product_column:
        with st.container(border=True):
            st.caption("产品筛选")
            current_prod = config.data_source.product_code
            available_prods = SessionManager.AVAILABLE_PRODUCTS
            selected_prod = st.selectbox(
                "📦 当前产品型号",
                options=available_prods,
                index=available_prods.index(current_prod) if current_prod in available_prods else 0,
                key=f"header_prod_sel_{title}",
                label_visibility="collapsed",
            )
            if selected_prod != current_prod:
                SessionManager.load_and_set_config(selected_prod)
                st.rerun()

    if is_admin:
        with admin_column:
            with st.container(border=True):
                st.caption("管理员操作")
                with st.container(horizontal=True):
                    st.button(
                        "🔄 刷新数据",
                        key=f"btn_refresh_{title}",
                        on_click=_refresh_data_callback,
                        width="stretch",
                        help=(
                            f"刷新底层 L1 数据快照，并同步刷新产品 {product_cache_scope} 的 L2 页面缓存。"
                            if product_cache_scope
                            else "刷新底层 L1 数据快照，并同步刷新当前页面的 L2 缓存。"
                        ),
                    )
                    st.button(
                        "🔄 刷新缓存",
                        key=f"btn_clear_{title}",
                        on_click=_hard_reset_callback,
                        width="stretch",
                        help=(
                            f"仅刷新产品 {product_cache_scope} 的当前报表缓存，并重载代码与配置。"
                            if product_cache_scope
                            else "清除当前报表缓存并重载代码与配置；普通浏览器刷新不会触发。"
                        ),
                    )
                if st.session_state.get("code_update_pending"):
                    st.caption("⚠️ 检测到项目文件变更，点击「刷新缓存」应用")


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

def detect_project_changes(enable: bool = True) -> bool:
    """
    [企业级工具] 项目变更被动检测守卫（手动热重载模式）。

    监控代码/配置/资源文件的哈希指纹变化，但绝不打断当前 run：
    发现变更时仅置位 st.session_state['code_update_pending'] 提示标记，
    由页头渲染"点击刷新缓存应用"的提示。代码重载、配置重读与缓存失效
    统一收敛到"刷新缓存"按钮（_hard_reset_callback）手动触发。

    Returns:
        bool: 本次运行是否检测到了变更（供调用方判断/测试）。
    """
    if not enable:
        return False

    try:
        from app.utils.reloader import get_project_revision
        from src.shared_kernel.config import ConfigLoader

        # 1. 计算当前代码目录的真实哈希指纹
        project_root = ConfigLoader.get_project_root()
        current_rev = get_project_revision(project_root)

        # 2. 从 session_state 获取上一次的指纹
        last_rev = st.session_state.get('last_code_revision')

        # 3. 先更新指纹，保证同一轮变更只提示一次。
        st.session_state['last_code_revision'] = current_rev

        if last_rev is None or last_rev == current_rev:
            return False

        # 4. 仅置提示标记；不卸载模块、不 rerun，绝不打断用户操作。
        logging.info("🔔 探测到项目文件变更，等待用户点击「刷新缓存」手动应用。")
        st.session_state['code_update_pending'] = True
        return True

    except (ImportError, OSError) as error:
        logging.warning(f"⚠️ 项目变更检测失败，已跳过: {error}")
        return False

