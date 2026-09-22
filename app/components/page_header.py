# -*- coding: utf-8 -*-
import hashlib
import logging
from pathlib import Path
from typing import Optional
from uuid import uuid4
import streamlit as st
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.config_model import AppConfig
from app.manager.session_manager import SessionManager
from app.components.indicator_cache import bump_indicator_product_revision

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
    data_forward_signature = ConfigLoader.get_data_forward_policy().signature
    return (
        f"{base_signature}|product={normalized_product}|revision={revision}"
        f"|data_forward={data_forward_signature}"
    )


def invalidate_page_cache(
    cached_funcs: list | None = None,
    *,
    product_code: str | None = None,
    indicator_keys: tuple[str, ...] = (),
) -> str:
    """Invalidate one product when scoped, otherwise preserve legacy global clearing."""
    if product_code:
        if indicator_keys:
            for indicator_key in dict.fromkeys(indicator_keys):
                bump_indicator_product_revision(indicator_key, product_code)
            return "indicator_product"
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
    product_cache_indicators: tuple[str, ...] = (),
    *,
    snapshot_refreshed: bool = False,
) -> None:
    """统一刷新缓存、重载代码、重读配置；也用于快照刷新成功后的收尾。"""
    # ---- 阶段 1: 优先仅失效当前产品；无产品作用域时保留旧的全量清理 ----
    cache_scope = invalidate_page_cache(
        cached_funcs,
        product_code=product_cache_scope,
        indicator_keys=product_cache_indicators,
    )

    # ---- 阶段 2: 清理前端 session_state 视图缓存 ----
    for key in list(st.session_state.keys()):
        if "view_model" in key: # type: ignore
            del st.session_state[key]

    # ---- 阶段 3: 手动热重载 = 代码重载 + 配置重读（总是执行） ----
    # 自动热重载已降级为被动检测（detect_project_changes 只置提示标记），
    # 两个刷新按钮都通过本流程应用代码与配置。
    try:
        from app.utils.reloader import deep_reload_modules
        deep_reload_modules()
        logging.info("♻️ [Hard Reset] 已卸载所有后端模块，下次 import 将加载最新代码。")
    except Exception:
        logging.exception("❌ [Hard Reset] 代码重载失败，刷新流程未完成。")
        raise

    try:
        current_product = product_cache_scope or st.session_state.get(SessionManager.KEY_PRODUCT)
        if current_product:
            SessionManager.load_and_set_config(current_product)
            logging.info(f"♻️ [Hard Reset] 配置已强制重读: {current_product}")
    except Exception:
        logging.exception("❌ [Hard Reset] 配置重读失败，刷新流程未完成。")
        raise

    st.session_state.pop("code_update_pending", None)

    # ---- 阶段 4: 清除页面级签名/视图状态，按钮点击后才允许缓存失效 ----
    for key in list(st.session_state.keys()):
        key_str = str(key)
        if (
            key_str.startswith("yield_composite_key_")
            or key_str.startswith("yield_snapshot_sig_")
            or key_str.startswith("spc_snapshot_sig_")
            or key_str.startswith("matrix_detail_")
            or key_str == "parts_baseline_sig"
            or key_str == "alert_matrix_board_loaded"
            or key_str == "monitor_query_signature"
            or key_str == "cpk_monitor_query_signature"
        ):
            del st.session_state[key]

    if cache_scope == "indicator_product":
        cache_message = f"{product_cache_scope} 当前报表指标缓存已刷新"
    elif cache_scope == "product":
        cache_message = f"{product_cache_scope} 缓存已刷新"
    else:
        cache_message = "缓存已刷新"
    snapshot_message = "数据快照已刷新 · " if snapshot_refreshed else ""
    st.toast(f"🔄 {snapshot_message}{cache_message} · 代码与配置已重载", icon="✅")


def render_page_header(
    title: Optional[str] = None,
    config: AppConfig = None,
    cached_funcs: list = None,
    refresh_handlers: list = None,
    product_cache_scope: str | None = None,
    show_product_filter: bool = True,
    product_cache_indicators: tuple[str, ...] = (),
    show_cache_refresh: bool = True,
    show_data_refresh: bool = True,
) -> None:
    # 每个报表页面都会经过统一页头；在渲染或查询数据前完成项目变更的被动检测。
    # 检测只置位提示标记，绝不打断当前 run；两个刷新按钮均可手动应用代码/配置/缓存更新。
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

    # 快照全部刷新成功后，复用「刷新缓存」的缓存失效、代码重载与配置重读流程。
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

        perform_hard_reset(
            cached_funcs,
            product_cache_scope,
            product_cache_indicators,
            snapshot_refreshed=True,
        )
        logging.info("🔄 [UI] 数据快照、页面缓存、代码与配置刷新完毕。")

    # 数据缓存按页面作用域失效；所有页面的「刷新缓存」均执行进程级代码重载。
    def _hard_reset_callback():
        perform_hard_reset(cached_funcs, product_cache_scope, product_cache_indicators)

    # 产品筛选与管理员操作使用独立边框分组，避免把常规筛选误认为维护操作。
    # show_product_filter=False（如 Q-Time 页，产品多选内聚在页面筛选区）时
    # 跳过产品 selectbox，管理员操作列布局不变。
    product_column, admin_column = st.columns(
        [2, 4],
        vertical_alignment="bottom",
    )
    if show_product_filter:
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
                    if show_data_refresh:
                        st.button(
                            "🔄 刷新数据",
                            key=f"btn_refresh_{title}",
                            on_click=_refresh_data_callback,
                            width="stretch",
                            help=(
                                f"刷新底层数据快照，成功后刷新产品 {product_cache_scope} 的页面缓存，并重载代码、重读配置。"
                                if product_cache_scope
                                else "刷新底层数据快照，成功后刷新当前页面缓存，并重载代码、重读配置。"
                            ),
                        )
                    if show_cache_refresh:
                        st.button(
                            "🔄 刷新缓存",
                            key=f"btn_clear_{title}",
                            on_click=_hard_reset_callback,
                            width="stretch",
                            help=(
                                f"刷新产品 {product_cache_scope} 的当前报表指标缓存，并重载代码与配置。"
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
    由两个刷新按钮共用的 perform_hard_reset 手动触发；刷新数据先完成快照刷新。

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

