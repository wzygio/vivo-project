"""User-visible Yield source health, independent of defect judgement."""

import streamlit as st


def render_yield_data_health(health: dict, *, admin_only: bool = True) -> None:
    """Show health only for ``?admin=true`` unless ``admin_only=False``.

    Unavailable data always stops the page, regardless of visibility.
    """
    is_visible = not admin_only or st.query_params.get("admin") == "true"
    status = health.get("status", "unknown")
    if status == "unavailable":
        if is_visible:
            st.error("入库数据暂不可用，无法判断当前质量状态。请刷新重试。")
        st.stop()
    if not is_visible:
        return
    if status == "stale":
        st.warning("源数据刷新失败，以下结果使用旧快照，不能据此认定当前无异常。")
    elif status in ("unknown", "partial"):
        st.warning("数据完整性或新鲜度尚未确认，以下结果仅供参考。")
    if health.get("refreshed_at") and status in ("fresh", "stale"):
        st.caption(
            f"最后成功更新：{health['refreshed_at']}；"
            f"源数据窗口：{health.get('source_start', '')} 至 {health.get('source_end', '')}"
        )
