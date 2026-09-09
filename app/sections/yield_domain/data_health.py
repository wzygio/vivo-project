"""User-visible Yield source health, independent of defect judgement."""

import streamlit as st


def render_yield_data_health(health: dict) -> None:
    status = health.get("status", "unknown")
    if status == "unavailable":
        st.error("入库数据暂不可用，无法判断当前质量状态。请刷新重试。")
        st.stop()
    if status == "stale":
        st.warning("源数据刷新失败，以下结果使用旧快照，不能据此认定当前无异常。")
    elif status in ("unknown", "partial"):
        st.warning("数据完整性或新鲜度尚未确认，以下结果仅供参考。")
    if health.get("refreshed_at") and status in ("fresh", "stale"):
        st.caption(
            f"最后成功更新：{health['refreshed_at']}；"
            f"源数据窗口：{health.get('source_start', '')} 至 {health.get('source_end', '')}"
        )
