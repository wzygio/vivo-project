"""站点展示标签工具：将工序字典描述拼接到站点展示点（纯展示字段）。"""

from __future__ import annotations

import logging

import streamlit as st

from src.inline_domain.infrastructure.shared.step_description_loader import (
    build_step_description_map,
    load_step_descriptions,
)

logger = logging.getLogger(__name__)


def format_step_label(step_id: str, step_desc_map: dict[str, str] | None = None) -> str:
    """有描述返回 ``f"{step_id} {desc}"``，否则原样返回 step_id。"""
    if not step_desc_map:
        return step_id
    desc = step_desc_map.get(str(step_id))
    return f"{step_id} {desc}" if desc else step_id


@st.cache_data(show_spinner=False, max_entries=3, ttl=4 * 60 * 60)
def get_cached_step_description_map(_db_manager) -> dict[str, str]:
    """加载并缓存 step_id -> 站点描述映射；异常时返回 {} 并 warning（不炸页面）。"""
    try:
        return build_step_description_map(load_step_descriptions(_db_manager))
    except Exception as exc:
        logger.warning("加载工序字典站点描述失败：%s", exc)
        return {}
