"""Admin-only editor for OOC decision ledgers across Inline scopes."""

from __future__ import annotations

from io import BytesIO

import pandas as pd
import streamlit as st

from app.sections.inline_domain.shared.decoration_admin import excel_bytes
from src.inline_domain.application.shared.ooc_history_service import OocHistoryService
from src.inline_domain.core.shared.sheet_oos_decoration import (
    DECISION_FLAG_COLUMN,
    _normalize_flag_action,
)
from src.shared_kernel.utils.excel_tools import replace_workbook_sheets

_TRUE_TOKENS = {"true", "1", "yes", "y", "是", "修饰", "截断"}
_FALSE_TOKENS = {"false", "0", "no", "n", "否", "不修饰", "不截断"}
_VALID_TOKENS = _TRUE_TOKENS | _FALSE_TOKENS | {"delete"}


def _normalize_upload(
    file_bytes: bytes, key_columns: tuple[str, ...]
) -> tuple[pd.DataFrame | None, str | None]:
    try:
        sheets = pd.read_excel(BytesIO(file_bytes), sheet_name=None)
    except Exception as exc:
        return None, f"无法读取 Excel：{exc}"
    source = sheets.get("决策台账") if sheets else None
    if source is None:
        return None, "上传文件缺少“决策台账”sheet。"
    required = [*key_columns, DECISION_FLAG_COLUMN]
    missing = [column for column in required if column not in source.columns]
    if missing:
        return None, f"决策台账缺少必要字段：{', '.join(missing)}"
    normalized = source[required].copy()
    for column in key_columns:
        normalized[column] = normalized[column].fillna("").astype(str)
    if normalized.duplicated(list(key_columns), keep=False).any():
        return None, "决策台账存在重复业务键，请去重后重试。"

    def is_valid(value: object) -> bool:
        if isinstance(value, bool):
            return True
        if isinstance(value, (int, float)) and not pd.isna(value):
            return value in (0, 1)
        return str(value).strip().lower() in _VALID_TOKENS

    if not normalized[DECISION_FLAG_COLUMN].apply(is_valid).all():
        return None, "flag 仅支持 True、False 或 Delete。"
    normalized[DECISION_FLAG_COLUMN] = normalized[DECISION_FLAG_COLUMN].apply(
        _normalize_flag_action
    )
    return normalized, None


def render_ooc_decision_admin(
    service: OocHistoryService,
    *,
    products: tuple[str, ...],
    scopes: tuple[str, ...],
) -> None:
    """Render one selected product/scope ledger; caller enforces admin access."""
    if not products or not scopes:
        return
    with st.expander("OOC 预警决策管理（管理员）", expanded=False):
        columns = st.columns(2)
        with columns[0]:
            product = st.selectbox("产品", products, key="monitor_ooc_admin_product")
        with columns[1]:
            scope = st.selectbox(
                "类型", scopes, format_func=str.upper, key="monitor_ooc_admin_scope"
            )
        view = service.build_decision_admin_view(str(scope), str(product))
        decision_columns = [*view.key_columns, DECISION_FLAG_COLUMN]
        decision_df = view.decision_df.reindex(columns=decision_columns)
        payload = excel_bytes({"当前明细": view.detail_df, "决策台账": decision_df})
        st.caption(
            "flag=True 抑制预警，False 释放到看板，Delete 隐藏记录；"
            f"工作簿：{view.workbook_path}"
        )
        left, right = st.columns(2)
        with left:
            st.download_button(
                "下载 OOC 决策表",
                data=payload,
                file_name=f"{product}_{scope}_ooc_decisions.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="monitor_ooc_admin_download",
                use_container_width=True,
            )
        with right:
            uploaded = st.file_uploader(
                "上传 OOC 决策表",
                type=["xlsx"],
                key="monitor_ooc_admin_upload",
                label_visibility="collapsed",
            )
            if uploaded is not None and st.button(
                "确认覆盖 OOC 决策",
                type="primary",
                key="monitor_ooc_admin_apply",
                use_container_width=True,
            ):
                normalized, error = _normalize_upload(
                    uploaded.getbuffer(), view.key_columns
                )
                if error:
                    st.error(error)
                else:
                    result = replace_workbook_sheets(
                        view.workbook_path, {view.decision_sheet: normalized}
                    )
                    if result.written:
                        st.success("OOC 决策台账已更新，正在刷新。")
                        st.rerun()
                    else:
                        st.error(f"保存失败：{result.error}")


__all__ = ["render_ooc_decision_admin"]
