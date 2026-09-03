"""两阶段决策签名入口（PRD §5.4）。

阶段 1：``file_stat_signature = (mtime_ns, size)``，每次页面运行廉价读取；
阶段 2：决策内容签名 = ``compute_decision_signature(load_sheet_oos_decisions(...))``，
由 ``st.cache_data`` 以 (workbook 路径, sheet, mtime_ns, size) 为键缓存——
file_stat 未变不重读 ``__flags``（避免普通 rerun 反复启动 Excel COM），
file_stat 变化才重读。系统重写产品明细 sheet 只改变 mtime，不改变决策内容 hash，
因此不会触发自触发刷新循环。

失败语义：工作簿不存在返回确定性空签名（``EMPTY_DECISION_SIGNATURE``）；
``__flags`` 存在但读取失败必须把 ``SheetOosDecorationReadError`` 向上抛，
不得降级为空签名（否则历史用户决策会被解释为默认 True 而丢失）。
"""

from __future__ import annotations

import logging
from pathlib import Path

import streamlit as st

from src.inline_domain.application.shared.decorated_data import (
    SCOPE_DECORATION_FILE_NAME,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    EMPTY_DECISION_SIGNATURE,
    compute_decision_signature,
)
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    load_sheet_oos_decisions,
)
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)


def get_decision_file_stat(workbook_path: Path) -> tuple[int, int] | None:
    """阶段 1：廉价 file_stat 探针；工作簿不存在返回 None。"""
    try:
        stat = Path(workbook_path).stat()
    except OSError:
        return None
    return (int(stat.st_mtime_ns), int(stat.st_size))


@st.cache_data(show_spinner=False, max_entries=64, ttl=4 * 60 * 60)
def _cached_decision_signature(
    workbook_path: str,
    sheet_name: str,
    mtime_ns: int,
    size: int,
) -> str:
    """阶段 2：决策内容签名缓存。键全部转 str/int，规避 Path 哈希差异。"""
    decisions_df = load_sheet_oos_decisions(
        product_dir=Path(workbook_path).parent,
        file_name=Path(workbook_path).name,
        sheet_name=sheet_name,
    )
    return compute_decision_signature(decisions_df)


def get_decision_signature(workbook_path: Path | str, sheet_name: str) -> str:
    """返回指定工作簿中 ``<sheet_name>__flags`` 决策台账的内容签名。

    工作簿不存在 -> 确定性空签名；``__flags`` 存在但读取失败 ->
    ``SheetOosDecorationReadError`` 上抛（页面现有 except 路径处理）。
    """
    path = Path(workbook_path)
    file_stat = get_decision_file_stat(path)
    if file_stat is None:
        return EMPTY_DECISION_SIGNATURE
    mtime_ns, size = file_stat
    return _cached_decision_signature(str(path), str(sheet_name), mtime_ns, size)


def get_scope_decision_signature(
    scope: str,
    prod_code: str,
    product_dir: Path | None = None,
) -> str:
    """scope 便捷入口：按 scope 定位用户维护的修饰工作簿并计算决策签名。

    ``product_dir`` 为测试用显式覆盖；默认经
    ``ConfigLoader.get_domain_resource_dir("inline_domain")`` 解析到
    ``resources/inline_domain/``（与 ``prepare_decorated_data`` 的
    ``resolve_product_resource_dir`` 工作簿定位保持一致）。
    """
    normalized_scope = (scope or "").strip().lower()
    if normalized_scope not in SCOPE_DECORATION_FILE_NAME:
        raise ValueError(f"unknown decoration scope: {scope!r}")
    base_dir = (
        Path(product_dir)
        if product_dir is not None
        else ConfigLoader.get_domain_resource_dir("inline_domain")
    )
    workbook_path = base_dir / SCOPE_DECORATION_FILE_NAME[normalized_scope]
    return get_decision_signature(workbook_path, prod_code)
