"""全指标矩阵的显示修饰；不修改计算结果，也不清理计算缓存。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import streamlit as st

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.core.alert_visibility import (
    ROW_COLUMN,
    apply_matrix_compliance,
    parse_matrix_config,
)
from src.shared_kernel.core.decoration_flags import DecorationFlag
from src.shared_kernel.infrastructure.matrix_decisions import SHEET_NAME, update_matrix_decisions
from src.shared_kernel.utils.excel_tools import read_workbook_sheet

CONFIG_PATH: Path | None = None


@st.cache_data(show_spinner=False, max_entries=16)
def _read_matrix_config(path: str, mtime_ns: int, size: int) -> dict:
    return parse_matrix_config(read_workbook_sheet(Path(path), SHEET_NAME))


def load_matrix_config(path: Path | None = None) -> dict[tuple[str, str], DecorationFlag]:
    source = Path(path or CONFIG_PATH or ConfigLoader.get_compliance_config_path())
    stat = source.stat()
    return _read_matrix_config(str(source.resolve()), stat.st_mtime_ns, stat.st_size)


def write_matrix_template(
    rows: Sequence[Mapping[str, Any]], products: Sequence[str], path: Path | None = None,
) -> None:
    """Create missing indicator/product keys while preserving historical choices."""
    target = Path(path or CONFIG_PATH or ConfigLoader.get_compliance_config_path())
    update_matrix_decisions(target, (row["display_name"] for row in rows), products)


def get_compliance_file_signature() -> str:
    """旧明细缓存兼容接口：显示修饰文件不得进入计算缓存签名。"""
    return "display-only-compliance"
