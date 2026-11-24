"""Incrementally maintain the indicator-by-product public visibility workbook."""

from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from src.shared_kernel.core.alert_visibility import ROW_COLUMN, parse_matrix_config
from src.shared_kernel.core.decoration_flags import DecorationFlag, parse_decoration_flag
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets, replace_workbook_sheets

SHEET_NAME = "全指标预警看板"


def update_matrix_decisions(
    path: Path, names: Iterable[str], products: Iterable[str], *,
    edit: tuple[str, str, DecorationFlag] | None = None,
    expected: DecorationFlag | None = None,
) -> None:
    """Extend the matrix with False and optionally edit one administrator-selected cell."""
    with publication_lock(path):
        if path.exists():
            frame = read_workbook_sheets(path, preserve_text=True)[SHEET_NAME]
            parse_matrix_config(frame)
            frame = frame.dropna(how="all").set_index(ROW_COLUMN)
            frame.index = frame.index.astype(str).str.strip()
        else:
            frame = pd.DataFrame(index=pd.Index([], name=ROW_COLUMN))
        rows = list(dict.fromkeys([*frame.index, *names]))
        columns = list(dict.fromkeys([*frame.columns, *products]))
        result = frame.reindex(index=rows, columns=columns, fill_value=False).astype(object)
        if edit is not None:
            name, product, action = edit
            if name not in result.index or product not in result.columns:
                raise ValueError("未知指标或产品")
            if expected is not None and parse_decoration_flag(result.at[name, product]) != expected:
                raise ValueError("该决定已变更，请刷新后重试")
            result.at[name, product] = parse_decoration_flag(action)
        if result.equals(frame.astype(object)) and path.exists():
            return
        written = replace_workbook_sheets(path, {SHEET_NAME: result.reset_index()})
        if not written.written:
            raise OSError("矩阵显示台账保存失败")
