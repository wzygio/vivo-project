"""Excel-backed Q-Time decoration decision ledger."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.indicator_domain.application.qtime.errors import QTimeDecorationAccessError
from src.indicator_domain.core.qtime.decoration import (
    DECISION_COLUMNS,
    DECISION_SHEET_NAME,
    QTIME_KEY_COLUMNS,
)
from src.shared_kernel.utils.excel_tools import (
    read_workbook_sheet,
    replace_workbook_sheets,
)


SAFE_DECORATION_ERROR = "Q-Time 修饰工作簿读写失败，请关闭 Excel 后重试。"


class QTimeDecorationRepository:
    def __init__(self, decoration_path: Path) -> None:
        self._decoration_path = Path(decoration_path)

    @property
    def decoration_path(self) -> Path:
        return self._decoration_path

    def load_decisions(self) -> pd.DataFrame:
        try:
            frame = read_workbook_sheet(self._decoration_path, DECISION_SHEET_NAME)
        except Exception as exc:
            raise QTimeDecorationAccessError(SAFE_DECORATION_ERROR) from exc
        if frame.empty:
            return pd.DataFrame(columns=DECISION_COLUMNS)
        for column in DECISION_COLUMNS:
            if column not in frame.columns:
                frame[column] = pd.NA
        result = frame[DECISION_COLUMNS].copy()
        for column in QTIME_KEY_COLUMNS:
            result[column] = result[column].fillna("").astype(str).str.strip()
        return result

    def save_decisions(self, decisions: pd.DataFrame) -> None:
        result = replace_workbook_sheets(
            self._decoration_path,
            {DECISION_SHEET_NAME: decisions[DECISION_COLUMNS].copy()},
        )
        if not result.written:
            raise QTimeDecorationAccessError(
                result.error or SAFE_DECORATION_ERROR
            )
