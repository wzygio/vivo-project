"""Excel-backed Q-Time decoration decision ledger."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.indicator_domain.application.qtime.decoration_service import (
    DECISION_SHEET_NAME,
)
from src.indicator_domain.application.qtime.errors import QTimeDecorationAccessError
from src.indicator_domain.core.qtime.decoration import (
    DECISION_COLUMNS,
    QTIME_KEY_COLUMNS,
    normalize_qtime_flag,
)
from src.shared_kernel.utils.excel_tools import (
    replace_workbook_sheets,
)
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.core.decoration_revision import validate_decision_update
from src.indicator_domain.infrastructure.qtime.alert_workbook import read_qtime_alert_workbook

SAFE_DECORATION_ERROR = "Q-Time 修饰工作簿读写失败，请关闭 Excel 后重试。"
LEGACY_SIGNATURE_SHEET = "__legacy_policy"


class QTimeDecorationRepository:
    def __init__(self, decoration_path: Path, *, workbook_reader=None) -> None:
        self._decoration_path = Path(decoration_path)
        self._reader = workbook_reader or read_qtime_alert_workbook

    @property
    def decoration_path(self) -> Path:
        return self._decoration_path

    def load_decisions(self) -> pd.DataFrame:
        try:
            stat = self._decoration_path.stat()
            sheets = self._reader(str(self._decoration_path.resolve()), stat.st_mtime_ns, stat.st_size)
            frame = sheets[DECISION_SHEET_NAME]
        except FileNotFoundError:
            return pd.DataFrame(columns=DECISION_COLUMNS)
        except Exception as exc:
            raise QTimeDecorationAccessError(SAFE_DECORATION_ERROR) from exc
        if not set(DECISION_COLUMNS) <= set(frame):
            raise QTimeDecorationAccessError(SAFE_DECORATION_ERROR)
        result = frame[DECISION_COLUMNS].copy()
        for column in QTIME_KEY_COLUMNS:
            result[column] = result[column].fillna("").astype(str).str.strip()
        if result[QTIME_KEY_COLUMNS].eq("").any().any() or result.duplicated(QTIME_KEY_COLUMNS).any():
            raise QTimeDecorationAccessError(SAFE_DECORATION_ERROR)
        result["flag"] = result["flag"].map(normalize_qtime_flag)
        return result

    def save_decisions(self, decisions: pd.DataFrame, *, revision: str | None = None) -> None:
        with publication_lock(self._decoration_path):
            current = self.load_decisions()
            validate_decision_update("qtime", current, decisions, QTIME_KEY_COLUMNS, revision)
            combined = pd.concat([current, decisions], ignore_index=True)
            self._write(combined.drop_duplicates(QTIME_KEY_COLUMNS, keep="last"))

    def initialize_decisions(self, candidates: pd.DataFrame) -> pd.DataFrame:
        if candidates.empty:
            return self.load_decisions()
        with publication_lock(self._decoration_path):
            current = self.load_decisions()
            additions = candidates[QTIME_KEY_COLUMNS].apply(lambda column: column.fillna("").astype(str).str.strip())
            if additions.eq("").any().any():
                raise QTimeDecorationAccessError("Q-Time 超规候选缺少业务键。")
            additions = additions.drop_duplicates()
            new = additions.loc[~pd.MultiIndex.from_frame(additions).isin(
                pd.MultiIndex.from_frame(current[QTIME_KEY_COLUMNS]))]
            if not new.empty:
                current = pd.concat([current, new.assign(flag=True)], ignore_index=True)
                self._write(current)
            return current

    def _write(self, decisions: pd.DataFrame) -> None:
        sheets = {DECISION_SHEET_NAME: decisions[DECISION_COLUMNS].copy()}
        if self._decoration_path.exists():
            stat = self._decoration_path.stat()
            existing = self._reader(str(self._decoration_path.resolve()), stat.st_mtime_ns, stat.st_size)
            if LEGACY_SIGNATURE_SHEET not in existing:
                # Older alert policy digests bound the business policy to the ledger file stat.
                # Keep its pre-migration version as text, preserving nanosecond precision in Excel.
                sheets[LEGACY_SIGNATURE_SHEET] = pd.DataFrame({"version": [f"{stat.st_mtime_ns}:{stat.st_size}"]})
        result = replace_workbook_sheets(
            self._decoration_path,
            sheets,
        )
        if not result.written:
            raise QTimeDecorationAccessError(result.error or SAFE_DECORATION_ERROR)

    def legacy_file_version(self) -> tuple[int, int] | None:
        try:
            stat = self._decoration_path.stat()
        except FileNotFoundError:
            return None
        existing = self._reader(str(self._decoration_path.resolve()), stat.st_mtime_ns, stat.st_size)
        header = existing.get(LEGACY_SIGNATURE_SHEET)
        if header is None:
            return None
        mtime, size = str(header["version"].item()).split(":")
        return int(mtime), int(size)
