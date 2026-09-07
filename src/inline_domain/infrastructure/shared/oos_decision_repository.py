"""Workbook adapter used when decorating durable OOS facts with user decisions."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    SheetOosDecorationReadError,
    get_sheet_oos_decoration_path,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    DECISION_FLAG_COLUMN,
    REFRESH_META_SHEET_NAME,
    get_decision_sheet_name,
)
from src.shared_kernel.utils.excel_tools import _read_all_sheets_via_com


class OosDecisionWorkbookRepository:
    def __init__(self, resource_dir: Path) -> None:
        self._resource_dir = resource_dir
        self._cache: dict[str, tuple[tuple[int, int] | None, dict[str, pd.DataFrame]]] = {}

    def _all_sheets(self, file_name: str) -> dict[str, pd.DataFrame]:
        path = self.source_path(file_name)
        if not path.exists():
            return {}
        stat = path.stat()
        signature = (stat.st_mtime_ns, stat.st_size)
        cached = self._cache.get(file_name)
        if cached is not None and cached[0] == signature:
            return cached[1]
        try:
            sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
        except Exception:
            try:
                sheets = _read_all_sheets_via_com(path)
            except Exception as com_exc:
                raise SheetOosDecorationReadError(
                    f"Unable to read OOS decision workbook: {path}"
                ) from com_exc
        normalized = {str(name): frame for name, frame in sheets.items()}
        self._cache[file_name] = (signature, normalized)
        return normalized

    def load_decisions(
        self, file_name: str, prod_code: str, key_columns: Iterable[str]
    ) -> pd.DataFrame:
        keys = tuple(key_columns)
        frame = self._all_sheets(file_name).get(get_decision_sheet_name(prod_code))
        if frame is None or frame.empty:
            return pd.DataFrame(columns=[*keys, DECISION_FLAG_COLUMN])
        result = frame.copy()
        for column in keys:
            if column not in result.columns:
                result[column] = ""
            result[column] = result[column].fillna("").astype(str)
        if DECISION_FLAG_COLUMN not in result.columns:
            result[DECISION_FLAG_COLUMN] = True
        return result.loc[:, [*keys, DECISION_FLAG_COLUMN]].copy()

    def load_fallback(
        self, file_name: str, prod_code: str, key_columns: Iterable[str]
    ) -> pd.DataFrame:
        frame = self._all_sheets(file_name).get(prod_code)
        if frame is None or frame.empty:
            return pd.DataFrame()
        result = frame.copy()
        for column in key_columns:
            if column in result.columns:
                result[column] = result[column].fillna("").astype(str)
        return result

    def load_refresh_time(
        self, file_name: str, scope: str, prod_code: str
    ) -> pd.Timestamp | None:
        meta = self._all_sheets(file_name).get(REFRESH_META_SHEET_NAME)
        refreshed_at = None
        if meta is not None and {"scope", "prod_code"}.issubset(meta.columns):
            matched = meta.loc[
                meta["scope"].fillna("").astype(str).eq(scope)
                & meta["prod_code"].fillna("").astype(str).eq(prod_code)
            ]
            if not matched.empty:
                refreshed_at = matched.iloc[-1].get("last_generated_at")
        parsed = pd.to_datetime(refreshed_at, errors="coerce")
        if not pd.isna(parsed):
            return pd.Timestamp(parsed)
        path = self.source_path(file_name)
        return pd.Timestamp(path.stat().st_mtime, unit="s") if path.exists() else None

    def source_path(self, file_name: str) -> Path:
        return get_sheet_oos_decoration_path(self._resource_dir, file_name)
