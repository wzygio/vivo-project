"""Plain XLSX fast path and protected-file COM adapter for rate overrides."""

from __future__ import annotations

import logging
from pathlib import Path
from zipfile import BadZipFile

import comtypes
import comtypes.client
import pandas as pd

logger = logging.getLogger(__name__)


class RateOverrideReadError(RuntimeError):
    """Raised when a configured rate-override workbook cannot be read safely."""


def _workbook_sheet_names(workbook: object) -> set[str]:
    sheets = workbook.Sheets
    return {
        str(sheets(index).Name).strip().casefold()
        for index in range(1, sheets.Count + 1)
    }


def _read_override_cells_via_com(path: Path, sheet_name: str) -> pd.DataFrame:
    com_initialized = False
    excel = None
    workbook = None
    try:
        comtypes.CoInitialize()
        com_initialized = True
        excel = comtypes.client.CreateObject("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.AutomationSecurity = 3
        workbook = excel.Workbooks.Open(str(path.resolve()), ReadOnly=True, UpdateLinks=0)
        names = {str(workbook.Sheets(index).Name).strip().casefold(): str(workbook.Sheets(index).Name)
                 for index in range(1, workbook.Sheets.Count + 1)}
        actual = names.get(sheet_name.strip().casefold())
        if actual is None:
            return pd.DataFrame()
        raw_data = workbook.Sheets(actual).UsedRange.Value()
        if not raw_data or len(raw_data) < 2:
            return pd.DataFrame()
        return pd.DataFrame(
            [list(row) if row else [None] * len(raw_data[0]) for row in raw_data[1:]],
            columns=[str(column).strip() for column in raw_data[0]],
        )
    finally:
        if workbook is not None:
            try:
                workbook.Close(False)
            except Exception:
                pass
        if excel is not None:
            try:
                excel.Quit()
            except Exception:
                pass
        if com_initialized:
            comtypes.CoUninitialize()


def _read_override_cells(path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        with pd.ExcelFile(path, engine='openpyxl', engine_kwargs={'data_only': False}) as workbook:
            names = {name.strip().casefold(): name for name in workbook.sheet_names}
            actual = names.get(sheet_name.strip().casefold())
            if actual is None:
                return pd.DataFrame()
            needs_calculation = any(cell.data_type == 'f'
                                    for row in workbook.book[actual].iter_rows() for cell in row)
            frame = None if needs_calculation else pd.read_excel(workbook, sheet_name=actual, dtype=object)
    except (BadZipFile, OSError, ValueError):
        return _read_override_cells_via_com(path, sheet_name)
    # Excel must calculate formula inputs. Pandas' default data_only path
    # could silently drop an uncached formula as a missing override value.
    if needs_calculation:
        return _read_override_cells_via_com(path, sheet_name)
    return frame


def load_rate_overrides(
    path: Path | None, sheet_name: str,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    if path is None or not sheet_name or not path.exists():
        return None, None
    try:
        frame = _read_override_cells(path, sheet_name)
        if frame.empty:
            return None, None
        frame.columns = [str(column).strip() for column in frame.columns]
        expected = ["lot_id", "sheet_id", "override_rate", "defect_desc"]
        if not set(expected).issubset(frame.columns):
            raise ValueError(f"Rate override sheet is missing required columns: {expected}")
        frame["override_rate"] = pd.to_numeric(
            frame["override_rate"].astype(str).str.rstrip("%"), errors="coerce"
        )
        if frame["override_rate"].mean() > 1.0:
            frame["override_rate"] /= 100.0
        frame["defect_desc"] = frame["defect_desc"].astype(str).str.strip()
        frame = frame.dropna(subset=expected)
        lots = frame.groupby(["lot_id", "defect_desc"])["override_rate"].mean().rename("override_rate_avg").reset_index()
        return frame[expected], lots[["lot_id", "defect_desc", "override_rate_avg"]]
    except Exception as exc:
        logger.error("Rate override workbook read failed: %s", type(exc).__name__)
        raise RateOverrideReadError("Rate override workbook could not be read.") from exc
