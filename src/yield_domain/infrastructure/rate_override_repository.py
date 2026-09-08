"""Excel COM adapter for legacy Sheet/Lot rate override input."""

from __future__ import annotations

import logging
from pathlib import Path

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


def load_rate_overrides(
    path: Path | None,
    sheet_name: str,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    if path is None or not sheet_name or not path.exists():
        return None, None
    com_initialized = False
    excel = None
    workbook = None
    try:
        comtypes.CoInitialize()
        com_initialized = True
        excel = comtypes.client.CreateObject("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        workbook = excel.Workbooks.Open(str(path.resolve()))
        if sheet_name.strip().casefold() not in _workbook_sheet_names(workbook):
            logger.info(
                "Rate override workbook has no sheet for product %s; "
                "continuing without overrides.",
                sheet_name,
            )
            return None, None
        raw_data = workbook.Sheets(sheet_name).UsedRange.Value()
        if not raw_data or len(raw_data) < 2:
            return None, None
        frame = pd.DataFrame(
            [list(row) if row else [None] * len(raw_data[0]) for row in raw_data[1:]],
            columns=[str(column).strip() for column in raw_data[0]],
        )
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
        lots = (
            frame.groupby(["lot_id", "defect_desc"])["override_rate"]
            .mean()
            .rename("override_rate_avg")
            .reset_index()
        )
        return frame[expected], lots[["lot_id", "defect_desc", "override_rate_avg"]]
    except Exception as exc:
        logger.error("Rate override workbook read failed: %s", exc, exc_info=True)
        raise RateOverrideReadError("Rate override workbook could not be read.") from exc
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
