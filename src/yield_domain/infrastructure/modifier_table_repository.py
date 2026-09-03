"""Excel and sidecar persistence adapter for the Yield modifier ledger."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from src.shared_kernel.utils.excel_tools import (
    read_workbook_sheet,
    replace_workbook_sheet,
)
from src.yield_domain.core.mwd_trend.modifier_table import (
    MODIFIER_TABLE_COLUMNS,
    _empty_table,
    _validate_rate_values,
)

logger = logging.getLogger(__name__)


def _read_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    return read_workbook_sheet(path, sheet_name)


def read_modifier_table(xlsx_path: Path, product_code: str) -> dict[str, pd.DataFrame]:
    """Read and validate the product's Group/Code sheets."""
    path = Path(xlsx_path)
    table: dict[str, pd.DataFrame] = {}
    for level, suffix in (("group", "Group级"), ("code", "Code级")):
        sheet_name = f"{product_code}_{suffix}"
        frame = _read_sheet(path, sheet_name) if path.exists() else _empty_table()
        if frame.empty:
            frame = _empty_table()
        else:
            missing = [column for column in MODIFIER_TABLE_COLUMNS if column not in frame]
            if missing:
                logger.warning("Yield modifier %s is missing columns %s", sheet_name, missing)
                for column in missing:
                    frame[column] = None
            frame = frame[MODIFIER_TABLE_COLUMNS]
            _validate_rate_values(frame, product_code=product_code, sheet_name=sheet_name)
        table[level] = frame
    return table


def load_modifier_signatures(path: Path) -> dict[str, str]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def store_modifier_signatures(path: Path, signatures: dict[str, str]) -> None:
    try:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(signatures, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        logger.warning("Yield modifier signature write failed: %s", exc)


def write_modifier_sheet(path: Path, sheet_name: str, frame: pd.DataFrame) -> bool:
    return replace_workbook_sheet(Path(path), sheet_name, frame)
