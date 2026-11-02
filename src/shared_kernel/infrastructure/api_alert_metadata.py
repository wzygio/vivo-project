"""Read local alert attribution; only explicit maintenance functions may write.

The HTTP reader never creates a database manager or fills missing metadata.
Factory and layer are mandatory. Yield alone permits a missing step identifier.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import logging
from pathlib import Path
from typing import Any
from zipfile import BadZipFile

import pandas as pd

from src.shared_kernel.infrastructure.file_lock import publication_lock

logger = logging.getLogger(__name__)
STATION_SHEET = "站点字典"
ATTRIBUTION_SHEET = "Yield归属"
STATION_COLUMNS = ("step_id", "description")
ATTRIBUTION_COLUMNS = ("product", "lot_id", "parameter_name", "factory", "step_id", "description")
WorkbookReader = Callable[[str, int, int], dict[str, pd.DataFrame]]
STEP_TABLES = ("mdw.dwr_mes_processoperationspec_v", "mdw.dwr_mes_processoperationspec")
MAX_STEPS = 1000
MAX_QUERY_ROWS = 10000


def _text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def read_metadata_workbook(path: str, mtime_ns: int, size: int) -> dict[str, pd.DataFrame]:
    """Preserve text identifiers, including Excel strings with leading zeroes."""
    del mtime_ns, size
    try:
        return pd.read_excel(path, sheet_name=None, dtype=str, keep_default_na=False)
    except (BadZipFile, OSError, ValueError):
        from src.shared_kernel.utils.excel_tools import read_workbook_sheets

        return read_workbook_sheets(Path(path))


def _frame(sheets: Mapping[str, pd.DataFrame], name: str, columns: Sequence[str]) -> pd.DataFrame:
    frame = sheets.get(name)
    if frame is None or frame.columns.duplicated().any() or set(columns) - set(frame.columns):
        raise ValueError("ALERT_METADATA_SCHEMA_INVALID")
    normalized = frame.loc[:, list(columns)].copy()
    for column in columns:
        normalized[column] = normalized[column].map(_text)
    return normalized.loc[normalized.ne("").any(axis=1)].reset_index(drop=True)


def _station_map(frame: pd.DataFrame) -> dict[str, str]:
    if frame.empty:
        return {}
    if frame.eq("").any(axis=None):
        raise ValueError("STATION_METADATA_INCOMPLETE")
    distinct = frame.drop_duplicates()
    if distinct["step_id"].duplicated().any():
        raise ValueError("STATION_METADATA_AMBIGUOUS")
    return dict(zip(distinct["step_id"], distinct["description"]))


class ApiAlertMetadata:
    """Read-only adapter for the maintained station/attribution workbook."""

    def __init__(self, path: Path, *, workbook_reader: WorkbookReader = read_metadata_workbook) -> None:
        self.path = Path(path)
        self._reader = workbook_reader

    def enrich(
        self, sub_domain: str, product: str, entity_id: str, parameter_name: str,
        factory: str | None, step_id: str | None,
    ) -> dict[str, str | None]:
        stat = self.path.stat()
        sheets = self._reader(str(self.path.resolve()), stat.st_mtime_ns, stat.st_size)
        stations = _station_map(_frame(sheets, STATION_SHEET, STATION_COLUMNS))
        chosen_factory, chosen_step = _text(factory).upper(), _text(step_id)
        description = ""
        is_yield = sub_domain.casefold() == "yield"
        if is_yield:
            attributions = _frame(sheets, ATTRIBUTION_SHEET, ATTRIBUTION_COLUMNS)
            candidates = attributions.loc[
                attributions["product"].eq(product)
                & attributions["parameter_name"].eq(parameter_name)
            ]
            selected = candidates.loc[candidates["lot_id"].eq(entity_id)]
            if selected.empty:
                selected = candidates.loc[candidates["lot_id"].eq("")]
            if len(selected) != 1:
                raise ValueError("YIELD_ATTRIBUTION_UNAVAILABLE")
            chosen = selected.iloc[0]
            chosen_factory, chosen_step = chosen["factory"].upper(), chosen["step_id"]
            description = chosen["description"]
        if not chosen_factory or (not is_yield and not chosen_step):
            raise ValueError("ALERT_ATTRIBUTION_INCOMPLETE")
        if chosen_step:
            description = stations.get(chosen_step, "")
        layer = description.split("_", 1)[0].strip()
        if not layer:
            raise ValueError("ALERT_LAYER_UNAVAILABLE")
        return {"factory": chosen_factory, "step_id": chosen_step or None, "layer": layer}


def initialize(path: Path) -> bool:
    """Explicitly create an empty editable template; preserve any existing file."""
    from src.shared_kernel.utils.excel_tools import replace_workbook_sheets

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with publication_lock(path):
        if path.exists():
            return False
        result = replace_workbook_sheets(path, {
            STATION_SHEET: pd.DataFrame(columns=STATION_COLUMNS),
            ATTRIBUTION_SHEET: pd.DataFrame(columns=ATTRIBUTION_COLUMNS),
        })
        if not result.written:
            raise OSError("ALERT_METADATA_INITIALIZATION_FAILED")
    return True


def _fetch_stations(engine: Any, steps: list[str], row_limit: int, timeout_ms: int) -> pd.DataFrame:
    from sqlalchemy import bindparam, text
    from sqlalchemy.exc import SQLAlchemyError

    for table in STEP_TABLES:
        query = text(
            f"SELECT oper_code AS step_id, description AS description FROM {table} "
            "WHERE oper_code IN :step_ids AND description IS NOT NULL "
            "ORDER BY oper_code, description LIMIT :row_limit"
        ).bindparams(bindparam("step_ids", expanding=True))
        try:
            # Each fallback gets a fresh transaction: PostgreSQL aborts a
            # transaction after a missing-view failure, so it cannot be reused.
            with engine.begin() as connection:
                connection.execute(
                    text("SELECT set_config('statement_timeout', :timeout, true)"),
                    {"timeout": f"{timeout_ms}ms"},
                )
                rows = connection.execute(
                    query, {"step_ids": steps, "row_limit": row_limit + 1},
                ).mappings().all()
        except SQLAlchemyError as exc:
            logger.warning("STATION_DICTIONARY_QUERY_FAILED exception_type=%s", type(exc).__name__)
            continue
        if len(rows) > row_limit:
            raise ValueError("STATION_DICTIONARY_TRUNCATED")
        frame = _frame({STATION_SHEET: pd.DataFrame(rows, columns=STATION_COLUMNS)}, STATION_SHEET, STATION_COLUMNS)
        mapping = _station_map(frame)
        if set(mapping) != set(steps):
            raise ValueError("STATION_DICTIONARY_INCOMPLETE")
        if any(value.startswith("=") for row in frame.itertuples(index=False, name=None) for value in row):
            raise ValueError("STATION_DICTIONARY_FORMULA_INVALID")
        return pd.DataFrame(list(mapping.items()), columns=STATION_COLUMNS)
    raise OSError("STATION_DICTIONARY_UNAVAILABLE")


def sync_metadata(
    path: Path, steps: Sequence[str], engine: Any, *,
    max_rows: int = MAX_QUERY_ROWS, timeout_ms: int = 10000,
) -> int:
    """Explicitly refresh requested stations; preserve attribution and history.

    This bounded command is separate from HTTP reads. Invalid/incomplete source
    responses fail before writing. Source views are never scanned without a
    requested station allowlist and a result cap.
    """
    from src.shared_kernel.utils.excel_tools import replace_workbook_sheets

    if (isinstance(steps, (str, bytes)) or not steps or len(steps) > MAX_STEPS
            or isinstance(max_rows, bool) or not isinstance(max_rows, int) or not 1 <= max_rows <= MAX_QUERY_ROWS
            or isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int) or not 1 <= timeout_ms <= 60000):
        raise ValueError("STATION_SYNC_SCOPE_INVALID")
    normalized = [_text(step) for step in steps]
    if any(not step or len(step) > 128 for step in normalized):
        raise ValueError("STATION_SYNC_SCOPE_INVALID")
    chosen = list(dict.fromkeys(normalized))
    path = Path(path)
    path.stat()  # Require explicit initialization before any database call.
    incoming = _fetch_stations(engine, chosen, max_rows, timeout_ms)
    with publication_lock(path):
        stat = path.stat()
        previous = read_metadata_workbook(str(path.resolve()), stat.st_mtime_ns, stat.st_size)
        stations = _frame(previous, STATION_SHEET, STATION_COLUMNS)
        _station_map(stations)
        _frame(previous, ATTRIBUTION_SHEET, ATTRIBUTION_COLUMNS)
        merged = pd.concat([
            stations.loc[~stations["step_id"].isin(chosen)], incoming,
        ], ignore_index=True).sort_values("step_id", kind="stable").reset_index(drop=True)
        result = replace_workbook_sheets(path, {STATION_SHEET: merged})
        if not result.written:
            raise OSError("STATION_DICTIONARY_PUBLICATION_FAILED")
    return len(incoming)
