# src/equipment_domain/infrastructure/spc/data_loader.py
"""
[Data Access Layer / DAO] Critical parts report data loader.

Responsibilities:
1. load_spec_baseline()       -- Load spec baseline config
2. load_part_life_snapshot()  -- Query DB, cache as Parquet snapshot
"""

import csv
import hashlib
import logging
import re
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Mapping

import pandas as pd
from sqlalchemy import text

from src.equipment_domain.config import get_equipment_runtime_config
from src.equipment_domain.infrastructure.fake_data import (
    SNAPSHOT_COLUMNS,
)
from src.equipment_domain.infrastructure.fake_data_updater import (
    ensure_fabricated_snapshot_file,
)
from src.shared_kernel.config import ConfigLoader

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)

REQUIRED_BASELINE_COLUMNS: list[str] = [
    "厂别", "备件类型", "设备类型",
    "膜层", "制程", "寿命规格",
    "站点", "机台号-腔室", "参数名称",
]


def load_spec_baseline(baseline_path: str | Path) -> pd.DataFrame:
    """Load spec baseline config. Prefer CSV, fallback to encrypted Excel."""
    runtime_config = get_equipment_runtime_config()
    path = Path(baseline_path)
    if not path.exists():
        logger.info(f"CSV not found, generating from encrypted Excel")
        _generate_baseline_csv_from_excel(path)
    logger.info(f"Loading spec baseline CSV: {path}")
    try:
        df = pd.read_csv(path, dtype=str, encoding=runtime_config.csv_encoding)
    except UnicodeDecodeError:
        _normalize_encrypted_baseline_csv(
            path,
            encoding=runtime_config.csv_encoding,
        )
        df = pd.read_csv(path, dtype=str, encoding=runtime_config.csv_encoding)
    missing_cols = [col for col in REQUIRED_BASELINE_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}. Current: {list(df.columns)}")
    df["寿命规格"] = df["寿命规格"].astype(str).str.replace(r"[^\d.]", "", regex=True)
    df["寿命规格"] = pd.to_numeric(df["寿命规格"], errors="coerce")
    df = df.dropna(how="all").reset_index(drop=True)
    logger.info(f"Loaded {len(df)} spec baseline records")
    return df


def _normalize_encrypted_baseline_csv(
    csv_path: Path,
    *,
    encoding: str,
) -> None:
    """Export an enterprise-encrypted CSV workbook as validated plain CSV."""
    if csv_path.read_bytes()[:4] != b"\x00\x00\x00\x00":
        raise ValueError(
            f"Baseline CSV is not UTF-8 and has no recognized encrypted header: {csv_path}"
        )
    rows = _read_encrypted_csv_rows_via_excel(csv_path)
    if not rows:
        raise ValueError(f"Encrypted baseline CSV is empty: {csv_path}")
    header = [str(value).strip() for value in rows[0][: len(REQUIRED_BASELINE_COLUMNS)]]
    if header != REQUIRED_BASELINE_COLUMNS:
        raise ValueError(
            "Encrypted baseline CSV columns do not match the required schema: "
            f"{header}"
        )

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            newline="",
            encoding=encoding,
            dir=csv_path.parent,
            prefix=f".{csv_path.stem}-",
            suffix=csv_path.suffix,
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            writer = csv.writer(temporary_file)
            writer.writerow(REQUIRED_BASELINE_COLUMNS)
            for row in rows[1:]:
                writer.writerow(list(row[: len(REQUIRED_BASELINE_COLUMNS)]))
        validation_df = pd.read_csv(temporary_path, dtype=str, encoding=encoding)
        missing = [
            column for column in REQUIRED_BASELINE_COLUMNS
            if column not in validation_df.columns
        ]
        if missing or validation_df.empty:
            raise ValueError(
                f"Decrypted baseline CSV failed validation; missing={missing}"
            )
        temporary_path.replace(csv_path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _read_encrypted_csv_rows_via_excel(csv_path: Path) -> list[tuple[str, ...]]:
    """Read a rights-managed CSV container through the local Excel client."""
    try:
        import pythoncom
        import win32com.client
    except ImportError as error:
        raise ImportError(f"pywin32 required to decrypt baseline CSV: {error}") from error

    pythoncom.CoInitialize()
    excel = None
    workbook = None
    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        workbook = excel.Workbooks.Open(str(csv_path.resolve()))
        worksheet = workbook.Worksheets(1)
        raw_values = worksheet.UsedRange.Value
        if raw_values is None:
            return []
        return [
            tuple("" if value is None else str(value).strip() for value in row)
            for row in raw_values
        ]
    finally:
        if workbook is not None:
            workbook.Close(False)
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()


def load_part_life_snapshot(
    db_manager: "DatabaseManager",
    spec_df: pd.DataFrame,
) -> pd.DataFrame:
    """Query DB for part life data matching spec table, with Parquet snapshot caching."""
    spec_hash = hashlib.md5(
        pd.util.hash_pandas_object(spec_df, index=True).values.tobytes()
    ).hexdigest()[:12]
    runtime_config = get_equipment_runtime_config()
    snapshot_path = runtime_config.snapshot_dir / f"part_life_snapshot_{spec_hash}.parquet"
    if snapshot_path.exists():
        mtime = datetime.fromtimestamp(snapshot_path.stat().st_mtime)
        if (datetime.now() - mtime).total_seconds() < runtime_config.snapshot_ttl_hours * 3600:
            logger.info(f"Loading from Parquet snapshot: {snapshot_path}")
            df = pd.read_parquet(snapshot_path)
            logger.info(f"Loaded {len(df)} records from snapshot")
            return df
        else:
            logger.info(f"Snapshot expired, re-querying...")
    df = _query_part_life_from_db(db_manager, spec_df)
    if df.empty:
        logger.warning("No part life data found in database.")
        return df
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(snapshot_path, index=False)
    logger.info(f"Saved snapshot: {snapshot_path} ({len(df)} records)")
    return df


def load_fabricated_part_life_snapshot(
    spec_df: pd.DataFrame,
    *,
    now: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Create/update and load the fabricated snapshot used for real-data gaps."""
    runtime_config = get_equipment_runtime_config()
    current_time = pd.Timestamp.now().floor("s") if now is None else pd.Timestamp(now)
    outcome = ensure_fabricated_snapshot_file(
        spec_df,
        runtime_config.fabrication_policy,
        output_dir=runtime_config.snapshot_dir,
        now=current_time,
    )
    snapshot_path = outcome.path
    logger.info(
        "Fabricated snapshot maintenance: path=%s reason=%s",
        snapshot_path,
        outcome.summary["reason"],
    )
    df = pd.read_parquet(snapshot_path)
    missing = [column for column in SNAPSHOT_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing fabricated snapshot columns: {missing}")
    logger.info("Loaded %s fabricated fallback records from %s", len(df), snapshot_path)
    return df.loc[:, SNAPSHOT_COLUMNS].copy()


def filter_recent_part_life_measurements(
    snapshot_df: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
    max_age_days: int,
) -> pd.DataFrame:
    """Keep measurements inside the report freshness window."""
    if max_age_days <= 0:
        raise ValueError("measurement maximum age must be positive")
    if snapshot_df.empty:
        return snapshot_df.copy()
    if "glass_start_time" not in snapshot_df.columns:
        raise ValueError("part-life snapshot is missing glass_start_time")
    current_time = pd.Timestamp(as_of)
    if pd.isna(current_time):
        raise ValueError("as_of must be a valid timestamp")
    if current_time.tzinfo is not None:
        current_time = current_time.tz_localize(None)
    measurement_times = pd.to_datetime(
        snapshot_df["glass_start_time"],
        errors="coerce",
    )
    cutoff = current_time - pd.Timedelta(days=max_age_days)
    recent_mask = measurement_times.between(
        cutoff,
        current_time,
        inclusive="both",
    )
    return snapshot_df.loc[recent_mask].copy()


def load_report_part_life_snapshots(
    db_manager: "DatabaseManager",
    spec_df: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
    max_age_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return real and fabricated snapshots on the report display-time axis.

    Snapshot creation, persistence, and freshness filtering all use source
    timestamps. The display offset is applied only to copied frames returned
    across this repository boundary.
    """
    source_snapshot = load_part_life_snapshot(db_manager, spec_df)
    recent_source_snapshot = filter_recent_part_life_measurements(
        source_snapshot,
        as_of=as_of,
        max_age_days=max_age_days,
    )
    source_fabricated_snapshot = load_fabricated_part_life_snapshot(
        spec_df,
        now=as_of,
    )
    policy = ConfigLoader.get_data_forward_policy()
    return (
        policy.shift_frame(recent_source_snapshot, ("glass_start_time",)),
        policy.shift_frame(source_fabricated_snapshot, ("glass_start_time",)),
    )

def _generate_baseline_csv_from_excel(csv_path: Path) -> None:
    """Read configured Excel sheets via COM and generate one flattened CSV."""
    runtime_config = get_equipment_runtime_config()
    excel_path = runtime_config.source_excel_path
    if not excel_path.exists():
        raise FileNotFoundError(f"Original Excel not found: {excel_path}")
    logger.info(f"Reading encrypted Excel via COM: {excel_path}")
    sheet_rows = _read_baseline_sheets_from_excel(
        excel_path,
        runtime_config.source_sheet_names,
    )
    expanded_rows = _expand_baseline_rows_from_sheets(sheet_rows)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding=runtime_config.csv_encoding) as f:
        writer = csv.DictWriter(f, fieldnames=REQUIRED_BASELINE_COLUMNS)
        writer.writeheader()
        writer.writerows(expanded_rows)
    source_rows = sum(len(rows) for rows in sheet_rows.values())
    logger.info(
        "Generated CSV from %s sheets: %s source rows -> %s expanded rows",
        len(sheet_rows), source_rows, len(expanded_rows),
    )


def _read_baseline_sheets_from_excel(
    excel_path: Path,
    sheet_names: tuple[str, ...],
) -> dict[str, list[list[str]]]:
    """Read the raw data rows from each configured encrypted Excel sheet."""
    try:
        import pythoncom
        import win32com.client
    except ImportError as e:
        raise ImportError(f"pywin32 required: {e}")
    excel = None
    wb = None
    pythoncom.CoInitialize()
    try:
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(str(excel_path.resolve()))
        available_sheet_names = [wb.Sheets(i).Name for i in range(1, wb.Sheets.Count + 1)]
        missing_sheet_names = [
            name for name in sheet_names
            if name not in available_sheet_names
        ]
        if missing_sheet_names:
            raise ValueError(
                f"Configured sheets not found: {missing_sheet_names}. "
                f"Available sheets: {available_sheet_names}"
            )

        sheet_rows: dict[str, list[list[str]]] = {}
        for sheet_name in sheet_names:
            ws = wb.Sheets(sheet_name)
            rows = ws.UsedRange.Rows.Count
            cols = ws.UsedRange.Columns.Count
            if rows < 2:
                raise ValueError(f"Empty spec table in sheet '{sheet_name}' ({rows} rows)")
            raw_rows: list[list[str]] = []
            for row_number in range(2, rows + 1):
                raw_rows.append([
                    str(ws.Cells(row_number, column_number).Value).strip()
                    if ws.Cells(row_number, column_number).Value is not None else ""
                    for column_number in range(1, cols + 1)
                ])
            sheet_rows[sheet_name] = raw_rows
    finally:
        if wb is not None:
            wb.Close(False)
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()
    return sheet_rows


def _expand_baseline_rows_from_sheets(
    sheet_rows: Mapping[str, list[list[str]]],
) -> list[dict[str, str]]:
    """Expand station/machine cells and merge all configured sheet rows."""
    expanded_rows: list[dict[str, str]] = []
    for sheet_name, rows in sheet_rows.items():
        for row_number, values in enumerate(rows, start=2):
            if len(values) < len(REQUIRED_BASELINE_COLUMNS):
                raise ValueError(
                    f"Sheet '{sheet_name}' row {row_number} has {len(values)} columns; "
                    f"expected at least {len(REQUIRED_BASELINE_COLUMNS)}"
                )
            vals = values[:len(REQUIRED_BASELINE_COLUMNS)]
            stations = [station.strip() for station in vals[6].split("\n") if station.strip()] or [vals[6]]
            machines = [machine.strip() for machine in vals[7].split("\n") if machine.strip()] or [vals[7]]
            for station in stations:
                for machine in machines:
                    expanded_rows.append(dict(zip(
                        REQUIRED_BASELINE_COLUMNS,
                        [*vals[:6], station, machine, vals[8]],
                        strict=True,
                    )))
    return expanded_rows


def _query_part_life_from_db(
    db_manager: "DatabaseManager",
    spec_df: pd.DataFrame,
) -> pd.DataFrame:
    """Query eda.ARRAY_PDS_RESULT_T for part life data matching spec entries."""
    if db_manager.engine is None:
        logger.warning("Database engine not initialized.")
        return pd.DataFrame()
    pairs = spec_df[["站点", "机台号-腔室"]].drop_duplicates()
    unique_stations = pairs["站点"].dropna().unique().tolist()
    unique_machines = pairs["机台号-腔室"].dropna().unique().tolist()
    if not unique_stations or not unique_machines:
        logger.warning("No valid pairs in spec table.")
        return pd.DataFrame()
    runtime_config = get_equipment_runtime_config()
    cutoff_date = (datetime.now() - timedelta(days=runtime_config.query_lookback_days)).strftime("%Y-%m-%d")
    st_ph = ",".join([f":st{i}" for i in range(len(unique_stations))])
    mc_ph = ",".join([f":mc{i}" for i in range(len(unique_machines))])

    # Build LIKE OR conditions from spec patterns
    patterns = spec_df["参数名称"].dropna().unique().tolist()
    like_clauses = []
    for i, pat in enumerate(patterns):
        like_clauses.append(f"param_name LIKE :pat{i}")
    like_sql = " OR ".join(like_clauses) if like_clauses else "1=0"

    sql = f"""
        SELECT step_id, sub_equip_id, param_name, value, glass_start_time
        FROM {runtime_config.query_source_table}
        WHERE step_id IN ({st_ph})
          AND sub_equip_id IN ({mc_ph})
          AND glass_start_time >= TO_DATE(:cutoff_date, 'YYYY-MM-DD')
          AND ({like_sql})
        ORDER BY glass_start_time DESC
    """
    params = {"cutoff_date": cutoff_date}
    for i, st in enumerate(unique_stations):
        params[f"st{i}"] = st
    for i, mc in enumerate(unique_machines):
        params[f"mc{i}"] = mc
    for i, pat in enumerate(patterns):
        params[f"pat{i}"] = pat

    try:
        logger.info(
            f"Querying: {len(unique_stations)} stations, {len(unique_machines)} machines, "
            f"{len(patterns)} patterns, since {cutoff_date}"
        )
        df = pd.read_sql(text(sql), db_manager.engine, params=params)
        df.columns = df.columns.str.lower()
        if df.empty:
            logger.info("Empty result.")
            return df
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])
        df["glass_start_time"] = pd.to_datetime(df["glass_start_time"], errors="coerce")
        logger.info(f"Extracted {len(df)} records.")
        return df
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        return pd.DataFrame()

def _filter_by_param_patterns(
    df: pd.DataFrame,
    spec_df: pd.DataFrame,
) -> pd.DataFrame:
    """Filter rows where param_name matches any LIKE pattern from spec."""
    patterns = spec_df["参数名称"].dropna().unique().tolist()
    if not patterns:
        return df
    regex_parts = []
    for pat in patterns:
        pat_str = str(pat).strip()
        if not pat_str:
            continue
        escaped = re.escape(pat_str)
        escaped = escaped.replace("%", ".*")
        escaped = escaped.replace("_", ".")
        regex_parts.append("^" + escaped + "$")
    if not regex_parts:
        return df
    combined = "|".join(regex_parts)
    logger.info(f"Filtering param_name against {len(regex_parts)} LIKE patterns")
    mask = df["param_name"].astype(str).str.match(combined, case=False, na=False)
    return df[mask].copy()



# ==============================================================================
#  Repository (following SpcRepository pattern)
# ==============================================================================

class PartsRepository:
    """
    [仓储层] 关键备件数据仓储。

    职责：封装快照 TTL 检查、过期重查、签名管理。
    参考 src/inline_domain/infrastructure/spc/repositories/spc_repository.py 的 SpcRepository 模式。

    用法:
        repo = PartsRepository(db_manager, spec_df)
        snapshot_df = repo.get_snapshot()
    """

    def __init__(
        self,
        db_manager: "DatabaseManager",
        spec_df: pd.DataFrame,
        snapshot_dir: str | Path | None = None,
        snapshot_ttl_hours: int | None = None,
    ):
        runtime_config = get_equipment_runtime_config()
        self._db = db_manager
        self._spec_df = spec_df
        self._snapshot_dir = Path(snapshot_dir) if snapshot_dir is not None else runtime_config.snapshot_dir
        self._snapshot_ttl_hours = (
            snapshot_ttl_hours
            if snapshot_ttl_hours is not None
            else runtime_config.snapshot_ttl_hours
        )

        # 计算规格签名
        self._spec_hash = hashlib.md5(
            pd.util.hash_pandas_object(spec_df, index=True).values.tobytes()
        ).hexdigest()[:12]

        self._snapshot_path = self._snapshot_dir / f"part_life_snapshot_{self._spec_hash}.parquet"

    def get_snapshot(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        获取快照数据。

        策略:
        1. 快照存在且未过期 → 直接读取
        2. 快照过期或不存在 → 重新查询 DB 并保存
        3. force_refresh=True → 强制重新查询

        Args:
            force_refresh: 是否强制刷新

        Returns:
            pd.DataFrame: 快照数据
        """
        if not force_refresh and self._snapshot_valid():
            logger.info(f"Loading from snapshot: {self._snapshot_path}")
            df = pd.read_parquet(self._snapshot_path)
            logger.info(f"Loaded {len(df)} records")
            return df

        logger.info("Refreshing snapshot from database...")
        df = _query_part_life_from_db(self._db, self._spec_df)

        if not df.empty:
            self._snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(self._snapshot_path, index=False)
            logger.info(f"Saved snapshot: {self._snapshot_path} ({len(df)} records)")
        else:
            logger.warning("No data from DB, snapshot not updated.")

        return df

    def invalidate(self) -> None:
        """删除快照文件，强制下次重新查询。"""
        if self._snapshot_path.exists():
            self._snapshot_path.unlink()
            logger.info(f"Snapshot deleted: {self._snapshot_path}")

    def _snapshot_valid(self) -> bool:
        """检查快照是否存在且未过期。"""
        if not self._snapshot_path.exists():
            return False
        mtime = datetime.fromtimestamp(self._snapshot_path.stat().st_mtime)
        return (datetime.now() - mtime).total_seconds() < self._snapshot_ttl_hours * 3600
