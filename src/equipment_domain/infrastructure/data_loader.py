# src/equipment_domain/infrastructure/data_loader.py
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
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, List

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)

REQUIRED_BASELINE_COLUMNS: List[str] = [
    "厂别", "备件类型", "设备类型",
    "膜层", "制程", "寿命规格",
    "站点", "机台号-腔室", "参数名称",
]

ORIGINAL_EXCEL_PATH: str = "resources/关键备件/供应商关键备件寿命管控清单 - new.xlsx"
ORIGINAL_SHEET_NAME: str = "规格表"
SNAPSHOT_DIR: str = "data/equipment"
SNAPSHOT_TTL_HOURS: int = 8
QUERY_LOOKBACK_DAYS: int = 90

def load_spec_baseline(baseline_path: str | Path) -> pd.DataFrame:
    """Load spec baseline config. Prefer CSV, fallback to encrypted Excel."""
    path = Path(baseline_path)
    if not path.exists():
        logger.info(f"CSV not found, generating from encrypted Excel")
        _generate_baseline_csv_from_excel(path)
    logger.info(f"Loading spec baseline CSV: {path}")
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    missing_cols = [col for col in REQUIRED_BASELINE_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}. Current: {list(df.columns)}")
    df["寿命规格"] = df["寿命规格"].astype(str).str.replace(r"[^\d.]", "", regex=True)
    df["寿命规格"] = pd.to_numeric(df["寿命规格"], errors="coerce")
    df = df.dropna(how="all").reset_index(drop=True)
    logger.info(f"Loaded {len(df)} spec baseline records")
    return df

def load_part_life_snapshot(
    db_manager: "DatabaseManager",
    spec_df: pd.DataFrame,
) -> pd.DataFrame:
    """Query DB for part life data matching spec table, with Parquet snapshot caching."""
    spec_hash = hashlib.md5(
        pd.util.hash_pandas_object(spec_df, index=True).values.tobytes()
    ).hexdigest()[:12]
    snapshot_path = Path(SNAPSHOT_DIR) / f"part_life_snapshot_{spec_hash}.parquet"
    if snapshot_path.exists():
        mtime = datetime.fromtimestamp(snapshot_path.stat().st_mtime)
        if (datetime.now() - mtime).total_seconds() < SNAPSHOT_TTL_HOURS * 3600:
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

def _generate_baseline_csv_from_excel(csv_path: Path) -> None:
    """Read encrypted Excel via COM, split multi-value cells, generate flat CSV."""
    excel_path = Path(ORIGINAL_EXCEL_PATH)
    if not excel_path.exists():
        raise FileNotFoundError(f"Original Excel not found: {excel_path}")
    logger.info(f"Reading encrypted Excel via COM: {excel_path}")
    try:
        import pythoncom
        import win32com.client
    except ImportError as e:
        raise ImportError(f"pywin32 required: {e}")
    pythoncom.CoInitialize()
    try:
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(str(excel_path.resolve()))
        target_sheet = None
        for i in range(1, wb.Sheets.Count + 1):
            if wb.Sheets(i).Name == ORIGINAL_SHEET_NAME:
                target_sheet = wb.Sheets(i)
                break
        if target_sheet is None:
            sheet_names = [wb.Sheets(i).Name for i in range(1, wb.Sheets.Count + 1)]
            wb.Close(False)
            excel.Quit()
            raise ValueError(f"Sheet not found: {ORIGINAL_SHEET_NAME}")
        ws = target_sheet
        rows = ws.UsedRange.Rows.Count
        cols = ws.UsedRange.Columns.Count
        if rows < 2:
            wb.Close(False)
            excel.Quit()
            raise ValueError(f"Empty spec table ({rows} rows)")
        all_rows: list[list[str]] = []
        for r in range(2, rows + 1):
            vals = []
            for c in range(1, cols + 1):
                v = ws.Cells(r, c).Value
                vals.append(str(v).strip() if v is not None else "")
            all_rows.append(vals)
        wb.Close(False)
        excel.Quit()
    finally:
        pythoncom.CoUninitialize()
    expanded_rows: list[dict] = []
    for vals in all_rows:
        stations = [s.strip() for s in vals[6].split("\n") if s.strip()]
        machines = [m.strip() for m in vals[7].split("\n") if m.strip()]
        if not stations:
            stations = [vals[6]]
        if not machines:
            machines = [vals[7]]
        for st in stations:
            for mc in machines:
                expanded_rows.append({
                    "厂别": vals[0], "备件类型": vals[1], "设备类型": vals[2],
                    "膜层": vals[3], "制程": vals[4], "寿命规格": vals[5],
                    "站点": st, "机台号-腔室": mc, "参数名称": vals[8],
                })
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=REQUIRED_BASELINE_COLUMNS)
        writer.writeheader()
        writer.writerows(expanded_rows)
    logger.info(f"Generated CSV: {len(all_rows)} orig -> {len(expanded_rows)} rows")


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
    cutoff_date = (datetime.now() - timedelta(days=QUERY_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
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
        FROM eda.ARRAY_PDS_RESULT_T
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
    参考 src/spc_domain/infrastructure/repositories/spc_repository.py 的 SpcRepository 模式。

    用法:
        repo = PartsRepository(db_manager, spec_df)
        snapshot_df = repo.get_snapshot()
    """

    SNAPSHOT_TTL_HOURS = 8

    def __init__(
        self,
        db_manager: "DatabaseManager",
        spec_df: pd.DataFrame,
        snapshot_dir: str = SNAPSHOT_DIR,
    ):
        self._db = db_manager
        self._spec_df = spec_df
        self._snapshot_dir = Path(snapshot_dir)

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
        return (datetime.now() - mtime).total_seconds() < self.SNAPSHOT_TTL_HOURS * 3600