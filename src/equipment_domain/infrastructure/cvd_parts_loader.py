"""按厂别-MMDD 读取 CVD 最新版本，兼容企业加密和偏移表头。"""

from datetime import date
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory
from zipfile import is_zipfile

import pandas as pd

from src.equipment_domain.core.cvd_parts import CVD_SOURCE_COLUMNS, parse_cvd_sheet_name
from src.shared_kernel.config import ConfigLoader


def _read_sheet(
    workbook: pd.ExcelFile, sheet_name: str, *, factory: str, updated_at: pd.Timestamp,
) -> pd.DataFrame:
    raw = workbook.parse(sheet_name=sheet_name, header=None)
    required = set(CVD_SOURCE_COLUMNS) - {"厂别", "测量时间"}
    for index, row in raw.iterrows():
        labels = row.map(lambda value: str(value).strip() if pd.notna(value) else "")
        if required <= set(labels):
            frame = raw.iloc[index + 1:].copy()
            frame.columns = labels
            source_columns = [column for column in CVD_SOURCE_COLUMNS if column in frame]
            frame = frame.loc[:, source_columns].dropna(how="all")
            if frame[["备件类型", "设备类型", "机台号"]].isna().any().any():
                raise ValueError(f"CVD Sheet {sheet_name!r} 备件身份字段存在空值")
            frame["厂别"] = factory
            frame["测量时间"] = updated_at
            return frame.loc[:, list(CVD_SOURCE_COLUMNS)].reset_index(drop=True)
    raise ValueError(f"CVD Sheet {sheet_name!r} 未找到完整字段表头")


def _read_latest_sheets(path: Path, *, as_of_date: str) -> pd.DataFrame:
    with pd.ExcelFile(path, engine="openpyxl") as workbook:
        selected: dict[str, tuple[pd.Timestamp, str]] = {}
        for sheet_name in workbook.sheet_names:
            factory, updated_at = parse_cvd_sheet_name(sheet_name, as_of_date=as_of_date)
            previous = selected.get(factory)
            if previous is not None and updated_at == previous[0]:
                raise ValueError(f"CVD Sheet 厂别与更新日期重复: {sheet_name!r}")
            if previous is None or updated_at > previous[0]:
                selected[factory] = updated_at, sheet_name
        frames = [
            _read_sheet(workbook, sheet_name, factory=factory, updated_at=updated_at)
            for factory, (updated_at, sheet_name) in selected.items()
        ]
    return pd.concat(frames, ignore_index=True)


def load_cvd_parts(path: str | Path, *, as_of_date: str = "") -> pd.DataFrame:
    """不改写源文件；加密源的值副本仅在本次读取期间存在。"""
    source_path = Path(path)
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    report_date = as_of_date or date.today().isoformat()
    if is_zipfile(source_path):
        return _read_latest_sheets(source_path, as_of_date=report_date)

    from fr_file_decryption import decrypt_file

    output_dir = ConfigLoader.get_project_root() / "output" / "decrypted_files"
    output_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="cvd-", dir=output_dir) as temporary_dir:
        # COM 仅接触临时副本，隔离企业加密客户端的读取行为。
        source_copy = Path(temporary_dir) / source_path.name
        copyfile(source_path, source_copy)
        result = decrypt_file(
            source_copy, output_dir=Path(temporary_dir) / "values", strategy="excel-values",
        )
        return _read_latest_sheets(result.output_path, as_of_date=report_date)
