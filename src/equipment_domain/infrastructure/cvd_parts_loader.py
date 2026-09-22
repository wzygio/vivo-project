"""读取 CVD 工作簿首个 sheet，兼容企业加密和偏移表头。"""

from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import is_zipfile

import pandas as pd

from src.equipment_domain.core.cvd_parts import CVD_SOURCE_COLUMNS
from src.shared_kernel.config import ConfigLoader


def _read_first_sheet(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=0, header=None, engine="openpyxl")
    required = set(CVD_SOURCE_COLUMNS)
    for index, row in raw.iterrows():
        labels = row.map(lambda value: str(value).strip() if pd.notna(value) else "")
        if required <= set(labels):
            frame = raw.iloc[index + 1:].copy()
            frame.columns = labels
            frame = frame.loc[:, list(CVD_SOURCE_COLUMNS)].dropna(how="all")
            if frame[["厂别", "备件类型", "设备类型", "机台号"]].isna().any().any():
                raise ValueError("CVD 备件身份字段存在空值")
            return frame.reset_index(drop=True)
    raise ValueError("CVD 首个 sheet 未找到完整字段表头")


def load_cvd_parts(path: str | Path) -> pd.DataFrame:
    """不改写源文件；加密源的值副本仅在本次读取期间存在。"""
    source_path = Path(path)
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    if is_zipfile(source_path):
        return _read_first_sheet(source_path)

    from fr_file_decryption import decrypt_file

    output_dir = ConfigLoader.get_project_root() / "output" / "decrypted_files"
    output_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="cvd-", dir=output_dir) as temporary_dir:
        result = decrypt_file(
            source_path, output_dir=Path(temporary_dir), strategy="excel-values",
        )
        return _read_first_sheet(result.output_path)
