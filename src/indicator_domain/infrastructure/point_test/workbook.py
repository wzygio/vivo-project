"""Standard XLSX and enterprise values-only reading with temporary isolation."""

from io import BytesIO
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory
from zipfile import is_zipfile

import pandas as pd

from src.indicator_domain.core.point_test.report import SHEET_PATTERN
from src.shared_kernel.config import ConfigLoader


def _read_sheets(payload: bytes) -> dict[str, pd.DataFrame]:
    with pd.ExcelFile(BytesIO(payload), engine="openpyxl") as workbook:
        return {name: workbook.parse(name, header=None)
                for name in workbook.sheet_names if SHEET_PATTERN.fullmatch(name)}


class PointTestWorkbook:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def read_sheets(self) -> dict[str, pd.DataFrame]:
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        if is_zipfile(self.path):
            return _read_sheets(self.path.read_bytes())

        from fr_file_decryption import decrypt_file
        import pythoncom

        staging = ConfigLoader.get_project_root() / "output" / "decrypted_files"
        staging.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix="point-test-", dir=staging) as directory:
            # Office touches only a caller-owned copy, never the maintained file.
            source_copy = Path(directory) / self.path.name
            copyfile(self.path, source_copy)
            # Keep the caller's COM apartment alive until all of the package's
            # worksheet/range proxies have been released on return. Its nested
            # CoInitialize/CoUninitialize pair then cannot tear down live proxies.
            pythoncom.CoInitialize()
            try:
                normalized = decrypt_file(
                    source_copy, output_dir=Path(directory) / "values", strategy="excel-values",
                )
                # Capture bytes immediately: enterprise protection can encrypt
                # disk output again. No decrypted file survives this read.
                return _read_sheets(Path(normalized.output_path).read_bytes())
            finally:
                pythoncom.CoUninitialize()
