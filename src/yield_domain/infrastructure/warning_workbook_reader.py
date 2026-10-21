"""Read the warning matrix without changing encrypted maintained workbooks."""
from pathlib import Path
from zipfile import BadZipFile

import pandas as pd

from src.shared_kernel.utils import excel_tools


def read_warning_matrix(path: Path, sheet_name: str) -> pd.DataFrame:
    """Retain header=None semantics through the existing read-only COM fallback."""
    try:
        return pd.read_excel(path, header=None, dtype=str, engine='openpyxl', sheet_name=sheet_name)
    except BadZipFile:
        frame = excel_tools._read_encrypted_xlsx_via_com(path, sheet_name=sheet_name)
        if not len(frame.columns):
            raise ValueError('Warning worksheet is missing or empty') from None
        header = pd.DataFrame([list(frame.columns)])
        body = frame.copy()
        body.columns = range(len(body.columns))
        return pd.concat([header, body], ignore_index=True).astype('string').fillna('')
