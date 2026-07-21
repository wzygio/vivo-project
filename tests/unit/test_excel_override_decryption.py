from pathlib import Path

import pandas as pd

from src.shared_kernel.utils import excel_tools
from src.yield_domain.application.excel_service import ExcelService


def test_parse_override_excel_uses_com_fallback_for_encrypted_workbook(
    tmp_path: Path,
    monkeypatch,
) -> None:
    encrypted_path = tmp_path / "encrypted_override.xlsx"
    encrypted_path.write_bytes(b"encrypted-placeholder")

    def fail_openpyxl(*args, **kwargs):
        raise ValueError("File is not a zip file")

    def read_via_com(path: Path, sheet_name: str | None = None) -> pd.DataFrame:
        assert path == encrypted_path
        if sheet_name == "Code级":
            return pd.DataFrame(
                [{"目标名称": "G向单暗线", "周期类型": "周度", "时间标签": "2026-W28", "期望不良率": 0.0029}]
            )
        return pd.DataFrame(columns=["目标名称", "周期类型", "时间标签", "期望不良率"])

    monkeypatch.setattr(pd, "read_excel", fail_openpyxl)
    monkeypatch.setattr(excel_tools, "_read_encrypted_xlsx_via_com", read_via_com)

    overrides = ExcelService._parse_override_excel(encrypted_path)

    assert overrides["code_weekly_values"] == {"G向单暗线": {"2026-W28": 0.0029}}
