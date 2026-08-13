from pathlib import Path

import pandas as pd
import pytest

from src.shared_kernel.utils.excel_tools import (
    read_workbook_sheet,
    replace_workbook_sheet,
)


def test_read_workbook_sheet_returns_empty_for_missing_file(tmp_path: Path) -> None:
    result = read_workbook_sheet(tmp_path / "missing.xlsx", "M678")
    assert result.empty


def test_read_workbook_sheet_returns_empty_for_missing_sheet(tmp_path: Path) -> None:
    workbook = tmp_path / "shared.xlsx"
    pd.DataFrame({"a": [1]}).to_excel(workbook, index=False, sheet_name="M626")

    result = read_workbook_sheet(workbook, "M678")
    assert result.empty


def test_read_workbook_sheet_reads_only_target_sheet(tmp_path: Path) -> None:
    workbook = tmp_path / "shared.xlsx"
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame({"a": [1]}).to_excel(writer, index=False, sheet_name="M626")
        pd.DataFrame({"b": [2, 3]}).to_excel(writer, index=False, sheet_name="M678")

    result = read_workbook_sheet(workbook, "M678")
    pd.testing.assert_frame_equal(result, pd.DataFrame({"b": [2, 3]}))


def test_read_workbook_sheet_falls_back_to_com_for_encrypted_file(monkeypatch, tmp_path: Path) -> None:
    workbook = tmp_path / "encrypted.xlsx"
    workbook.write_bytes(b"\x00not-a-real-xlsx")
    expected = pd.DataFrame({"a": [1]})

    monkeypatch.setattr(pd, "read_excel", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("encrypted")))
    import src.shared_kernel.utils.excel_tools as excel_tools

    monkeypatch.setattr(excel_tools, "_read_encrypted_xlsx_via_com", lambda path, sheet_name=None: expected)

    result = read_workbook_sheet(workbook, "M678")
    pd.testing.assert_frame_equal(result, expected)


def test_replace_workbook_sheet_creates_new_workbook(tmp_path: Path) -> None:
    workbook = tmp_path / "shared.xlsx"
    replace_workbook_sheet(workbook, "M678", pd.DataFrame({"a": [1, 2]}))

    assert workbook.exists()
    result = pd.read_excel(workbook, sheet_name="M678")
    pd.testing.assert_frame_equal(result, pd.DataFrame({"a": [1, 2]}))


def test_replace_workbook_sheet_preserves_other_sheets(tmp_path: Path) -> None:
    workbook = tmp_path / "shared.xlsx"
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame({"keep": [1]}).to_excel(writer, index=False, sheet_name="M626")
        pd.DataFrame({"old": [9]}).to_excel(writer, index=False, sheet_name="M678")

    replace_workbook_sheet(workbook, "M678", pd.DataFrame({"new": [5]}))

    preserved = pd.read_excel(workbook, sheet_name="M626")
    replaced = pd.read_excel(workbook, sheet_name="M678")
    pd.testing.assert_frame_equal(preserved, pd.DataFrame({"keep": [1]}))
    pd.testing.assert_frame_equal(replaced, pd.DataFrame({"new": [5]}))


def test_replace_workbook_sheet_skips_locked_workbook_without_raising(monkeypatch, tmp_path: Path) -> None:
    workbook = tmp_path / "shared.xlsx"
    original = pd.DataFrame({"a": [1]})
    original.to_excel(workbook, index=False, sheet_name="M678")

    import openpyxl

    def _locked_save(self, path):
        raise PermissionError("locked")

    monkeypatch.setattr(openpyxl.Workbook, "save", _locked_save)

    replace_workbook_sheet(workbook, "M678", pd.DataFrame({"a": [2]}))

    # 原内容未被破坏
    pd.testing.assert_frame_equal(pd.read_excel(workbook, sheet_name="M678"), original)


def test_replace_workbook_sheet_rewrites_encrypted_workbook_via_com(monkeypatch, tmp_path: Path) -> None:
    workbook = tmp_path / "encrypted.xlsx"
    workbook.write_bytes(b"\x00enterprise-encrypted")

    import openpyxl
    import src.shared_kernel.utils.excel_tools as excel_tools

    monkeypatch.setattr(
        openpyxl, "load_workbook", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("encrypted"))
    )
    monkeypatch.setattr(
        excel_tools,
        "_read_all_sheets_via_com",
        lambda path: {"M626": pd.DataFrame({"keep": [1]}), "M678": pd.DataFrame({"old": [9]})},
    )

    replace_workbook_sheet(workbook, "M678", pd.DataFrame({"new": [5]}))

    monkeypatch.undo()  # 恢复 openpyxl.load_workbook，以便真实读回结果

    pd.testing.assert_frame_equal(pd.read_excel(workbook, sheet_name="M626"), pd.DataFrame({"keep": [1]}))
    pd.testing.assert_frame_equal(pd.read_excel(workbook, sheet_name="M678"), pd.DataFrame({"new": [5]}))
