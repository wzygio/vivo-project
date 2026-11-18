"""Native capability append preserves maintained workbook cells and formulas."""
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import openpyxl
from openpyxl.styles import PatternFill
import pandas as pd
import pytest

from src.inline_domain.infrastructure.spc import capability_workbook_writer as writer


def test_append_preserves_formulas_styles_text_ids_and_other_sheets(tmp_path):
    path = tmp_path / "capability.xlsx"
    book = openpyxl.Workbook()
    sheet = book.active
    sheet.title = "M678"
    columns = ["prod_code", "step_id", "note", "calculation", "flag"]
    sheet.append(columns)
    sheet.append(["M678", "00123", "operator note", "=1+2", True])
    sheet["D2"].fill = PatternFill("solid", fgColor="FFFF00")
    sheet["B2"].number_format = "@"
    other = book.create_sheet("OTHER")
    other["A1"] = "=4+5"
    other.merge_cells("A3:C3")
    book.save(path)
    book.close()
    before = path.read_bytes()
    rows = pd.DataFrame([["M678", "00456", None, None, False]], columns=columns)

    result = writer.create_capability_rows(path, "M678", rows)

    assert result.written, result.error
    assert path.with_suffix(".xlsx.bak").read_bytes() == before
    saved = openpyxl.load_workbook(path, data_only=False)
    try:
        assert saved.sheetnames == ["M678", "OTHER"]
        assert saved["M678"]["D2"].value == "=1+2"
        assert saved["M678"]["D2"].fill.fgColor.rgb == "00FFFF00"
        assert saved["M678"]["B2"].value == "00123"
        assert saved["M678"]["B2"].number_format == "@"
        assert list(saved["M678"].values)[-1] == ("M678", "00456", None, None, False)
        assert saved["OTHER"]["A1"].value == "=4+5"
        assert str(saved["OTHER"].merged_cells) == "A3:C3"
    finally:
        saved.close()


def test_new_sheet_and_new_file_are_created_without_replacing_existing_sheets(tmp_path):
    path = tmp_path / "capability.xlsx"
    columns = ["prod_code", "cpk_corrected"]
    assert writer.create_capability_rows(path, "M678", pd.DataFrame([["M678", .9]], columns=columns)).written
    assert writer.create_capability_rows(path, "Z622", pd.DataFrame([["Z622", .8]], columns=columns)).written
    book = openpyxl.load_workbook(path)
    try:
        assert book.sheetnames == ["M678", "Z622"]
        assert book["M678"]["B2"].value == .9
    finally:
        book.close()


def test_bad_existing_header_and_empty_append_do_not_touch_file(tmp_path):
    path = tmp_path / "capability.xlsx"
    pd.DataFrame({"operator_column": ["keep"]}).to_excel(path, sheet_name="M678", index=False)
    before = path.read_bytes()
    result = writer.create_capability_rows(path, "M678", pd.DataFrame({"cpk_corrected": [.9]}))
    assert not result.written and "manual maintenance" in result.error
    assert path.read_bytes() == before
    assert not writer.create_capability_rows(path, "M678", pd.DataFrame()).written
    assert path.read_bytes() == before
    assert sorted(item.name for item in tmp_path.iterdir()) == ["capability.xlsx"]


def test_source_change_during_append_is_not_overwritten(tmp_path, monkeypatch):
    path = tmp_path / "capability.xlsx"
    pd.DataFrame({"value": [1]}).to_excel(path, sheet_name="M678", index=False)
    actual = writer._append_openxml
    external = b"a newer operator version"
    def competing_write(*args):
        changed = actual(*args)
        path.write_bytes(external)
        return changed
    monkeypatch.setattr(writer, "_append_openxml", competing_write)
    result = writer.create_capability_rows(path, "M678", pd.DataFrame({"value": [2]}))
    assert not result.written and "changed during append" in result.error
    assert path.read_bytes() == external


@pytest.mark.parametrize("fail_save", [False, True])
@pytest.mark.parametrize("new_values", [("M678", .9), ("00123", pd.Timestamp("2026-11-09 12:34:56")),
                                        ("00123", float("-inf"))])
def test_encrypted_append_uses_native_ranges_and_closes_owned_workbook(tmp_path, monkeypatch, fail_save, new_values):
    path = tmp_path / "encrypted.xlsx"
    path.write_bytes(b"enterprise encrypted input")
    header = SimpleNamespace(Value=(("prod_code", "cpk_corrected"),))
    target = SimpleNamespace(Value=None)
    sheet = MagicMock()
    sheet.Name = "M678"
    sheet.UsedRange = SimpleNamespace(Row=1, Column=1, Columns=SimpleNamespace(Count=2), Rows=SimpleNamespace(Count=3))
    cells = {}
    def cell(row, column):
        return cells.setdefault((row, column), SimpleNamespace(position=(row, column)))
    sheet.Cells.side_effect = cell
    sheet.Range.side_effect = lambda start, end: header if start.position == (1, 1) else target
    workbook = MagicMock()
    workbook.ReadOnly = False
    workbook.Worksheets.__iter__.return_value = iter([sheet])
    workbook.Worksheets.return_value = sheet
    if fail_save:
        workbook.Save.side_effect = OSError("save failed")
    excel = MagicMock()
    excel.Workbooks.Open.return_value = workbook
    lifecycle = []
    @contextmanager
    def application():
        lifecycle.append("start")
        try:
            yield excel
        finally:
            lifecycle.append("quit")
    monkeypatch.setattr(writer, "_excel_application", application)
    result = writer.create_capability_rows(path, "M678", pd.DataFrame({"prod_code": [new_values[0]], "cpk_corrected": [new_values[1]]}))
    assert result.written is not fail_save
    assert target.Value == (tuple(writer._cell(value) for value in new_values),)
    sheet.Range.assert_any_call(cells[(4, 1)], cells[(4, 2)])
    assert cells[(4, 1)].NumberFormat == '@'
    if isinstance(new_values[1], pd.Timestamp):
        assert cells[(4, 2)].NumberFormat == 'yyyy-mm-dd hh:mm:ss'
    elif new_values[1] == float('-inf'):
        assert cells[(4, 2)].NumberFormat == '@'
    workbook.Close.assert_called_once_with(SaveChanges=False)
    assert lifecycle == ["start", "quit"]
    assert excel.Workbooks.Open.call_args.kwargs == {"ReadOnly": False, "UpdateLinks": 0}
    if fail_save:
        assert path.read_bytes() == b"enterprise encrypted input"


def test_create_header_only_missing_sheet_then_leave_it_unchanged(tmp_path):
    path = tmp_path / 'capabilities.xlsx'
    pd.DataFrame({'note': ['retain']}).to_excel(path, sheet_name='NOTES', index=False)
    empty = pd.DataFrame(columns=['prod_code', 'cpk_corrected'])
    assert writer.create_capability_rows(path, 'M678', empty).written
    before = path.read_bytes()
    result = writer.create_capability_rows(path, 'M678', empty)
    assert not result.written and result.error is None
    assert path.read_bytes() == before
    assert pd.read_excel(path, sheet_name='NOTES')['note'].tolist() == ['retain']


def test_openxml_creation_retains_text_dates_and_infinite_metric_semantics(tmp_path):
    path = tmp_path / 'capabilities.xlsx'
    rows = pd.DataFrame({'step_id': ['00123'], 'period_start': [pd.Timestamp('2026-11-09 12:34:56')],
                         'cpk_corrected': [float('-inf')]})
    assert writer.create_capability_rows(path, 'M678', rows).written
    book = openpyxl.load_workbook(path)
    try:
        assert list(book['M678'].values)[1] == ('00123', pd.Timestamp('2026-11-09 12:34:56').to_pydatetime(), '-inf')
    finally:
        book.close()


def test_new_text_records_are_not_interpreted_as_formulas(tmp_path):
    path = tmp_path / 'capabilities.xlsx'
    assert writer.create_capability_rows(path, 'M678', pd.DataFrame({'param_name': ['=1+2']})).written
    book = openpyxl.load_workbook(path)
    try:
        assert book['M678']['A2'].value == '=1+2'
        assert book['M678']['A2'].data_type == 's'
    finally:
        book.close()


@pytest.mark.parametrize("reused", [False, True])
def test_excel_application_cleans_only_its_new_process(monkeypatch, reused):
    import pythoncom
    import win32com.client
    import win32process

    excel = MagicMock()
    process = MagicMock()
    process.create_time.return_value = 42.0
    initialize, uninitialize = MagicMock(), MagicMock()
    monkeypatch.setattr(pythoncom, "CoInitialize", initialize)
    monkeypatch.setattr(pythoncom, "CoUninitialize", uninitialize)
    monkeypatch.setattr(win32com.client, "DispatchEx", lambda name: excel)
    monkeypatch.setattr(win32process, "GetWindowThreadProcessId", lambda hwnd: (1, 12345))
    monkeypatch.setattr(writer, "_excel_process_ids", lambda: {12345} if reused else set())
    monkeypatch.setattr(writer.psutil, "Process", lambda pid: process)
    if reused:
        with pytest.raises(RuntimeError, match="not task-owned"):
            with writer._excel_application():
                pytest.fail("Reused application was exposed")
        excel.Quit.assert_not_called()
        process.wait.assert_not_called()
    else:
        with pytest.raises(ValueError, match="task failed"):
            with writer._excel_application() as application:
                assert application is excel
                assert application.AutomationSecurity == 3
                raise ValueError("task failed")
        excel.Quit.assert_called_once()
        process.wait.assert_called_once_with(timeout=5)
    initialize.assert_called_once()
    uninitialize.assert_called_once()
