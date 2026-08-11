import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

from src.shared_kernel.utils.excel_tools import _read_encrypted_xlsx_via_com


class FakeWorkbook:
    def __init__(self) -> None:
        self.close_calls: list[bool] = []
        self.Worksheets = lambda selector: type(
            "FakeWorksheet",
            (),
            {"UsedRange": type("FakeRange", (), {"Value": (("a", "b"), (1, 2))})()},
        )()

    def Close(self, SaveChanges: bool) -> None:
        self.close_calls.append(SaveChanges)


class FakeExcel:
    def __init__(self, workbook: FakeWorkbook | None = None, open_error: Exception | None = None) -> None:
        self.workbook = workbook
        self.open_error = open_error
        self.open_calls: list[tuple[str, bool]] = []
        self.quit_calls = 0
        self.Workbooks = type("FakeWorkbooks", (), {"Open": self._open})()

    def _open(self, path: str, ReadOnly: bool = False):
        self.open_calls.append((path, ReadOnly))
        if self.open_error is not None:
            raise self.open_error
        return self.workbook

    def Quit(self) -> None:
        self.quit_calls += 1


def _install_fake_com(monkeypatch, excel: FakeExcel) -> dict[str, int]:
    counters = {"initialize": 0, "uninitialize": 0, "dispatch_ex": 0, "dispatch": 0}
    pythoncom = ModuleType("pythoncom")
    pythoncom.CoInitialize = lambda: counters.__setitem__("initialize", counters["initialize"] + 1)
    pythoncom.CoUninitialize = lambda: counters.__setitem__(
        "uninitialize", counters["uninitialize"] + 1
    )

    client = ModuleType("win32com.client")
    client.DispatchEx = lambda name: (
        counters.__setitem__("dispatch_ex", counters["dispatch_ex"] + 1) or excel
    )
    client.Dispatch = lambda name: (
        counters.__setitem__("dispatch", counters["dispatch"] + 1) or excel
    )
    win32com = ModuleType("win32com")
    win32com.client = client

    monkeypatch.setitem(sys.modules, "pythoncom", pythoncom)
    monkeypatch.setitem(sys.modules, "win32com", win32com)
    monkeypatch.setitem(sys.modules, "win32com.client", client)
    return counters


def test_read_encrypted_xlsx_uses_isolated_excel_and_releases_com(monkeypatch) -> None:
    workbook = FakeWorkbook()
    excel = FakeExcel(workbook=workbook)
    counters = _install_fake_com(monkeypatch, excel)

    result = _read_encrypted_xlsx_via_com(Path("encrypted.xlsx"))

    pd.testing.assert_frame_equal(result, pd.DataFrame([{"a": 1, "b": 2}]))
    assert counters == {
        "initialize": 1,
        "uninitialize": 1,
        "dispatch_ex": 1,
        "dispatch": 0,
    }
    assert workbook.close_calls == [False]
    assert excel.open_calls[0][1] is True
    assert excel.quit_calls == 1


def test_read_encrypted_xlsx_releases_excel_when_open_fails(monkeypatch) -> None:
    excel = FakeExcel(open_error=RuntimeError("open failed"))
    counters = _install_fake_com(monkeypatch, excel)

    with pytest.raises(RuntimeError, match="open failed"):
        _read_encrypted_xlsx_via_com(Path("encrypted.xlsx"))

    assert counters["uninitialize"] == 1
    assert counters["dispatch_ex"] == 1
    assert counters["dispatch"] == 0
    assert excel.quit_calls == 1
