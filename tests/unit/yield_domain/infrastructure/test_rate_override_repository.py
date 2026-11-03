from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import pandas as pd

from src.yield_domain.infrastructure import rate_override_repository
from src.yield_domain.infrastructure.rate_override_repository import (
    RateOverrideReadError,
    load_rate_overrides,
)


class _FakeSheets:
    def __init__(self, sheets: dict[str, tuple[tuple[object, ...], ...]]) -> None:
        self._sheets = sheets
        self.Count = len(sheets)

    def __call__(self, key: int | str) -> SimpleNamespace:
        if isinstance(key, int):
            name = list(self._sheets)[key - 1]
            return SimpleNamespace(Name=name)
        return SimpleNamespace(
            Name=key,
            UsedRange=SimpleNamespace(Value=lambda: self._sheets[key]),
        )


class _FakeWorkbook:
    def __init__(self, sheets: dict[str, tuple[tuple[object, ...], ...]]) -> None:
        self.Sheets = _FakeSheets(sheets)
        self.was_closed = False

    def Close(self, _save_changes: bool) -> None:
        self.was_closed = True


class _FakeExcel:
    def __init__(self, workbook: _FakeWorkbook) -> None:
        def open_workbook(_path, **kwargs):
            assert self.AutomationSecurity == 3
            assert kwargs == {'ReadOnly': True, 'UpdateLinks': 0}
            return workbook
        self.Workbooks = SimpleNamespace(Open=open_workbook)
        self.was_quit = False

    def Quit(self) -> None:
        self.was_quit = True


def _install_fake_excel(
    monkeypatch: pytest.MonkeyPatch,
    workbook: _FakeWorkbook,
) -> _FakeExcel:
    excel = _FakeExcel(workbook)
    monkeypatch.setattr(rate_override_repository.comtypes, "CoInitialize", lambda: None)
    monkeypatch.setattr(rate_override_repository.comtypes, "CoUninitialize", lambda: None)
    monkeypatch.setattr(
        rate_override_repository.comtypes.client,
        "CreateObject",
        lambda _name: excel,
    )
    return excel


def test_missing_product_sheet_means_no_rate_overrides(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workbook = _FakeWorkbook(
        {
            "M678": (
                ("lot_id", "sheet_id", "override_rate", "defect_desc"),
                ("L1", "S1", 0.1, "defect"),
            )
        }
    )
    excel = _install_fake_excel(monkeypatch, workbook)
    path = tmp_path / "override_rates.xlsx"
    path.touch()

    sheet_overrides, lot_overrides = load_rate_overrides(path, "M626")

    assert sheet_overrides is None
    assert lot_overrides is None
    assert workbook.was_closed
    assert excel.was_quit


def test_existing_product_sheet_with_invalid_schema_still_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workbook = _FakeWorkbook(
        {
            "M626": (
                ("lot_id", "sheet_id"),
                ("L1", "S1"),
            )
        }
    )
    _install_fake_excel(monkeypatch, workbook)
    path = tmp_path / "override_rates.xlsx"
    path.touch()

    with pytest.raises(RateOverrideReadError):
        load_rate_overrides(path, "M626")


def test_plain_xlsx_uses_no_com_and_preserves_percentage_and_sheet_semantics(tmp_path, monkeypatch):
    path = tmp_path / 'overrides.xlsx'
    rows = pd.DataFrame({'lot_id': ['L1', 'L1'], 'sheet_id': ['001', '002'],
                         'override_rate': ['2%', '4%'], 'defect_desc': [' C ', 'C']})
    rows.to_excel(path, sheet_name='M626', index=False)
    monkeypatch.setattr(rate_override_repository.comtypes.client, 'CreateObject',
                        lambda *_: pytest.fail('Plain XLSX must not start Excel'))
    sheet, lot = load_rate_overrides(path, 'm626')
    assert sheet['sheet_id'].tolist() == ['001', '002']
    assert sheet['override_rate'].tolist() == [.02, .04]
    assert lot['override_rate_avg'].tolist() == [.03]
    assert load_rate_overrides(path, 'MISSING') == (None, None)


def test_formula_without_cached_value_requires_excel_calculation(tmp_path, monkeypatch):
    path = tmp_path / 'formulas.xlsx'
    pd.DataFrame({'lot_id': ['L1'], 'sheet_id': ['S1'], 'override_rate': ['=1/100'],
                  'defect_desc': ['C']}).to_excel(path, sheet_name='M626', index=False, engine='openpyxl')
    workbook = _FakeWorkbook({'M626': (('lot_id', 'sheet_id', 'override_rate', 'defect_desc'),
                                      ('L1', 'S1', .01, 'C'))})
    excel = _install_fake_excel(monkeypatch, workbook)
    frame, _ = load_rate_overrides(path, 'M626')
    assert frame['override_rate'].tolist() == [.01]
    assert workbook.was_closed and excel.was_quit


def test_array_formula_is_not_silently_dropped_as_a_missing_override(tmp_path, monkeypatch):
    from openpyxl import Workbook
    from openpyxl.worksheet.formula import ArrayFormula

    path = tmp_path / 'array-formulas.xlsx'
    ordinary = Workbook()
    ordinary.active.title = 'M626'
    ordinary.active.append(['lot_id', 'sheet_id', 'override_rate', 'defect_desc'])
    ordinary.active.append(['L1', 'S1', None, 'C'])
    ordinary.active['C2'] = ArrayFormula(ref='C2', text='=1/100')
    ordinary.save(path)
    ordinary.close()
    workbook = _FakeWorkbook({'M626': (('lot_id', 'sheet_id', 'override_rate', 'defect_desc'),
                                      ('L1', 'S1', .01, 'C'))})
    _install_fake_excel(monkeypatch, workbook)
    frame, _ = load_rate_overrides(path, 'M626')
    assert frame['override_rate'].tolist() == [.01]
