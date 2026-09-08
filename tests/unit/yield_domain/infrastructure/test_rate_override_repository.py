from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

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
        self.Workbooks = SimpleNamespace(Open=lambda _path: workbook)
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
