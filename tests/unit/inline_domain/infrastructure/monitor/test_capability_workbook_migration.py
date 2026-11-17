from pathlib import Path

import pandas as pd
import pytest
from openpyxl import Workbook, load_workbook

from src.inline_domain.infrastructure.monitor.capability_workbook_migration import split_summary_workbook
from src.inline_domain.application.monitor.capability_rebuild_provenance import workbook_signature


def test_split_preserves_alarm_formula_style_source_and_backups(tmp_path):
    source = tmp_path / "combined.xlsx"
    book = Workbook()
    alarm = book.active
    alarm.title = "M626报警率"
    alarm["A1"] = "formula"
    alarm["A2"] = "=1+2"
    alarm["A2"].number_format = "0.00%"
    for metric in ("CPK", "CPM"):
        sheet = book.create_sheet(f"M626 {metric}")
        sheet.append(["产品", "显示标签", "时间标签"])
        sheet.append(["M626", "W46", "2026-W46"])
    book.save(source)
    original = source.read_bytes()
    targets = {key: tmp_path / f"{key}.xlsx" for key in ("alarm", "cpk", "cpm")}
    frames = {f"M626 {metric}": pd.DataFrame({"产品": ["M626"], "时间标签": ["2026-W46"],
                                           f"{metric}总项目数": [10]}) for metric in ("CPK", "CPM")}
    result = split_summary_workbook(source, targets, revised_sheets=frames)
    assert source.read_bytes() == original
    assert Path(result["backup"]).read_bytes() == original
    check = load_workbook(targets["alarm"])
    assert check.sheetnames == ["M626报警率"]
    assert check.active["A2"].value == "=1+2"
    assert check.active["A2"].number_format == "0.00%"
    check.close()
    for metric in ("CPK", "CPM"):
        frame = pd.read_excel(targets[metric.lower()])
        assert "显示标签" not in frame
        assert frame[f"{metric}总项目数"].tolist() == [10]
    with pytest.raises(FileExistsError):
        split_summary_workbook(source, targets, revised_sheets=frames)
    assert not list(tmp_path.glob("*.staging-*.xlsx"))


def source_book(tmp_path):
    source = tmp_path / "combined.xlsx"
    with pd.ExcelWriter(source) as writer:
        for name in ("M626报警率", "M626 CPK", "M626 CPM"):
            pd.DataFrame({"key": [1], "value": [2]}).to_excel(writer, sheet_name=name, index=False)
    targets = {key: tmp_path / f"{key}.xlsx" for key in ("alarm", "cpk", "cpm")}
    return source, targets


def test_split_rejects_source_edited_since_historical_planning(tmp_path):
    source, targets = source_book(tmp_path)
    signature = workbook_signature(pd.read_excel(source, sheet_name=None))
    book = load_workbook(source)
    book["M626 CPK"]["B2"] = 999
    book.save(source)
    book.close()
    edited = source.read_bytes()
    with pytest.raises(RuntimeError, match="changed after historical planning"):
        split_summary_workbook(source, targets, revised_sheets={}, expected_source_signature=signature)
    assert source.read_bytes() == edited
    assert not any(path.exists() for path in targets.values())


def test_target_race_never_overwrites_or_deletes_another_writers_file(tmp_path, monkeypatch):
    import os
    source, targets = source_book(tmp_path)
    link = os.link

    def create_competing_file(stage, target):
        if target == targets["cpk"]:
            target.write_bytes(b"user-maintained content")
        link(stage, target)

    monkeypatch.setattr(os, "link", create_competing_file)
    with pytest.raises(FileExistsError):
        split_summary_workbook(source, targets, revised_sheets={})
    assert targets["cpk"].read_bytes() == b"user-maintained content"
    assert not targets["alarm"].exists()
    assert not targets["cpm"].exists()
    assert not list(tmp_path.glob("*.staging-*.xlsx"))
