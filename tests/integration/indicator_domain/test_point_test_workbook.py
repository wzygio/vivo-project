"""Public application + workbook adapter integration, no database."""

from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
import os

import openpyxl
import pandas as pd
import pytest

from src.indicator_domain.composition import build_point_test_service
from src.indicator_domain.infrastructure.point_test.workbook import PointTestWorkbook
from tests.unit.indicator_domain.point_test.test_report import MemorySource


def workbook_bytes() -> bytes:
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    for name, frame in MemorySource().read_sheets().items():
        sheet = workbook.create_sheet(name)
        for row in frame.itertuples(index=False, name=None):
            sheet.append([None if pd.isna(value) else value for value in row])
    output = BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()


def test_standard_workbook_uses_configured_complete_path_and_preserves_source(tmp_path):
    path = tmp_path / "renamed.xlsx"
    payload = workbook_bytes()
    path.write_bytes(payload)
    report = build_point_test_service(path).read_report()
    assert list(report) == ["Z576"]
    assert len(report["Z576"]["summary"]) == 36
    assert report["Z576"]["detail"].loc[0, "PARAM_VALUE"] == 12.543
    assert path.read_bytes() == payload


@pytest.mark.parametrize("failure", [False, True])
def test_enterprise_read_is_isolated_and_temp_files_are_cleaned(tmp_path, monkeypatch, failure):
    path = tmp_path / "encrypted.xlsx"
    path.write_bytes(b"\x00\x00\x00\x00enterprise")
    touched = []

    def convert(source, *, output_dir, strategy):
        assert Path(source) != path
        assert Path(source).read_bytes() == path.read_bytes()
        assert strategy == "excel-values"
        touched.append(Path(source).parent)
        if failure:
            raise RuntimeError("read failed")
        output_dir.mkdir()
        target = output_dir / "values.xlsx"
        target.write_bytes(workbook_bytes())
        return SimpleNamespace(output_path=target)

    monkeypatch.setattr("fr_file_decryption.decrypt_file", convert)
    if failure:
        with pytest.raises(RuntimeError, match="read failed"):
            build_point_test_service(path).read_report()
    else:
        assert len(build_point_test_service(path).read_report()["Z576"]["summary"]) == 36
    assert touched and not touched[0].exists()
    assert path.read_bytes() == b"\x00\x00\x00\x00enterprise"


def test_missing_workbook_is_an_error(tmp_path):
    with pytest.raises(FileNotFoundError):
        PointTestWorkbook(tmp_path / "missing.xlsx").read_sheets()


@pytest.mark.skipif(os.environ.get("VIVO_POINT_TEST_LIVE") != "1", reason="explicit local workbook verification")
def test_real_enterprise_workbook_matches_supplied_values():
    report = build_point_test_service().read_report()["Z576"]
    assert report["detail"].shape == (936, 41)
    assert report["summary"].groupby("阶段").size().to_dict() == {
        "调整前": 72, "调整后": 72, "修正后": 72,
    }
    summary = report["summary"]
    rows = summary[(summary["SHEET_ID"] == "L3Z86902B30") & (summary["膜位"] == "1EA0")]
    assert rows.set_index("阶段")["MAX"].to_dict() == {"调整后": 13.505, "修正后": 13.405}
