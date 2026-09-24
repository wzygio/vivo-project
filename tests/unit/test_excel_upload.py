from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest

from src.shared_kernel.utils import excel_tools


def test_standard_upload_reads_first_sheet_without_disk_or_com(monkeypatch) -> None:
    expected = pd.DataFrame({"step_id": ["00120"], "flag": [False]})
    payload = BytesIO()
    with pd.ExcelWriter(payload, engine="openpyxl") as writer:
        expected.to_excel(writer, index=False)

    def unexpected_fallback(*args, **kwargs):
        pytest.fail("Standard uploads should not need Excel COM")

    monkeypatch.setattr(excel_tools, "_read_encrypted_xlsx_via_com", unexpected_fallback)
    actual = excel_tools.read_uploaded_excel_sheet(payload.getbuffer())
    assert actual.flag.tolist() == [False]
    assert len(actual) == 1


@pytest.mark.parametrize("read_fails", [False, True])
def test_encrypted_upload_stages_bytes_and_cleans_up(monkeypatch, tmp_path, read_fails) -> None:
    payload = b"\x00\x00\x00\x00enterprise-encrypted-workbook"
    expected = pd.DataFrame({"flag": [True], "cpk_corrected": [1.37]})
    staged_paths = []
    monkeypatch.setattr(excel_tools.ConfigLoader, "get_project_root", lambda: tmp_path)

    def read_staged(path: Path) -> pd.DataFrame:
        staged_paths.append(path)
        assert path.read_bytes() == payload
        assert path.is_relative_to(tmp_path / "output" / "tmp")
        assert path.suffix == ".xlsx"
        if read_fails:
            raise RuntimeError("Excel unavailable")
        return expected

    monkeypatch.setattr(excel_tools, "_read_encrypted_xlsx_via_com", read_staged)
    if read_fails:
        with pytest.raises(RuntimeError, match="Excel unavailable"):
            excel_tools.read_uploaded_excel_sheet(payload)
    else:
        pd.testing.assert_frame_equal(excel_tools.read_uploaded_excel_sheet(payload), expected)
    assert len(staged_paths) == 1
    assert not staged_paths[0].exists()

