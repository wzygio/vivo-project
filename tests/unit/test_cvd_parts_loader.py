import pandas as pd
import pytest

from src.equipment_domain.infrastructure.cvd_parts_loader import load_cvd_parts


def test_only_first_sheet_and_offset_header_are_read(tmp_path):
    path = tmp_path / "cvd.xlsx"
    source = pd.DataFrame({
        "厂别": ["OLED"], "备件类型": ["Mask"], "设备类型": ["CVD"],
        "膜层": ["SIN"], "制程": ["SIN DEPO"], "寿命规格": [6500],
        "站点": [21200], "机台号": ["M1"], "测量值": [2477],
        "进度": [352], "测量时间": [None],
    })
    with pd.ExcelWriter(path) as writer:
        source.to_excel(writer, sheet_name="first", index=False, startrow=6, startcol=2)
        pd.DataFrame({"ignored": [1]}).to_excel(writer, sheet_name="second")
    loaded = load_cvd_parts(path)
    assert len(loaded) == 1
    assert loaded.iloc[0]["机台号"] == "M1"
    assert loaded.iloc[0]["进度"] == 352
    assert not any(str(c).startswith("Unnamed") for c in loaded.columns)


def test_missing_workbook_is_not_reported_as_empty(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_cvd_parts(tmp_path / "missing.xlsx")
