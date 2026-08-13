import io
from pathlib import Path

import pandas as pd

from app.components.file_uploader import _product_sheet_name
from src.shared_kernel.utils.excel_tools import read_workbook_sheet, replace_workbook_sheet


def test_product_sheet_name_mapping() -> None:
    assert _product_sheet_name("M678", "Sheet1") == "M678"
    assert _product_sheet_name("M678", "Group级") == "M678_Group级"
    assert _product_sheet_name("M678", "Code级") == "M678_Code级"


def test_shared_workbook_upload_download_round_trip(tmp_path: Path) -> None:
    """模拟 Tab1 上传：把模板名的 sheets 写进共享工作簿，再按产品读回，互不影响。"""
    workbook = tmp_path / "趋势图人工修正.xlsx"
    template_dfs = {
        "Group级": pd.DataFrame(columns=["目标名称", "周期类型", "时间标签", "期望不良率"]),
        "Code级": pd.DataFrame(columns=["目标名称", "周期类型", "时间标签", "期望不良率"]),
    }

    # M678 上传
    uploaded = pd.DataFrame(
        [{"目标名称": "组A", "周期类型": "月度", "时间标签": "2026-07", "期望不良率": 0.01}]
    )
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        uploaded.to_excel(writer, index=False, sheet_name="Group级")
        pd.DataFrame(
            [{"目标名称": "G向单暗线", "周期类型": "日度", "时间标签": "2026-07-01", "期望不良率": 0.003}]
        ).to_excel(writer, index=False, sheet_name="Code级")
    buffer.seek(0)

    uploaded_xls = pd.read_excel(buffer, sheet_name=None)
    for tpl_name in template_dfs:
        replace_workbook_sheet(workbook, _product_sheet_name("M678", tpl_name), uploaded_xls[tpl_name])

    # M626 上传（不同数据），不应影响 M678 的 sheet
    m626_df = pd.DataFrame(
        [{"目标名称": "组B", "周期类型": "月度", "时间标签": "2026-07", "期望不良率": 0.02}]
    )
    replace_workbook_sheet(workbook, _product_sheet_name("M626", "Group级"), m626_df)

    m678_group = read_workbook_sheet(workbook, "M678_Group级")
    m626_group = read_workbook_sheet(workbook, "M626_Group级")
    assert m678_group["目标名称"].tolist() == ["组A"]
    assert m626_group["目标名称"].tolist() == ["组B"]
