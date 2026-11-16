import io
from pathlib import Path

import pandas as pd
from types import SimpleNamespace

from app.components.file_uploader import (
    YIELD_MODIFIER_CONFIG_KEY,
    YIELD_MODIFIER_TEMPLATES,
    _product_sheet_name,
    _refresh_uploaded_config,
)
from src.shared_kernel.utils.excel_tools import read_workbook_sheet, replace_workbook_sheet


def test_upload_refresh_is_product_scoped_and_shared_mapping_covers_other_products(monkeypatch):
    from app.components import page_header
    from src.shared_kernel.config import ConfigLoader
    resets, invalidations = [], []
    monkeypatch.setattr(page_header, "perform_hard_reset", lambda **kwargs: resets.append(kwargs))
    monkeypatch.setattr(page_header, "invalidate_page_cache", lambda **kwargs: invalidations.append(kwargs))
    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda: ["M678", "Z553"])
    config = SimpleNamespace(data_source=SimpleNamespace(product_code="Z553"))
    _refresh_uploaded_config(config, "yield_modifier_config")
    assert not invalidations
    assert resets[-1]["product_cache_scope"] == "Z553"
    _refresh_uploaded_config(config, "mapping_config")
    assert [call["product_code"] for call in invalidations] == ["M678"]
    assert "yield_report_source" in resets[-1]["product_cache_indicators"]


def test_product_sheet_name_mapping() -> None:
    assert _product_sheet_name("M678", "Sheet1") == "M678"
    assert _product_sheet_name("M678", "Group级") == "M678_Group级"
    assert _product_sheet_name("M678", "Code级") == "M678_Code级"


def test_first_upload_tab_targets_yield_modifier_table() -> None:
    assert YIELD_MODIFIER_CONFIG_KEY == "yield_modifier_config"
    assert set(YIELD_MODIFIER_TEMPLATES) == {"Group级", "Code级"}
    expected_columns = [
        "不良类型",
        "周期类型",
        "时间标签",
        "当月良损",
        "指定良损",
        "缩放倍数",
    ]
    assert list(YIELD_MODIFIER_TEMPLATES["Group级"].columns) == expected_columns


def test_shared_workbook_upload_download_round_trip(tmp_path: Path) -> None:
    """模拟 Tab1 上传：把模板名的 sheets 写进共享工作簿，再按产品读回，互不影响。"""
    workbook = tmp_path / "入库良率修饰表.xlsx"
    template_dfs = YIELD_MODIFIER_TEMPLATES

    # M678 上传
    uploaded = pd.DataFrame(
        [
            {
                "不良类型": "组A",
                "周期类型": "月度",
                "时间标签": "2026-07",
                "当月良损": 0.01,
                "指定良损": 0.008,
                "缩放倍数": 0.8,
            }
        ]
    )
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        uploaded.to_excel(writer, index=False, sheet_name="Group级")
        pd.DataFrame(
            [
                {
                    "不良类型": "G向单暗线",
                    "周期类型": "月度",
                    "时间标签": "2026-07",
                    "当月良损": 0.003,
                    "指定良损": 0.002,
                    "缩放倍数": 0.667,
                }
            ]
        ).to_excel(writer, index=False, sheet_name="Code级")
    buffer.seek(0)

    uploaded_xls = pd.read_excel(buffer, sheet_name=None)
    for tpl_name in template_dfs:
        replace_workbook_sheet(workbook, _product_sheet_name("M678", tpl_name), uploaded_xls[tpl_name])

    # M626 上传（不同数据），不应影响 M678 的 sheet
    m626_df = pd.DataFrame(
        [
            {
                "不良类型": "组B",
                "周期类型": "月度",
                "时间标签": "2026-07",
                "当月良损": 0.02,
                "指定良损": 0.01,
                "缩放倍数": 0.5,
            }
        ]
    )
    replace_workbook_sheet(workbook, _product_sheet_name("M626", "Group级"), m626_df)

    m678_group = read_workbook_sheet(workbook, "M678_Group级")
    m626_group = read_workbook_sheet(workbook, "M626_Group级")
    assert m678_group["不良类型"].tolist() == ["组A"]
    assert m626_group["不良类型"].tolist() == ["组B"]
