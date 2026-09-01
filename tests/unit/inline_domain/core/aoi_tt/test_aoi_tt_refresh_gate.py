"""AOI_TT core 刷新门控测试：scope 门控 missing 首写 → unchanged 不写 → revision 重写。

与 SPC/CTQ 共用 ``persist_sheet_oos_decoration`` 门控（参照
``tests/unit/inline_domain/core/shared/test_sheet_oos_refresh.py`` 模式），
全部使用 tmp_path 临时工作簿，不触碰真实 resources/。
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.inline_domain.core.aoi_tt.aoi_tt_decoration import prepare_aoi_tt_decoration
from src.inline_domain.core.shared import sheet_oos_decoration
from src.inline_domain.core.shared.sheet_oos_decoration import REFRESH_META_SHEET_NAME

NOW = datetime(2026, 8, 18, 12, 0, 0)


def _tt_details_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "start_time": pd.Timestamp("2026-07-15 08:00:00"),
                "sheet_id": "SHT-A01",
                "lot_id": "LOT-A1",
                "step_id": "11620",
                "tt_name": "TDSUM",
                "tt_qty": 99.0,
            }
        ]
    )


def _spec_df() -> pd.DataFrame:
    return pd.DataFrame(
        [{"prod_code": "M678", "step_id": "11620", "tt_name": "TDSUM", "usl": 5.0}]
    )


def _spy_writes(monkeypatch) -> list[tuple[str, ...]]:
    """拦截共享层 replace_workbook_sheets，记录每次写入的 sheet 名并继续真实写入。"""
    write_calls: list[tuple[str, ...]] = []
    real_replace = sheet_oos_decoration.replace_workbook_sheets

    def spy(path, sheets):
        write_calls.append(tuple(sheets.keys()))
        return real_replace(path, sheets)

    monkeypatch.setattr(sheet_oos_decoration, "replace_workbook_sheets", spy)
    return write_calls


def test_prepare_aoi_tt_decoration_scope_gates_workbook_writes(
    tmp_path: Path, monkeypatch
) -> None:
    write_calls = _spy_writes(monkeypatch)
    kwargs = dict(scope="aoi_tt", product_revision="R1", decision_signature="sig-1")

    # 首次：产品 sheet + 决策台账 + meta 一起写入
    first = prepare_aoi_tt_decoration(
        _tt_details_df(), _spec_df(), tmp_path, "M678", now=NOW, **kwargs
    )
    assert write_calls == [("M678", "M678__flags", REFRESH_META_SHEET_NAME)]
    assert first.decoration_df["flag"].tolist() == [True]

    # 未变化且 TTL 未到 → 不写文件，但仍返回 merge 结果
    second = prepare_aoi_tt_decoration(
        _tt_details_df(), _spec_df(), tmp_path, "M678",
        now=NOW + timedelta(hours=1), **kwargs
    )
    assert len(write_calls) == 1
    assert second.decoration_df["flag"].tolist() == [True]

    # revision 变化（距上次仅 1h，TTL 未到）→ 立即重写
    prepare_aoi_tt_decoration(
        _tt_details_df(), _spec_df(), tmp_path, "M678",
        scope="aoi_tt", product_revision="R2", decision_signature="sig-1",
        now=NOW + timedelta(hours=2),
    )
    assert len(write_calls) == 2
    assert write_calls[-1] == ("M678", REFRESH_META_SHEET_NAME)

    # 决策签名变化 → 重写
    prepare_aoi_tt_decoration(
        _tt_details_df(), _spec_df(), tmp_path, "M678",
        scope="aoi_tt", product_revision="R2", decision_signature="sig-2",
        now=NOW + timedelta(hours=2, minutes=30),
    )
    assert len(write_calls) == 3


def test_prepare_aoi_tt_decoration_without_scope_keeps_legacy_always_write(
    tmp_path: Path, monkeypatch
) -> None:
    """不传 scope 保持旧语义：总是持久化、不维护 __refresh_meta__。"""
    write_calls = _spy_writes(monkeypatch)

    prepare_aoi_tt_decoration(_tt_details_df(), _spec_df(), tmp_path, "M678")
    prepare_aoi_tt_decoration(_tt_details_df(), _spec_df(), tmp_path, "M678")

    assert write_calls == [("M678", "M678__flags"), ("M678",)]


def test_prepare_aoi_tt_decoration_does_not_inherit_legacy_product_sheet_flags(
    tmp_path: Path, monkeypatch
) -> None:
    """全局语义（2026-09-01 起，所有 scope）：不从旧产品 sheet 迁移 flag（__flags 只记人为决策）。

    旧产品 sheet 中即使存在 flag=False 的行也不继承——该键按默认 True 修饰，
    且首次写入生成的 __flags 为空台账。
    """
    _spy_writes(monkeypatch)
    legacy = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M678",
                "step_id": "11620",
                "tt_name": "TDSUM",
                "sheet_id": "SHT-A01",
                "flag": False,
            }
        ]
    )
    legacy.to_excel(
        tmp_path / "aoi_tt_sheet_oos_decoration.xlsx",
        sheet_name="M678",
        index=False,
        engine="openpyxl",
    )

    result = prepare_aoi_tt_decoration(
        _tt_details_df(),
        _spec_df(),
        tmp_path,
        "M678",
        scope="aoi_tt",
        product_revision="R1",
        decision_signature="sig-1",
        now=NOW,
    )

    # 旧表中的 False 不继承：该键按默认 True（自动截断）处理
    assert result.decoration_df["flag"].tolist() == [True]
    # __flags 为空台账（只记人为决策，不搬旧表噪声）
    decisions = sheet_oos_decoration.load_sheet_oos_decisions(
        tmp_path, "aoi_tt_sheet_oos_decoration.xlsx", "M678"
    )
    assert decisions.empty
