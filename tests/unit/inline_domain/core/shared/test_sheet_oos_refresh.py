"""Sheet OOS 修饰 core 层改造测试（PRD §5.1/§5.2/§5.3/§5.5）。

覆盖：生成判定纯函数、决策 sheet（<产品>__flags）读写与签名、
__refresh_meta__ 读写、持久化编排与兼容语义。
旧产品 sheet 的 flag 永不生效（不做旧表迁移）；首写生成空 __flags。
全部使用 tmp_path 临时工作簿，不触碰真实 resources/。
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.shared.sheet_oos_decoration_service import (
    prepare_sheet_oos_decoration,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    OOS_DECORATION_FILE_NAME,
    OOS_KEY_COLUMNS,
    REFRESH_META_SHEET_NAME,
    RefreshDecision,
    build_refresh_meta_row,
    build_sheet_oos_detail,
    compute_decision_signature,
    merge_detail_with_decoration_flags,
    should_regenerate_detail,
)
from src.inline_domain.infrastructure.shared import (
    sheet_oos_decoration_repository as sheet_oos_decoration,
)
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    SheetOosDecorationReadError,
    SheetOosDecorationWriteError,
    load_refresh_meta,
    load_sheet_oos_decisions,
    persist_sheet_oos_decoration,
)
from src.shared_kernel.utils.excel_tools import (
    WorkbookWriteResult,
    read_workbook_sheet,
    replace_workbook_sheets,
)

NOW = datetime(2026, 8, 18, 12, 0, 0)


def _sheet_features() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "sheet_id": "S1",
                "sheet_start_time": "2026-07-01 08:00:00",
                "sheet_max": 7.5,
                "sheet_min": -1.0,
                "sheet_mean": 0.5,
                "usl": 6.0,
                "lsl": -6.0,
            },
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "sheet_id": "S2",
                "sheet_start_time": "2026-07-01 09:00:00",
                "sheet_max": 2.0,
                "sheet_min": -7.5,
                "sheet_mean": -0.5,
                "usl": 6.0,
                "lsl": -6.0,
            },
        ]
    )


def _decisions_ledger() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"prod_code": "Z571", "step_id": "21200", "param_name": "PPA_B_X", "sheet_id": "S1", "flag": True},
            {"prod_code": "Z571", "step_id": "21200", "param_name": "PPA_B_X", "sheet_id": "S2", "flag": False},
        ]
    )


def _make_product_dir(tmp_path: Path) -> Path:
    product_dir = tmp_path / "resources"
    product_dir.mkdir(parents=True, exist_ok=True)
    return product_dir


def _write_workbook(product_dir: Path, sheets: dict[str, pd.DataFrame]) -> None:
    result = replace_workbook_sheets(product_dir / OOS_DECORATION_FILE_NAME, sheets)
    assert result.written, result.error


def _spy_writes(monkeypatch) -> list[tuple[str, ...]]:
    """拦截模块内的 replace_workbook_sheets，记录每次写入的 sheet 名并继续真实写入。"""
    write_calls: list[tuple[str, ...]] = []
    real_replace = sheet_oos_decoration.replace_workbook_sheets

    def spy(path, sheets):
        write_calls.append(tuple(sheets.keys()))
        return real_replace(path, sheets)

    monkeypatch.setattr(sheet_oos_decoration, "replace_workbook_sheets", spy)
    return write_calls


# ---------------------------------------------------------------------------
# 1) should_regenerate_detail 判定全分支
# ---------------------------------------------------------------------------


def test_should_regenerate_detail_missing_when_sheet_absent() -> None:
    decision = should_regenerate_detail(
        current_sheet_exists=False,
        last_generated_at=NOW - timedelta(hours=1),
        stored_product_revision="R1",
        current_product_revision="R1",
        stored_decision_signature="sig",
        current_decision_signature="sig",
        now=NOW,
    )
    assert decision == RefreshDecision(should_write=True, reason="missing")


def test_should_regenerate_detail_missing_when_meta_absent() -> None:
    decision = should_regenerate_detail(
        current_sheet_exists=True,
        last_generated_at=None,
        stored_product_revision=None,
        current_product_revision="R1",
        stored_decision_signature=None,
        current_decision_signature="sig",
        now=NOW,
    )
    assert decision == RefreshDecision(should_write=True, reason="missing")


def test_should_regenerate_detail_revision_change_beats_ttl() -> None:
    # 距上次生成仅 1h（TTL 未到），但 revision 变化 → 立即重写
    decision = should_regenerate_detail(
        current_sheet_exists=True,
        last_generated_at=NOW - timedelta(hours=1),
        stored_product_revision="R1",
        current_product_revision="R2",
        stored_decision_signature="sig",
        current_decision_signature="sig",
        now=NOW,
    )
    assert decision == RefreshDecision(should_write=True, reason="product_revision_changed")


def test_should_regenerate_detail_decision_change_beats_ttl() -> None:
    decision = should_regenerate_detail(
        current_sheet_exists=True,
        last_generated_at=NOW - timedelta(hours=1),
        stored_product_revision="R1",
        current_product_revision="R1",
        stored_decision_signature="sig-old",
        current_decision_signature="sig-new",
        now=NOW,
    )
    assert decision == RefreshDecision(should_write=True, reason="decision_changed")


def test_should_regenerate_detail_ttl_boundary() -> None:
    base_kwargs = dict(
        current_sheet_exists=True,
        stored_product_revision="R1",
        current_product_revision="R1",
        stored_decision_signature="sig",
        current_decision_signature="sig",
        now=NOW,
    )
    # 3h59m → 不写
    decision = should_regenerate_detail(
        last_generated_at=NOW - timedelta(hours=3, minutes=59), **base_kwargs
    )
    assert decision == RefreshDecision(should_write=False, reason="unchanged")
    # 恰好 4h00m → 写
    decision = should_regenerate_detail(
        last_generated_at=NOW - timedelta(hours=4), **base_kwargs
    )
    assert decision == RefreshDecision(should_write=True, reason="ttl_expired")


def test_should_regenerate_detail_unchanged() -> None:
    decision = should_regenerate_detail(
        current_sheet_exists=True,
        last_generated_at=NOW - timedelta(hours=1),
        stored_product_revision="R1",
        current_product_revision="R1",
        stored_decision_signature="sig",
        current_decision_signature="sig",
        now=NOW,
    )
    assert decision == RefreshDecision(should_write=False, reason="unchanged")


# ---------------------------------------------------------------------------
# 2) compute_decision_signature
# ---------------------------------------------------------------------------


def test_compute_decision_signature_row_order_invariant() -> None:
    ledger = _decisions_ledger()
    shuffled = ledger.iloc[::-1].reset_index(drop=True)
    assert compute_decision_signature(ledger) == compute_decision_signature(shuffled)


def test_compute_decision_signature_changes_with_flag() -> None:
    ledger = _decisions_ledger()
    changed = ledger.copy()
    changed.loc[0, "flag"] = False
    assert compute_decision_signature(ledger) != compute_decision_signature(changed)


def test_compute_decision_signature_empty_is_deterministic() -> None:
    empty_a = pd.DataFrame()
    empty_b = pd.DataFrame(columns=[*OOS_KEY_COLUMNS, "flag"])
    assert compute_decision_signature(empty_a) == compute_decision_signature(empty_b)


# ---------------------------------------------------------------------------
# 3) load_sheet_oos_decisions
# ---------------------------------------------------------------------------


def test_load_sheet_oos_decisions_returns_empty_when_missing(tmp_path: Path) -> None:
    product_dir = _make_product_dir(tmp_path)
    # 文件不存在
    decisions = load_sheet_oos_decisions(product_dir, sheet_name="Z571")
    assert decisions.empty
    assert list(decisions.columns) == [*OOS_KEY_COLUMNS, "flag"]

    # 文件存在但 __flags sheet 不存在（尚未首次写入）
    _write_workbook(product_dir, {"Z571": pd.DataFrame({"a": [1]})})
    decisions = load_sheet_oos_decisions(product_dir, sheet_name="Z571")
    assert decisions.empty
    assert list(decisions.columns) == [*OOS_KEY_COLUMNS, "flag"]


def test_load_sheet_oos_decisions_reads_flags_sheet(tmp_path: Path) -> None:
    product_dir = _make_product_dir(tmp_path)
    _write_workbook(
        product_dir,
        {"Z571": pd.DataFrame({"a": [1]}), "Z571__flags": _decisions_ledger()},
    )

    decisions = load_sheet_oos_decisions(product_dir, sheet_name="Z571")

    assert list(decisions.columns) == [*OOS_KEY_COLUMNS, "flag"]
    assert decisions["sheet_id"].tolist() == ["S1", "S2"]
    assert decisions["flag"].tolist() == [True, False]


def test_load_sheet_oos_decisions_raises_when_existing_sheet_unreadable(
    tmp_path: Path, monkeypatch
) -> None:
    """决策 sheet 存在但读取失败必须抛错，不得降级为空导致用户决策被覆盖。"""
    product_dir = _make_product_dir(tmp_path)
    _write_workbook(product_dir, {"Z571__flags": _decisions_ledger()})

    def _boom(path, sheet_name):
        raise RuntimeError("COM unavailable")

    monkeypatch.setattr(sheet_oos_decoration, "read_workbook_sheet", _boom)

    with pytest.raises(SheetOosDecorationReadError):
        load_sheet_oos_decisions(product_dir, sheet_name="Z571")


# ---------------------------------------------------------------------------
# 4) merge 输入为决策台账：历史键不进明细、同键重现恢复 flag
# ---------------------------------------------------------------------------


def test_merge_with_decision_ledger_drops_history_and_restores_flag() -> None:
    detail = build_sheet_oos_detail(_sheet_features())  # S1, S2
    ledger = detail.assign(flag=[False, True])[OOS_KEY_COLUMNS + ["flag"]]
    # 历史键 S9 只存在于台账，不得进入当前明细
    history = pd.DataFrame(
        [{"prod_code": "Z571", "step_id": "21200", "param_name": "PPA_B_X", "sheet_id": "S9", "flag": False}]
    )
    ledger = pd.concat([ledger, history], ignore_index=True)

    merged = merge_detail_with_decoration_flags(detail, ledger)

    assert merged["sheet_id"].tolist() == ["S1", "S2"]
    assert merged["flag"].tolist() == [False, True]

    # 同一键消失后重现 → 仍从台账恢复 flag
    merged_again = merge_detail_with_decoration_flags(detail, ledger)
    assert merged_again["flag"].tolist() == [False, True]


# ---------------------------------------------------------------------------
# 5) __refresh_meta__ 读写
# ---------------------------------------------------------------------------


def test_refresh_meta_round_trip(tmp_path: Path) -> None:
    product_dir = _make_product_dir(tmp_path)
    row = build_refresh_meta_row(
        scope="spc",
        prod_code="Z571",
        generated_at=NOW,
        product_revision="R1",
        decision_signature="sig-1",
        detail_row_count=2,
    )
    _write_workbook(product_dir, {REFRESH_META_SHEET_NAME: pd.DataFrame([row])})

    meta = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, "spc", "Z571")

    assert meta is not None
    assert meta["scope"] == "spc"
    assert meta["prod_code"] == "Z571"
    assert meta["last_generated_at"] == NOW
    assert meta["product_revision"] == "R1"
    assert meta["decision_signature"] == "sig-1"
    assert int(meta["detail_row_count"]) == 2


def test_load_refresh_meta_returns_none_when_missing(tmp_path: Path) -> None:
    product_dir = _make_product_dir(tmp_path)
    # 文件不存在
    assert load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, "spc", "Z571") is None
    # meta sheet 存在但无匹配行
    row = build_refresh_meta_row("spc", "M678", NOW, "R1", "sig-1", 2)
    _write_workbook(product_dir, {REFRESH_META_SHEET_NAME: pd.DataFrame([row])})
    assert load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, "spc", "Z571") is None


def test_load_refresh_meta_bad_timestamp_yields_none(tmp_path: Path) -> None:
    product_dir = _make_product_dir(tmp_path)
    row = build_refresh_meta_row("spc", "Z571", NOW, "R1", "sig-1", 2)
    row["last_generated_at"] = "not-a-timestamp"
    _write_workbook(product_dir, {REFRESH_META_SHEET_NAME: pd.DataFrame([row])})

    meta = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, "spc", "Z571")

    assert meta is not None
    assert meta["last_generated_at"] is None


def test_persist_meta_upsert_preserves_other_scopes(tmp_path: Path) -> None:
    product_dir = _make_product_dir(tmp_path)
    detail = build_sheet_oos_detail(_sheet_features())

    persist_sheet_oos_decoration(
        product_dir, detail, sheet_name="Z571",
        scope="spc", product_revision="R1", decision_signature="sig-1", now=NOW,
    )
    persist_sheet_oos_decoration(
        product_dir, detail, sheet_name="Z571",
        scope="aoi", product_revision="R1", decision_signature="sig-9", now=NOW,
    )

    spc_meta = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, "spc", "Z571")
    aoi_meta = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, "aoi", "Z571")
    assert spc_meta["decision_signature"] == "sig-1"
    assert aoi_meta["decision_signature"] == "sig-9"


# ---------------------------------------------------------------------------
# 6) 持久化编排：判定驱动写入、写失败抛错、旧语义兼容
# ---------------------------------------------------------------------------


def test_persist_with_scope_skips_write_when_unchanged(tmp_path: Path, monkeypatch) -> None:
    product_dir = _make_product_dir(tmp_path)
    detail = build_sheet_oos_detail(_sheet_features())
    write_calls = _spy_writes(monkeypatch)

    # 首次：产品 sheet + 决策台账 + meta 一起写入
    persist_sheet_oos_decoration(
        product_dir, detail, sheet_name="Z571",
        scope="spc", product_revision="R1", decision_signature="sig-1", now=NOW,
    )
    assert write_calls == [("Z571", "Z571__flags", REFRESH_META_SHEET_NAME)]

    # 未变化且 TTL 未到 → 不写文件，但仍返回 merge 结果
    merged = persist_sheet_oos_decoration(
        product_dir, detail, sheet_name="Z571",
        scope="spc", product_revision="R1", decision_signature="sig-1",
        now=NOW + timedelta(hours=1),
    )
    assert len(write_calls) == 1
    assert merged["flag"].tolist() == [True, True]

    # TTL 到期 → 重写产品 sheet + meta；__flags 已存在不重写
    persist_sheet_oos_decoration(
        product_dir, detail, sheet_name="Z571",
        scope="spc", product_revision="R1", decision_signature="sig-1",
        now=NOW + timedelta(hours=4),
    )
    assert write_calls[-1] == ("Z571", REFRESH_META_SHEET_NAME)

    # revision 变化（距上次仅 1h，TTL 未到）→ 立即重写
    persist_sheet_oos_decoration(
        product_dir, detail, sheet_name="Z571",
        scope="spc", product_revision="R2", decision_signature="sig-1",
        now=NOW + timedelta(hours=5),
    )
    assert len(write_calls) == 3

    # 决策签名变化 → 重写
    persist_sheet_oos_decoration(
        product_dir, detail, sheet_name="Z571",
        scope="spc", product_revision="R2", decision_signature="sig-2",
        now=NOW + timedelta(hours=5, minutes=30),
    )
    assert len(write_calls) == 4

    # force → 即使 unchanged 也重写
    persist_sheet_oos_decoration(
        product_dir, detail, sheet_name="Z571",
        scope="spc", product_revision="R2", decision_signature="sig-2",
        now=NOW + timedelta(hours=5, minutes=40), force=True,
    )
    assert len(write_calls) == 5


def test_persist_raises_when_atomic_write_fails(tmp_path: Path, monkeypatch) -> None:
    """written=False（如文件被 Excel 占用）必须上抛业务异常，不得静默成功。"""
    monkeypatch.setattr(
        sheet_oos_decoration,
        "replace_workbook_sheets",
        lambda path, sheets: WorkbookWriteResult(
            written=False, path=Path(path), updated_sheets=(), error="文件被占用，请关闭 Excel 后重试"
        ),
    )

    with pytest.raises(SheetOosDecorationWriteError):
        persist_sheet_oos_decoration(
            tmp_path, build_sheet_oos_detail(_sheet_features()), sheet_name="Z571"
        )

    assert not (tmp_path / OOS_DECORATION_FILE_NAME).exists()


def test_persist_legacy_mode_always_writes_and_skips_meta(tmp_path: Path, monkeypatch) -> None:
    """旧调用（不传 scope 等新参数）保持“总是持久化”语义，且不维护 meta sheet。"""
    product_dir = _make_product_dir(tmp_path)
    detail = build_sheet_oos_detail(_sheet_features())
    write_calls = _spy_writes(monkeypatch)

    persist_sheet_oos_decoration(product_dir, detail, sheet_name="Z571")
    persist_sheet_oos_decoration(product_dir, detail, sheet_name="Z571")

    assert write_calls == [("Z571", "Z571__flags"), ("Z571",)]
    # 旧模式不写 __refresh_meta__
    assert read_workbook_sheet(
        product_dir / OOS_DECORATION_FILE_NAME, REFRESH_META_SHEET_NAME
    ).empty


def test_persist_reads_decisions_from_flags_sheet_not_product_sheet(tmp_path: Path) -> None:
    """决策只来源于 __flags；旧产品 sheet 的 flag 永远不再生效（不做迁移）。"""
    product_dir = _make_product_dir(tmp_path)
    detail = build_sheet_oos_detail(_sheet_features())

    merged_first = persist_sheet_oos_decoration(product_dir, detail, sheet_name="Z571")

    # 用户在决策 sheet 中把 S1 改为 False（基于首次生成的键行编辑）
    decisions = merged_first[[*OOS_KEY_COLUMNS, "flag"]].copy()
    decisions.loc[decisions["sheet_id"] == "S1", "flag"] = False
    _write_workbook(product_dir, {"Z571__flags": decisions})

    merged = persist_sheet_oos_decoration(product_dir, detail, sheet_name="Z571")

    assert bool(merged.loc[merged["sheet_id"] == "S1", "flag"].iloc[0]) is False
    assert bool(merged.loc[merged["sheet_id"] == "S2", "flag"].iloc[0]) is True


# ---------------------------------------------------------------------------
# 7) prepare_sheet_oos_decoration 兼容性
# ---------------------------------------------------------------------------


def test_prepare_sheet_oos_decoration_legacy_default_still_persists(tmp_path: Path) -> None:
    raw = pd.DataFrame(
        [
            {
                "factory": "OLED", "prod_code": "Z571", "step_id": "21200",
                "param_name": "PPA_B_X", "sheet_id": "S1",
                "sheet_start_time": "2026-07-01 08:00:00",
                "site_name": "P1", "unit_id": "U1", "param_value": 7.5,
            }
        ]
    )

    result = prepare_sheet_oos_decoration(raw, _sheet_features(), tmp_path, persist_files=True)

    # 旧语义：默认总是持久化
    assert (tmp_path / OOS_DECORATION_FILE_NAME).exists()
    assert result.decoration_df["flag"].tolist() == [True, True]
    # 新增字段向后兼容（带默认值）
    assert result.decision_sheet == "Sheet1__flags"
    assert result.decision_df is not None
    assert result.refresh_reason == "missing"


def test_prepare_sheet_oos_decoration_with_scope_skips_unchanged(tmp_path: Path, monkeypatch) -> None:
    write_calls = _spy_writes(monkeypatch)
    kwargs = dict(scope="spc", product_revision="R1", decision_signature="sig-1")

    first = prepare_sheet_oos_decoration(
        pd.DataFrame(), _sheet_features(), tmp_path, persist_files=True, now=NOW, **kwargs
    )
    assert first.refresh_reason == "missing"
    assert len(write_calls) == 1

    second = prepare_sheet_oos_decoration(
        pd.DataFrame(), _sheet_features(), tmp_path, persist_files=True,
        now=NOW + timedelta(hours=1), **kwargs
    )
    assert second.refresh_reason == "unchanged"
    assert len(write_calls) == 1  # 未再写文件
    assert second.decoration_df["flag"].tolist() == [True, True]
