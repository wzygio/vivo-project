"""Sheet OOS 修饰刷新机制集成测试（Phase 7.1）。

端到端验证：旧格式迁移、决策变更同步、系统自写无二次触发、
revision 推进立即重建、写失败保留旧状态。
全部使用 tmp_path 真实工作簿，不触碰真实 resources/。
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.core.shared import sheet_oos_decoration
from src.inline_domain.core.shared.sheet_oos_decoration import (
    OOS_DECORATION_FILE_NAME,
    OOS_KEY_COLUMNS,
    REFRESH_META_COLUMNS,
    REFRESH_META_SHEET_NAME,
    SheetOosDecorationWriteError,
    compute_decision_signature,
    load_refresh_meta,
    load_sheet_oos_decisions,
    prepare_sheet_oos_decoration,
)
from src.shared_kernel.utils.excel_tools import (
    WorkbookWriteResult,
    read_workbook_sheet,
    replace_workbook_sheets,
)

NOW = datetime(2026, 8, 18, 12, 0, 0)
SHEET = "Z571"
SCOPE = "spc"


def _sheet_features() -> pd.DataFrame:
    """当前明细：S1（USL 超限）、S2（LSL 超限）。"""
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


def _raw_measurements() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "OLED", "prod_code": "Z571", "step_id": "21200",
                "param_name": "PPA_B_X", "sheet_id": "S1",
                "sheet_start_time": "2026-07-01 08:00:00",
                "site_name": "P1", "unit_id": "U1", "param_value": 7.5,
            },
            {
                "factory": "OLED", "prod_code": "Z571", "step_id": "21200",
                "param_name": "PPA_B_X", "sheet_id": "S2",
                "sheet_start_time": "2026-07-01 09:00:00",
                "site_name": "P1", "unit_id": "U1", "param_value": -7.5,
            },
        ]
    )


def _write_legacy_workbook(product_dir: Path) -> Path:
    """构造旧格式工作簿：单产品 sheet（明细列 + flag），含重复键与显式 True。

    - S1 flag=False（用户关闭修饰）
    - S2 重复键 [True, Delete] → 迁移后 keep=last 应为 Delete
    - S9 flag=True（历史键，显式 True 必须保留）
    """
    product_dir.mkdir(parents=True, exist_ok=True)
    workbook_path = product_dir / OOS_DECORATION_FILE_NAME
    legacy = pd.DataFrame(
        [
            {"prod_code": "Z571", "step_id": "21200", "param_name": "PPA_B_X",
             "sheet_id": "S1", "sheet_max": 7.5, "flag": False},
            {"prod_code": "Z571", "step_id": "21200", "param_name": "PPA_B_X",
             "sheet_id": "S2", "sheet_max": 2.0, "flag": True},
            {"prod_code": "Z571", "step_id": "21200", "param_name": "PPA_B_X",
             "sheet_id": "S2", "sheet_max": 2.0, "flag": "Delete"},
            {"prod_code": "Z571", "step_id": "21200", "param_name": "PPA_B_X",
             "sheet_id": "S9", "sheet_max": 9.9, "flag": True},
        ]
    )
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        legacy.to_excel(writer, index=False, sheet_name=SHEET)
    return workbook_path


def _run_prepare(product_dir: Path, *, revision: str = "R1", now: datetime = NOW):
    return prepare_sheet_oos_decoration(
        _raw_measurements(),
        _sheet_features(),
        product_dir,
        persist_files=True,
        decoration_sheet_name=SHEET,
        scope=SCOPE,
        product_revision=revision,
        now=now,
    )


def _read_product_sheet(workbook_path: Path) -> pd.DataFrame:
    return read_workbook_sheet(workbook_path, SHEET)


def _flags_by_sheet_id(df: pd.DataFrame) -> dict[str, object]:
    return dict(zip(df["sheet_id"].astype(str), df["flag"]))


# ---------------------------------------------------------------------------
# 场景 1：旧格式迁移端到端 + 二次运行幂等
# ---------------------------------------------------------------------------


def test_legacy_migration_end_to_end_and_idempotent_second_run(tmp_path: Path) -> None:
    product_dir = tmp_path / "resources"
    workbook_path = _write_legacy_workbook(product_dir)

    result = _run_prepare(product_dir, revision="R1", now=NOW)

    assert result.refresh_reason == "missing"
    assert result.decision_sheet == "Z571__flags"

    # __flags 决策台账：全部 flag 保留（含显式 True 的历史键 S9），重复键 keep=last
    decisions = load_sheet_oos_decisions(product_dir, sheet_name=SHEET)
    assert list(decisions.columns) == [*OOS_KEY_COLUMNS, "flag"]
    assert decisions["sheet_id"].tolist() == ["S1", "S2", "S9"]
    assert _flags_by_sheet_id(decisions) == {"S1": False, "S2": "Delete", "S9": True}

    # 产品 sheet 重建为当前明细（历史键 S9 不进入），flag 按决策台账生效
    product = _read_product_sheet(workbook_path)
    assert product["sheet_id"].tolist() == ["S1", "S2"]
    assert _flags_by_sheet_id(product) == {"S1": False, "S2": "Delete"}
    assert result.decoration_df["sheet_id"].tolist() == ["S1", "S2"]

    # 三态作用于 raw：S1 flag=False 保留真实值；S2 Delete 剔除点位
    decorated = result.raw_measurements_df
    assert decorated["sheet_id"].tolist() == ["S1"]
    assert decorated["param_value"].iloc[0] == 7.5

    # __refresh_meta__ 写入且字段齐全
    meta = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, SCOPE, SHEET)
    assert meta is not None
    assert meta["scope"] == SCOPE
    assert meta["prod_code"] == SHEET
    assert meta["last_generated_at"] == NOW
    assert meta["product_revision"] == "R1"
    assert meta["decision_signature"] == compute_decision_signature(decisions)
    assert int(meta["detail_row_count"]) == 2
    meta_sheet = read_workbook_sheet(workbook_path, REFRESH_META_SHEET_NAME)
    assert set(REFRESH_META_COLUMNS).issubset(meta_sheet.columns)

    # 二次运行幂等：TTL 内不重写，决策台账不变
    mtime_before = workbook_path.stat().st_mtime_ns
    second = _run_prepare(product_dir, revision="R1", now=NOW + timedelta(hours=1))
    assert second.refresh_reason == "unchanged"
    assert workbook_path.stat().st_mtime_ns == mtime_before
    decisions_after = load_sheet_oos_decisions(product_dir, sheet_name=SHEET)
    assert decisions_after["sheet_id"].tolist() == ["S1", "S2", "S9"]
    assert _flags_by_sheet_id(decisions_after) == {"S1": False, "S2": "Delete", "S9": True}


# ---------------------------------------------------------------------------
# 场景 2：决策变更同步
# ---------------------------------------------------------------------------


def test_decision_change_triggers_rewrite_and_updates_signature(tmp_path: Path) -> None:
    product_dir = tmp_path / "resources"
    workbook_path = _write_legacy_workbook(product_dir)
    _run_prepare(product_dir, revision="R1", now=NOW)

    # 用户直接编辑 __flags：S1 False→True（恢复修饰）、S2 Delete→False（恢复展示）
    decisions = load_sheet_oos_decisions(product_dir, sheet_name=SHEET)
    decisions.loc[decisions["sheet_id"] == "S1", "flag"] = True
    decisions.loc[decisions["sheet_id"] == "S2", "flag"] = False
    write_result = replace_workbook_sheets(workbook_path, {"Z571__flags": decisions})
    assert write_result.written, write_result.error
    expected_signature = compute_decision_signature(decisions)

    edited_at = NOW + timedelta(minutes=30)
    result = _run_prepare(product_dir, revision="R1", now=edited_at)

    # 判定为决策变化 → 重写产品 sheet（revision 未变、TTL 未到）
    assert result.refresh_reason == "decision_changed"

    # 返回 payload 与产品 sheet 按新决策生效
    assert _flags_by_sheet_id(result.decoration_df) == {"S1": True, "S2": False}
    product = _read_product_sheet(workbook_path)
    assert _flags_by_sheet_id(product) == {"S1": True, "S2": False}

    # 三态作用同步：S1 恢复修饰（截断到 spec 内）；S2 恢复展示（保留真实值）
    decorated = result.raw_measurements_df
    assert decorated["sheet_id"].tolist() == ["S1", "S2"]
    s1_value = decorated.loc[decorated["sheet_id"] == "S1", "param_value"].iloc[0]
    assert -6.0 < s1_value < 6.0
    assert decorated.loc[decorated["sheet_id"] == "S2", "param_value"].iloc[0] == -7.5

    # meta 的 decision_signature 与生成时间更新
    meta = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, SCOPE, SHEET)
    assert meta["decision_signature"] == expected_signature
    assert meta["last_generated_at"] == edited_at
    assert meta["product_revision"] == "R1"


# ---------------------------------------------------------------------------
# 场景 3：系统自写无二次触发
# ---------------------------------------------------------------------------


def test_self_written_workbook_does_not_retrigger_within_ttl(tmp_path: Path) -> None:
    product_dir = tmp_path / "resources"
    workbook_path = _write_legacy_workbook(product_dir)

    first = _run_prepare(product_dir, revision="R1", now=NOW)
    assert first.refresh_reason == "missing"
    mtime_before = workbook_path.stat().st_mtime_ns
    meta_before = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, SCOPE, SHEET)

    # 相同 revision/决策签名，时钟推进 1 小时（TTL 4h 内）
    second = _run_prepare(product_dir, revision="R1", now=NOW + timedelta(hours=1))

    assert second.refresh_reason == "unchanged"
    assert workbook_path.stat().st_mtime_ns == mtime_before
    meta_after = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, SCOPE, SHEET)
    assert meta_after == meta_before
    # 不写文件但 merge 结果仍可用
    assert second.decoration_df["sheet_id"].tolist() == ["S1", "S2"]


# ---------------------------------------------------------------------------
# 场景 4：revision 推进立即重建
# ---------------------------------------------------------------------------


def test_revision_change_rebuilds_immediately(tmp_path: Path, monkeypatch) -> None:
    product_dir = tmp_path / "resources"
    _write_legacy_workbook(product_dir)
    _run_prepare(product_dir, revision="R1", now=NOW)

    write_calls: list[tuple[str, ...]] = []
    real_replace = sheet_oos_decoration.replace_workbook_sheets

    def spy(path, sheets):
        write_calls.append(tuple(sheets.keys()))
        return real_replace(path, sheets)

    monkeypatch.setattr(sheet_oos_decoration, "replace_workbook_sheets", spy)

    # TTL 内仅推进 revision → 立即重写一次（产品 sheet + meta；__flags 不动）
    rebuilt_at = NOW + timedelta(hours=1)
    result = _run_prepare(product_dir, revision="R2", now=rebuilt_at)

    assert result.refresh_reason == "product_revision_changed"
    assert write_calls == [(SHEET, REFRESH_META_SHEET_NAME)]

    meta = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, SCOPE, SHEET)
    assert meta["product_revision"] == "R2"
    assert meta["last_generated_at"] == rebuilt_at

    # 用户决策在重建后仍保留
    product = _read_product_sheet(product_dir / OOS_DECORATION_FILE_NAME)
    assert _flags_by_sheet_id(product) == {"S1": False, "S2": "Delete"}


# ---------------------------------------------------------------------------
# 场景 5：写失败保留旧状态
# ---------------------------------------------------------------------------


def test_write_failure_raises_and_preserves_old_state(tmp_path: Path, monkeypatch) -> None:
    product_dir = tmp_path / "resources"
    workbook_path = _write_legacy_workbook(product_dir)
    _run_prepare(product_dir, revision="R1", now=NOW)

    bytes_before = workbook_path.read_bytes()
    meta_before = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, SCOPE, SHEET)

    def fail_write(path, sheets):
        return WorkbookWriteResult(
            written=False, path=Path(path), updated_sheets=(),
            error="文件被占用，请关闭 Excel 后重试",
        )

    monkeypatch.setattr(sheet_oos_decoration, "replace_workbook_sheets", fail_write)

    # revision 推进触发写入，写入失败必须上抛业务异常
    with pytest.raises(SheetOosDecorationWriteError):
        _run_prepare(product_dir, revision="R2", now=NOW + timedelta(hours=1))

    # 旧文件与旧 meta 不变
    assert workbook_path.read_bytes() == bytes_before
    meta_after = load_refresh_meta(product_dir, OOS_DECORATION_FILE_NAME, SCOPE, SHEET)
    assert meta_after == meta_before
    assert meta_after["product_revision"] == "R1"
