"""两阶段决策签名 helper 测试（PRD §5.4）。

阶段 1：file_stat_signature = (mtime_ns, size)，每次运行廉价读取；
阶段 2：决策内容签名由 st.cache_data 缓存，键含 (workbook, sheet, mtime_ns, size)，
file_stat 未变不重读 ``__flags``（避免反复启动 Excel COM）。
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.shared import decision_signature as decision_signature_module
from src.inline_domain.core.shared.sheet_oos_decoration import (
    EMPTY_DECISION_SIGNATURE,
    get_decision_sheet_name,
)
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    SheetOosDecorationReadError,
)

PROD = "M678"


@pytest.fixture(autouse=True)
def _clear_signature_cache():
    decision_signature_module._cached_decision_signature.clear()
    yield
    decision_signature_module._cached_decision_signature.clear()


def _write_workbook_with_flags(workbook_path: Path, flag: object = "Delete") -> Path:
    """写一个仅含 <产品>__flags 决策 sheet 的工作簿。"""
    decisions_df = pd.DataFrame(
        [
            {
                "prod_code": PROD,
                "step_id": "100",
                "param_name": "THK",
                "sheet_id": "S1",
                "flag": flag,
            }
        ]
    )
    workbook_path.parent.mkdir(parents=True, exist_ok=True)
    decisions_df.to_excel(
        workbook_path,
        sheet_name=get_decision_sheet_name(PROD),
        index=False,
        engine="openpyxl",
    )
    return workbook_path


def _counting_load_spy(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """用计数 spy 替换 core 的决策读取，其余行为保持不变。"""
    calls = [0]
    original = decision_signature_module.load_sheet_oos_decisions

    def spy(*args, **kwargs):
        calls[0] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(decision_signature_module, "load_sheet_oos_decisions", spy)
    return calls


def test_missing_workbook_returns_deterministic_empty_signature(tmp_path: Path) -> None:
    signature = decision_signature_module.get_decision_signature(
        tmp_path / "spc_sheet_oos_decoration.xlsx", PROD
    )

    assert signature == EMPTY_DECISION_SIGNATURE


def test_file_stat_unchanged_does_not_reread_decisions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    workbook = _write_workbook_with_flags(tmp_path / "spc_sheet_oos_decoration.xlsx")
    calls = _counting_load_spy(monkeypatch)

    first = decision_signature_module.get_decision_signature(workbook, PROD)
    second = decision_signature_module.get_decision_signature(workbook, PROD)

    assert first == second
    assert first != EMPTY_DECISION_SIGNATURE
    assert calls[0] == 1


def test_file_stat_change_triggers_reread(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    workbook = _write_workbook_with_flags(tmp_path / "spc_sheet_oos_decoration.xlsx")
    calls = _counting_load_spy(monkeypatch)

    first = decision_signature_module.get_decision_signature(workbook, PROD)
    # 追加字节改变 (mtime_ns, size)，模拟用户编辑后的工作簿。
    with workbook.open("ab") as fh:
        fh.write(b"decision-changed")
    second = decision_signature_module.get_decision_signature(workbook, PROD)

    assert calls[0] == 2
    assert isinstance(second, str) and second
    assert first != EMPTY_DECISION_SIGNATURE


def test_unreadable_flags_sheet_raises_without_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """__flags 存在但读取失败必须上抛，不得降级为空签名（PRD §5.4 规则 9）。"""
    workbook = _write_workbook_with_flags(tmp_path / "spc_sheet_oos_decoration.xlsx")

    def fail_load(*_args, **_kwargs):
        raise SheetOosDecorationReadError("unreadable __flags")

    monkeypatch.setattr(
        decision_signature_module, "load_sheet_oos_decisions", fail_load
    )

    with pytest.raises(SheetOosDecorationReadError):
        decision_signature_module.get_decision_signature(workbook, PROD)


def test_scope_decision_signature_uses_scope_workbook(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """scope 便捷入口：spc/ctq 各自定位自己的工作簿；文件缺失 -> empty。"""
    monkeypatch.setattr(
        decision_signature_module.ConfigLoader,
        "get_domain_resource_dir",
        classmethod(lambda _cls, _domain: tmp_path / "resources" / "inline_domain"),
    )
    resources = tmp_path / "resources" / "inline_domain"
    workbook = _write_workbook_with_flags(resources / "spc_sheet_oos_decoration.xlsx")

    spc_signature = decision_signature_module.get_scope_decision_signature("spc", PROD)
    ctq_signature = decision_signature_module.get_scope_decision_signature("ctq", PROD)

    assert spc_signature != EMPTY_DECISION_SIGNATURE
    assert ctq_signature == EMPTY_DECISION_SIGNATURE
    # 显式 product_dir 覆盖（测试用），不依赖 ConfigLoader。
    direct = decision_signature_module.get_scope_decision_signature(
        "spc", PROD, product_dir=workbook.parent
    )
    assert direct == spc_signature
