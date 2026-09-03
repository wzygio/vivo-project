"""Sheet OOS 修饰管理区的纯逻辑层（PRD §5.9）：下载构建、上传校验与决策 sheet 写入。

本模块不依赖 streamlit，便于单测；UI 薄壳在
``app.sections.inline_domain.shared.decoration_admin.render_sheet_oos_decoration_admin``。

写入契约：上传只覆盖 ``<产品sheet>__flags`` 决策 sheet（完整决策集覆盖语义），
绝不触碰产品当前明细 sheet；``__refresh_meta__`` 由系统维护，不进下载、不由上传更新。
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Literal

import pandas as pd

from src.inline_domain.core.shared.sheet_oos_decoration import (
    DECISION_FLAG_COLUMN,
    OOS_DECORATION_COLUMNS,
    OOS_KEY_COLUMNS,
    _normalize_flag_action,
    _normalize_key_columns,
    compute_decision_signature,
    get_decision_sheet_name,
)
from src.inline_domain.application.shared.sheet_oos_decoration_service import (
    SheetOosDecorationResult,
)
from src.shared_kernel.utils.excel_tools import replace_workbook_sheets

DETAIL_DOWNLOAD_SHEET = "当前明细"
DECISION_DOWNLOAD_SHEET = "决策台账"
# 决策台账列：四列键 + flag
DECISION_DOWNLOAD_COLUMNS = [*OOS_KEY_COLUMNS, DECISION_FLAG_COLUMN]

# flag 合法取值：布尔、0/1、既有解析兼容文本与 Delete
_TRUE_TOKENS = {"true", "1", "yes", "y", "是", "修饰", "截断"}
_FALSE_TOKENS = {"false", "0", "no", "n", "否", "不修饰", "不截断"}
_DELETE_TOKENS = {"delete"}
_VALID_FLAG_TOKENS = _TRUE_TOKENS | _FALSE_TOKENS | _DELETE_TOKENS


@dataclass(frozen=True)
class DecisionUploadOutcome:
    """上传处理结果：status ∈ success / unchanged / error，message 面向用户。"""

    status: Literal["success", "unchanged", "error"]
    message: str


def _is_valid_flag(value: object) -> bool:
    """flag 值合法性：布尔、0/1 数值或既有解析兼容文本（含 Delete）。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return value in (0, 1)
    return str(value).strip().lower() in _VALID_FLAG_TOKENS


def build_decision_download_sheets(
    decoration_result: SheetOosDecorationResult,
) -> dict[str, pd.DataFrame]:
    """构建下载工作簿的两个 sheet：当前明细 + 决策台账（不含 __refresh_meta__）。

    空数据时保留列头：明细用 OOS_DECORATION_COLUMNS，台账用键列 + flag。
    """
    decoration_df = decoration_result.decoration_df
    detail_df = (
        decoration_df
        if decoration_df is not None and not decoration_df.empty
        else pd.DataFrame(columns=OOS_DECORATION_COLUMNS)
    )
    decisions_df = decoration_result.decision_df
    if decisions_df is None or decisions_df.empty:
        ledger_df = pd.DataFrame(columns=DECISION_DOWNLOAD_COLUMNS)
    else:
        ledger_df = decisions_df.copy()
        for column in DECISION_DOWNLOAD_COLUMNS:
            if column not in ledger_df.columns:
                ledger_df[column] = pd.NA
        ledger_df = ledger_df[DECISION_DOWNLOAD_COLUMNS]
    return {DETAIL_DOWNLOAD_SHEET: detail_df, DECISION_DOWNLOAD_SHEET: ledger_df}


def validate_decision_upload(
    uploaded_df: pd.DataFrame | None,
) -> tuple[bool, str | None, pd.DataFrame | None]:
    """校验并规范化上传的决策台账。

    - 键列（prod_code/step_id/param_name/sheet_id）与 flag 必须齐全；
    - flag 仅允许 True/False/Delete 及其既有解析兼容形式；
    - 重复键显式拒绝（keep=last 只是旧表迁移逻辑的行为，管理员上传必须报错）。

    返回 (ok, error_msg, normalized_df)；normalized_df 仅含键列 + flag，
    flag 已规范化为 True/False/"Delete"。空表（仅列头）合法，表示清空显式决策。
    """
    if uploaded_df is None:
        return False, "上传内容为空。", None
    missing = [c for c in DECISION_DOWNLOAD_COLUMNS if c not in uploaded_df.columns]
    if missing:
        return False, f"决策台账缺少必要字段：{', '.join(missing)}", None

    df = _normalize_key_columns(uploaded_df[DECISION_DOWNLOAD_COLUMNS].copy(), OOS_KEY_COLUMNS)
    invalid_mask = ~df[DECISION_FLAG_COLUMN].apply(_is_valid_flag)
    if invalid_mask.any():
        bad_values = df.loc[invalid_mask, DECISION_FLAG_COLUMN].head(3).tolist()
        return (
            False,
            f"flag 存在非法取值（仅支持 True/False/Delete）：{bad_values}",
            None,
        )
    duplicated_mask = df.duplicated(OOS_KEY_COLUMNS, keep=False)
    if duplicated_mask.any():
        sample = df.loc[duplicated_mask, OOS_KEY_COLUMNS].head(1).iloc[0].to_dict()
        return False, f"决策台账存在重复键，请去重后重试：{sample}", None

    df[DECISION_FLAG_COLUMN] = df[DECISION_FLAG_COLUMN].apply(_normalize_flag_action)
    return True, None, df.reset_index(drop=True)


def parse_decision_upload(
    file_bytes: bytes,
) -> tuple[bool, str | None, pd.DataFrame | None]:
    """解析上传的 Excel：优先“决策台账”sheet；兼容旧单 sheet 文件（取第一个 sheet）。"""
    try:
        sheets = pd.read_excel(BytesIO(file_bytes), sheet_name=None)
    except Exception as exc:
        return False, f"无法读取上传的 Excel 文件：{exc}", None
    if not sheets:
        return False, "上传的 Excel 文件不包含任何 sheet。", None
    if DECISION_DOWNLOAD_SHEET in sheets:
        df = sheets[DECISION_DOWNLOAD_SHEET]
    else:
        # 兼容旧单 sheet 修饰表：取第一个 sheet，键列/flag 缺失由校验报错
        df = next(iter(sheets.values()))
    return validate_decision_upload(df)


def apply_decision_upload(
    decoration_result: SheetOosDecorationResult,
    normalized_df: pd.DataFrame,
) -> DecisionUploadOutcome:
    """把校验后的决策集完整覆盖写入 ``<产品sheet>__flags``（不触碰产品明细 sheet）。

    与现有决策规范化后签名一致 → unchanged，不重写文件；
    写入失败（如文件被 Excel 占用）→ error，携带底层可操作错误信息。
    """
    decision_sheet = decoration_result.decision_sheet or get_decision_sheet_name(
        decoration_result.decoration_sheet
    )
    if compute_decision_signature(normalized_df) == compute_decision_signature(
        decoration_result.decision_df
    ):
        return DecisionUploadOutcome("unchanged", "内容一致，无需更新。")

    write_result = replace_workbook_sheets(
        decoration_result.decoration_path, {decision_sheet: normalized_df}
    )
    if not write_result.written:
        return DecisionUploadOutcome("error", f"保存决策台账失败：{write_result.error}")
    if normalized_df.empty:
        return DecisionUploadOutcome("success", "已清空该产品的显式决策（空 __flags 已写入）。")
    return DecisionUploadOutcome("success", "决策台账已更新，正在刷新。")


def handle_decision_upload(
    decoration_result: SheetOosDecorationResult,
    file_bytes: bytes,
) -> DecisionUploadOutcome:
    """上传处理入口：解析 → 校验 → 写入 __flags。"""
    ok, error, normalized_df = parse_decision_upload(file_bytes)
    if not ok:
        return DecisionUploadOutcome("error", error or "上传校验失败。")
    return apply_decision_upload(decoration_result, normalized_df)
