"""Validation and workbook payload helpers for Q-Time decoration decisions."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Literal

import pandas as pd
from src.shared_kernel.core.decoration_revision import VERSION_SHEET, decision_revision, upload_revision

from src.indicator_domain.core.qtime.decoration import (
    DECISION_COLUMNS,
    QTIME_DECORATION_COLUMNS,
    QTIME_KEY_COLUMNS,
    normalize_qtime_flag,
)

DECISION_SHEET_NAME = "决策台账"
DETAIL_SHEET_NAME = "当前超规明细"
VALID_FLAG_TOKENS = {
    "true",
    "1",
    "yes",
    "y",
    "是",
    "修饰",
    "截断",
    "false",
    "0",
    "no",
    "n",
    "否",
    "不修饰",
    "不截断",
    "delete",
}


@dataclass(frozen=True)
class QTimeDecisionUploadResult:
    status: Literal["success", "error"]
    message: str
    decisions: pd.DataFrame | None = None
    revision: str | None = None


def _valid_flag(value: object) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return value in (0, 1)
    return str(value).strip().lower() in VALID_FLAG_TOKENS


def parse_qtime_decision_upload(file_bytes: bytes) -> QTimeDecisionUploadResult:
    try:
        sheets = pd.read_excel(BytesIO(file_bytes), sheet_name=None, dtype=object, keep_default_na=False)
    except Exception as exc:
        return QTimeDecisionUploadResult("error", f"无法读取上传的 Excel：{exc}")
    if not sheets:
        return QTimeDecisionUploadResult("error", "上传文件不包含任何 sheet。")
    try:
        revision = upload_revision(sheets.get(VERSION_SHEET))
    except ValueError as exc:
        return QTimeDecisionUploadResult("error", str(exc))
    if revision is not None and DECISION_SHEET_NAME not in sheets:
        return QTimeDecisionUploadResult("error", "上传文件缺少决策台账工作表。")
    ledger = sheets.get(DECISION_SHEET_NAME, next(iter(sheets.values())))
    missing = [column for column in DECISION_COLUMNS if column not in ledger.columns]
    if missing:
        return QTimeDecisionUploadResult(
            "error",
            f"决策台账缺少必要字段：{', '.join(missing)}",
        )

    result = ledger[DECISION_COLUMNS].copy()
    for column in QTIME_KEY_COLUMNS:
        result[column] = result[column].fillna("").astype(str).str.strip()
        if result[column].eq("").any():
            return QTimeDecisionUploadResult("error", "决策台账的业务键不能为空。")
    if (~result["flag"].map(_valid_flag)).any():
        return QTimeDecisionUploadResult(
            "error",
            "flag 仅支持 True、False 或 Delete。",
        )
    if result.duplicated(QTIME_KEY_COLUMNS, keep=False).any():
        return QTimeDecisionUploadResult("error", "决策台账存在重复键，请去重后重试。")
    result["flag"] = result["flag"].map(normalize_qtime_flag)
    return QTimeDecisionUploadResult(
        "success",
        "决策台账校验通过。",
        result.reset_index(drop=True),
        revision,
    )


def build_qtime_decoration_workbook(
    decoration: pd.DataFrame,
    decisions: pd.DataFrame,
) -> bytes:
    detail = decoration.reindex(columns=QTIME_DECORATION_COLUMNS)
    current_decisions = decoration.reindex(columns=DECISION_COLUMNS)
    stored_decisions = decisions.reindex(columns=DECISION_COLUMNS)
    ledger = pd.concat(
        [current_decisions, stored_decisions],
        ignore_index=True,
    ).drop_duplicates(QTIME_KEY_COLUMNS, keep="last")
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter", engine_kwargs={
        "options": {"strings_to_formulas": False, "strings_to_urls": False},
    }) as writer:
        detail.to_excel(writer, index=False, sheet_name=DETAIL_SHEET_NAME)
        ledger.to_excel(writer, index=False, sheet_name=DECISION_SHEET_NAME)
        revision = decision_revision("qtime", stored_decisions, QTIME_KEY_COLUMNS)
        pd.DataFrame({"revision": [revision]}).to_excel(writer, sheet_name=VERSION_SHEET, index=False)
        writer.sheets[VERSION_SHEET].hide()
    return output.getvalue()
