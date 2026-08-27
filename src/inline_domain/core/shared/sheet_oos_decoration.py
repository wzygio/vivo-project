from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.shared_kernel.utils.excel_tools import (
    _read_encrypted_xlsx_via_com,
    read_workbook_sheet,
    replace_workbook_sheets,
)

logger = logging.getLogger(__name__)

OOS_DECORATION_FILE_NAME = "spc_sheet_oos_decoration.xlsx"
OOS_KEY_COLUMNS = ["prod_code", "step_id", "param_name", "sheet_id"]
OOS_DETAIL_COLUMNS = [
    "factory",
    "prod_code",
    "step_id",
    "param_name",
    "sheet_id",
    "sheet_start_time",
    "sheet_max",
    "sheet_min",
    "sheet_mean",
    "usl",
    "lsl",
    "oos_type",
]
OOS_DECORATION_COLUMNS = [*OOS_DETAIL_COLUMNS, "flag"]
DELETE_ACTION = "Delete"


class SheetOosDecorationReadError(RuntimeError):
    """Raised when an existing user-maintained decoration file cannot be read safely."""


class SheetOosDecorationWriteError(RuntimeError):
    """共享工作簿原子写失败（如文件被 Excel 占用）时抛出，不得静默成功。"""


# 决策 sheet：独立于产品明细 sheet 的用户决策台账（<产品sheet>__flags）
DECISION_FLAG_COLUMN = "flag"
# 刷新元数据 sheet：记录每个 scope + 产品的最近生成状态
REFRESH_META_SHEET_NAME = "__refresh_meta__"
REFRESH_META_COLUMNS = [
    "scope",
    "prod_code",
    "last_generated_at",
    "product_revision",
    "decision_signature",
    "detail_row_count",
]
# 明细重生成 TTL：超过该间隔即使内容未变也重写产品 sheet
DETAIL_REFRESH_TTL = timedelta(hours=4)
# 空决策台账的固定签名（保持确定性）
EMPTY_DECISION_SIGNATURE = "empty"


@dataclass(frozen=True)
class RefreshDecision:
    """明细重生成判定结果。reason 取值：missing / ttl_expired /
    product_revision_changed / decision_changed / unchanged。"""

    should_write: bool
    reason: str


@dataclass(frozen=True)
class SheetOosDecorationResult:
    raw_measurements_df: pd.DataFrame
    decoration_df: pd.DataFrame
    decoration_path: Path
    decoration_sheet: str
    decision_sheet: str = ""
    decision_df: pd.DataFrame | None = None
    refresh_reason: str = ""


def get_sheet_oos_decoration_path(product_dir: Path, file_name: str = OOS_DECORATION_FILE_NAME) -> Path:
    return product_dir / file_name


def _empty_detail_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OOS_DETAIL_COLUMNS)


def _empty_decoration_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OOS_DECORATION_COLUMNS)


def _ordered_existing_columns(df: pd.DataFrame, ordered_columns: Iterable[str]) -> pd.DataFrame:
    for column in ordered_columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df[list(ordered_columns)].copy()


def _resolve_key_columns(key_columns: Iterable[str] | None) -> list[str]:
    """Custom key columns let non-SPC modules (e.g. aoi) reuse this machinery."""
    return list(key_columns) if key_columns else OOS_KEY_COLUMNS


def _normalize_key_columns(
    df: pd.DataFrame, key_columns: Iterable[str] | None = None
) -> pd.DataFrame:
    result = df.copy()
    for column in _resolve_key_columns(key_columns):
        if column in result.columns:
            result[column] = result[column].fillna("").astype(str)
    return result


def _parse_flag(value: object) -> bool:
    if pd.isna(value):
        return True
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"false", "0", "no", "n", "否", "不修饰", "不截断"}:
        return False
    return True


def _is_delete_action(value: object) -> bool:
    return not pd.isna(value) and str(value).strip().lower() == DELETE_ACTION.lower()


def _normalize_flag_action(value: object) -> bool | str:
    return DELETE_ACTION if _is_delete_action(value) else _parse_flag(value)


def _stable_fraction(parts: Iterable[object]) -> float:
    seed = "|".join("" if pd.isna(part) else str(part) for part in parts)
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(0xFFFFFFFFFFFF)


def _clip_inside_spec(row: pd.Series, side: str) -> float:
    value = row.get("param_value")
    usl = row.get("_oos_usl")
    lsl = row.get("_oos_lsl")
    if pd.isna(value) or pd.isna(usl) or pd.isna(lsl) or float(usl) <= float(lsl):
        return value

    span = float(usl) - float(lsl)
    fraction = _stable_fraction(
        [
            row.get("prod_code"),
            row.get("step_id"),
            row.get("param_name"),
            row.get("sheet_id"),
            row.get("site_name"),
            row.get("unit_id"),
            value,
            side,
        ]
)
    margin = (0.05 + fraction * 0.1) * span
    if side == "upper":
        return float(usl) - margin
    return float(lsl) + margin


def _apply_clip_rules(
    spec_df: pd.DataFrame,
    clip_rules: Iterable[dict[str, object]] | None,
) -> pd.DataFrame:
    """Return effective clip bounds without changing the official spec columns upstream."""
    result = spec_df.copy()
    if not clip_rules or "param_name" not in result.columns:
        return result

    matched = pd.Series(False, index=result.index)
    param_names = result["param_name"].fillna("").astype(str)
    for rule in clip_rules:
        needle = str(rule.get("param_name_contains", "")).strip()
        if not needle:
            continue
        try:
            lower_offset = float(rule.get("lower_offset", 0.0))
            upper_offset = float(rule.get("upper_offset", 0.0))
        except (TypeError, ValueError):
            continue
        rule_mask = ~matched & param_names.str.contains(needle, case=False, regex=False)
        result.loc[rule_mask, "lsl"] = (
            pd.to_numeric(result.loc[rule_mask, "lsl"], errors="coerce") + lower_offset
        )
        result.loc[rule_mask, "usl"] = (
            pd.to_numeric(result.loc[rule_mask, "usl"], errors="coerce") + upper_offset
        )
        matched |= rule_mask
    return result


def build_sheet_oos_detail(sheet_features_df: pd.DataFrame) -> pd.DataFrame:
    """Return Sheet-level rows whose point max/min crosses USL/LSL."""
    required_cols = {"factory", *OOS_KEY_COLUMNS, "sheet_start_time", "sheet_max", "sheet_min", "sheet_mean", "usl", "lsl"}
    if sheet_features_df.empty or not required_cols.issubset(sheet_features_df.columns):
        return _empty_detail_frame()

    df = sheet_features_df.copy()
    for column in ["sheet_max", "sheet_min", "sheet_mean", "usl", "lsl"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    upper_mask = df["sheet_max"] > df["usl"]
    lower_mask = df["sheet_min"] < df["lsl"]
    oos_df = df[upper_mask | lower_mask].copy()
    if oos_df.empty:
        return _empty_detail_frame()

    oos_df["oos_type"] = "USL"
    oos_df.loc[lower_mask.loc[oos_df.index], "oos_type"] = "LSL"
    both_mask = upper_mask.loc[oos_df.index] & lower_mask.loc[oos_df.index]
    oos_df.loc[both_mask, "oos_type"] = "USL/LSL"
    oos_df = _normalize_key_columns(oos_df)
    return _ordered_existing_columns(oos_df, OOS_DETAIL_COLUMNS).sort_values(
        ["factory", "prod_code", "step_id", "param_name", "sheet_start_time", "sheet_id"],
        kind="stable",
    ).reset_index(drop=True)


def load_sheet_oos_decoration(
    product_dir: Path,
    file_name: str = OOS_DECORATION_FILE_NAME,
    sheet_name: str | None = None,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Load the user-editable decoration flags from the shared workbook sheet."""
    decoration_path = get_sheet_oos_decoration_path(product_dir, file_name)
    if not decoration_path.exists():
        return _empty_decoration_frame() if key_columns is None else pd.DataFrame()
    try:
        if sheet_name is None:
            df = pd.read_excel(decoration_path, engine="openpyxl")
        else:
            try:
                df = pd.read_excel(decoration_path, sheet_name=sheet_name)
            except ValueError:
                # 指定 sheet 缺失 —— 与文件缺失语义一致
                return _empty_decoration_frame() if key_columns is None else pd.DataFrame()
    except Exception as excel_exc:
        try:
            df = _read_encrypted_xlsx_via_com(decoration_path, sheet_name)
            logger.info(
                "[SPC] loaded enterprise-encrypted Sheet OOS decoration file via Excel COM: %s",
                decoration_path,
            )
        except Exception as com_exc:
            logger.error(
                "[CPM] failed to read Sheet OOS decoration file %s with openpyxl (%s) and Excel COM (%s)",
                decoration_path,
                excel_exc,
                com_exc,
            )
            raise SheetOosDecorationReadError(
                f"Unable to read existing Sheet OOS decoration file: {decoration_path}"
            ) from com_exc
    if df.empty:
        return _empty_decoration_frame() if key_columns is None else pd.DataFrame()
    df = _normalize_key_columns(df, key_columns)
    if key_columns is None:
        return _ordered_existing_columns(df, OOS_DECORATION_COLUMNS)
    return df


def merge_detail_with_decoration_flags(
    detail_df: pd.DataFrame,
    existing_decoration_df: pd.DataFrame,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Attach existing user flags to current OOS details, defaulting to True."""
    if key_columns is None:
        detail_columns: list[str] = OOS_DETAIL_COLUMNS
        decoration_columns: list[str] = OOS_DECORATION_COLUMNS
    else:
        detail_columns = list(detail_df.columns)
        decoration_columns = [*detail_columns, "flag"]
    if detail_df.empty:
        return pd.DataFrame(columns=decoration_columns)

    keys = _resolve_key_columns(key_columns)
    detail_df = _normalize_key_columns(
        _ordered_existing_columns(detail_df, detail_columns), keys
    )
    if existing_decoration_df.empty or "flag" not in existing_decoration_df.columns:
        result = detail_df.copy()
        result["flag"] = True
        return result[decoration_columns]

    flags_df = _normalize_key_columns(existing_decoration_df, keys).copy()
    flags_df["flag"] = flags_df["flag"].apply(_normalize_flag_action)
    flags_df = flags_df[keys + ["flag"]].drop_duplicates(keys, keep="last")
    result = detail_df.merge(flags_df, on=keys, how="left")
    result["flag"] = result["flag"].apply(_normalize_flag_action)
    return result[decoration_columns]


def _exclude_delete_flagged_measurements(
    raw_measurements_df: pd.DataFrame,
    decoration_df: pd.DataFrame,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    keys = _resolve_key_columns(key_columns)
    required_columns = set(keys)
    if not required_columns.issubset(raw_measurements_df.columns):
        return raw_measurements_df.copy()

    delete_keys = decoration_df.loc[
        decoration_df["flag"].apply(_is_delete_action),
        keys,
    ].drop_duplicates(keys)
    if delete_keys.empty:
        return raw_measurements_df.copy()

    delete_keys = _normalize_key_columns(delete_keys, keys).assign(_delete_action=True)
    result = _normalize_key_columns(raw_measurements_df, keys).merge(
        delete_keys,
        on=keys,
        how="left",
        validate="many_to_one",
    )
    return result.loc[result["_delete_action"].ne(True)].drop(
        columns="_delete_action"
    )


# ---------------------------------------------------------------------------
# 决策 sheet（<产品>__flags）与刷新 meta
# ---------------------------------------------------------------------------


def get_decision_sheet_name(sheet_name: str | None = None) -> str:
    """决策 sheet 名：产品 sheet 名加 __flags 后缀。"""
    return f"{sheet_name or 'Sheet1'}__flags"


def _empty_decisions_frame(key_columns: Iterable[str] | None = None) -> pd.DataFrame:
    return pd.DataFrame(columns=[*_resolve_key_columns(key_columns), DECISION_FLAG_COLUMN])


def _workbook_sheet_names(xlsx_path: Path) -> list[str] | None:
    """openpyxl 可打开时返回 sheet 名列表；企业加密等打不开时返回 None。"""
    if not xlsx_path.exists():
        return []
    try:
        import openpyxl

        wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    except Exception:
        return None
    try:
        return list(wb.sheetnames)
    finally:
        wb.close()


def load_sheet_oos_decisions(
    product_dir: Path,
    file_name: str = OOS_DECORATION_FILE_NAME,
    sheet_name: str | None = None,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """读取决策 sheet（<产品>__flags）中的用户决策台账。

    文件或决策 sheet 不存在（首次迁移前）返回空决策台账；
    决策 sheet 存在但读取失败必须抛 SheetOosDecorationReadError，
    不得降级为空——否则用户决策会被覆盖丢失。
    """
    keys = _resolve_key_columns(key_columns)
    decoration_path = get_sheet_oos_decoration_path(product_dir, file_name)
    if not decoration_path.exists():
        return _empty_decisions_frame(keys)
    decision_sheet = get_decision_sheet_name(sheet_name)
    names = _workbook_sheet_names(decoration_path)
    if names is not None and decision_sheet not in names:
        return _empty_decisions_frame(keys)
    try:
        df = read_workbook_sheet(decoration_path, decision_sheet)
    except Exception as exc:
        logger.error(
            "[SPC] 决策 sheet [%s] 读取失败: %s (%s)",
            decision_sheet,
            decoration_path,
            exc,
        )
        raise SheetOosDecorationReadError(
            f"Unable to read existing Sheet OOS decision sheet [{decision_sheet}]: {decoration_path}"
        ) from exc
    if df.empty:
        return _empty_decisions_frame(keys)
    df = _normalize_key_columns(df, keys)
    return _ordered_existing_columns(df, [*keys, DECISION_FLAG_COLUMN])


def compute_decision_signature(
    decisions_df: pd.DataFrame,
    key_columns: Iterable[str] | None = None,
) -> str:
    """决策台账内容签名：键列归一化 + flag 规范化后稳定排序的 SHA-256。

    行序变化不改变签名；任意 flag 变化必须改变签名；空表返回固定值。
    """
    keys = _resolve_key_columns(key_columns)
    if decisions_df is None or decisions_df.empty:
        return EMPTY_DECISION_SIGNATURE
    df = decisions_df.copy()
    for column in keys:
        if column not in df.columns:
            df[column] = ""
    df = _normalize_key_columns(df, keys)
    if DECISION_FLAG_COLUMN in df.columns:
        df[DECISION_FLAG_COLUMN] = df[DECISION_FLAG_COLUMN].apply(
            lambda value: str(_normalize_flag_action(value))
        )
    else:
        df[DECISION_FLAG_COLUMN] = "True"
    df = df.sort_values(keys, kind="stable")
    payload = "\n".join(
        "|".join(str(value) for value in row)
        for row in df[[*keys, DECISION_FLAG_COLUMN]].itertuples(index=False, name=None)
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def migrate_legacy_flags_if_needed(
    product_dir: Path,
    file_name: str = OOS_DECORATION_FILE_NAME,
    sheet_name: str | None = None,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """旧表迁移（幂等）：返回决策台账 df（键列 + flag）。

    - __flags 已存在 → 直接读取返回（不再迁移）；
    - 不存在但旧产品 sheet 存在 → 提取键列 + flag，重复键保留最后一行，
      全部 flag（含显式 True）都保留；
    - 产品 sheet 也不存在 → 空决策台账。
    """
    keys = _resolve_key_columns(key_columns)
    decoration_path = get_sheet_oos_decoration_path(product_dir, file_name)
    decision_sheet = get_decision_sheet_name(sheet_name)
    names = _workbook_sheet_names(decoration_path)
    if names is not None and decision_sheet in names:
        return load_sheet_oos_decisions(product_dir, file_name, sheet_name, keys)
    if names is not None and (sheet_name or "Sheet1") not in names:
        return _empty_decisions_frame(keys)
    # names 为 None（无法列举 sheet，如企业加密）或旧产品 sheet 存在：从旧表迁移
    legacy = load_sheet_oos_decoration(product_dir, file_name, sheet_name, key_columns)
    if legacy.empty:
        return _empty_decisions_frame(keys)
    legacy = _normalize_key_columns(legacy, keys)
    if DECISION_FLAG_COLUMN not in legacy.columns:
        legacy[DECISION_FLAG_COLUMN] = True
    return (
        legacy[[*keys, DECISION_FLAG_COLUMN]]
        .drop_duplicates(keys, keep="last")
        .reset_index(drop=True)
    )


def build_refresh_meta_row(
    scope: str,
    prod_code: str,
    generated_at: datetime,
    product_revision: str,
    decision_signature: str,
    detail_row_count: int,
) -> dict:
    """构造一行 __refresh_meta__ 记录（last_generated_at 序列化为 ISO 字符串）。"""
    return {
        "scope": scope,
        "prod_code": prod_code,
        "last_generated_at": generated_at.isoformat()
        if isinstance(generated_at, datetime)
        else generated_at,
        "product_revision": product_revision,
        "decision_signature": decision_signature,
        "detail_row_count": int(detail_row_count),
    }


def load_refresh_meta(
    product_dir: Path,
    file_name: str = OOS_DECORATION_FILE_NAME,
    scope: str | None = None,
    prod_code: str | None = None,
) -> dict | None:
    """读取 __refresh_meta__ 中匹配 (scope, prod_code) 的最新一行。

    sheet 或行不存在返回 None；last_generated_at 解析失败视为 None；
    meta 读取失败不阻断主流程（视为缺失 → 触发重写）。
    """
    decoration_path = get_sheet_oos_decoration_path(product_dir, file_name)
    if not decoration_path.exists():
        return None
    try:
        df = read_workbook_sheet(decoration_path, REFRESH_META_SHEET_NAME)
    except Exception as exc:
        logger.warning(
            "[SPC] 刷新 meta sheet 读取失败，按缺失处理: %s (%s)",
            decoration_path,
            exc,
        )
        return None
    if df.empty or not {"scope", "prod_code"}.issubset(df.columns):
        return None
    mask = (df["scope"].fillna("").astype(str) == str(scope)) & (
        df["prod_code"].fillna("").astype(str) == str(prod_code)
    )
    matched = df.loc[mask]
    if matched.empty:
        return None
    row = matched.iloc[-1]
    generated_at = pd.to_datetime(row.get("last_generated_at"), errors="coerce")

    def _text(column: str) -> str | None:
        value = row.get(column)
        return None if value is None or pd.isna(value) else str(value)

    return {
        "scope": str(row.get("scope")),
        "prod_code": str(row.get("prod_code")),
        "last_generated_at": None if pd.isna(generated_at) else generated_at.to_pydatetime(),
        "product_revision": _text("product_revision"),
        "decision_signature": _text("decision_signature"),
        "detail_row_count": row.get("detail_row_count"),
    }


def _upsert_refresh_meta_row(
    existing_meta_df: pd.DataFrame, meta_row: dict
) -> pd.DataFrame:
    """把 meta 行 upsert 进现有 meta sheet 数据（按 scope + prod_code 去旧）。"""
    if existing_meta_df is None or existing_meta_df.empty:
        return pd.DataFrame([meta_row], columns=REFRESH_META_COLUMNS)
    df = existing_meta_df.copy()
    for column in REFRESH_META_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
    mask = (df["scope"].fillna("").astype(str) == str(meta_row["scope"])) & (
        df["prod_code"].fillna("").astype(str) == str(meta_row["prod_code"])
    )
    df = df.loc[~mask]
    return pd.concat([df, pd.DataFrame([meta_row])], ignore_index=True)[REFRESH_META_COLUMNS]


def should_regenerate_detail(
    *,
    current_sheet_exists: bool,
    last_generated_at: datetime | None,
    stored_product_revision: str | None,
    current_product_revision: str,
    stored_decision_signature: str | None,
    current_decision_signature: str,
    now: datetime,
) -> RefreshDecision:
    """明细重生成判定（纯函数）。

    优先级：sheet/meta 缺失 → revision 变化 → 决策签名变化 → TTL 到期 → unchanged。
    revision/决策变化优先于 TTL，保证立即重写。
    """
    if not current_sheet_exists or last_generated_at is None:
        return RefreshDecision(should_write=True, reason="missing")
    if stored_product_revision is None or str(stored_product_revision) != str(
        current_product_revision
    ):
        return RefreshDecision(should_write=True, reason="product_revision_changed")
    if stored_decision_signature is None or str(stored_decision_signature) != str(
        current_decision_signature
    ):
        return RefreshDecision(should_write=True, reason="decision_changed")
    if now - last_generated_at >= DETAIL_REFRESH_TTL:
        return RefreshDecision(should_write=True, reason="ttl_expired")
    return RefreshDecision(should_write=False, reason="unchanged")


def _log_refresh_decision(
    *,
    scope: str | None,
    prod_code: str,
    decision_signature: str,
    product_revision: str,
    last_generated_at: datetime | None,
    refresh_reason: str,
    write_attempted: bool,
    write_succeeded: bool,
    detail_row_count: int,
) -> None:
    """PRD §8 结构化判定日志：不记录完整明细，决策签名只记前 12 位。"""
    logger.info(
        "[SPC] sheet OOS refresh decision: product=%s scope=%s "
        "decision_signature=%s product_revision=%s last_generated_at=%s "
        "refresh_reason=%s write_attempted=%s write_succeeded=%s detail_row_count=%s",
        prod_code,
        scope,
        (decision_signature or "")[:12],
        product_revision,
        last_generated_at.isoformat() if last_generated_at else None,
        refresh_reason,
        write_attempted,
        write_succeeded,
        detail_row_count,
    )


@dataclass(frozen=True)
class _SheetOosPersistOutcome:
    """persist 编排的内部结果：merge 后的明细、决策台账与刷新判定。"""

    decoration_df: pd.DataFrame
    decisions_df: pd.DataFrame
    refresh_decision: RefreshDecision


def _persist_sheet_oos_decoration(
    product_dir: Path,
    detail_df: pd.DataFrame,
    file_name: str,
    sheet_name: str | None,
    key_columns: Iterable[str] | None,
    *,
    scope: str | None,
    prod_code: str | None,
    product_revision: str | None,
    decision_signature: str | None,
    now: datetime | None,
    force: bool,
) -> _SheetOosPersistOutcome:
    """持久化编排：迁移/读取决策台账 → merge → 判定 → 需要时原子写多 sheet。"""
    product_dir.mkdir(parents=True, exist_ok=True)
    decoration_path = get_sheet_oos_decoration_path(product_dir, file_name)
    sheet = sheet_name or "Sheet1"
    decision_sheet = get_decision_sheet_name(sheet)
    keys = _resolve_key_columns(key_columns)

    sheet_names = _workbook_sheet_names(decoration_path)
    decision_sheet_exists = sheet_names is not None and decision_sheet in sheet_names
    current_sheet_exists = sheet_names is not None and sheet in sheet_names

    # 决策来源于 __flags（首次运行时从旧产品 sheet 迁移），历史键不进当前明细
    decisions = migrate_legacy_flags_if_needed(product_dir, file_name, sheet_name, keys)
    merged = merge_detail_with_decoration_flags(detail_df, decisions, keys)

    effective_now = now or datetime.now()
    effective_revision = "" if product_revision is None else str(product_revision)
    effective_signature = decision_signature or compute_decision_signature(decisions, keys)
    meta = (
        load_refresh_meta(product_dir, file_name, scope, prod_code or sheet)
        if scope is not None
        else None
    )
    decision = should_regenerate_detail(
        current_sheet_exists=current_sheet_exists,
        last_generated_at=meta["last_generated_at"] if meta else None,
        stored_product_revision=meta["product_revision"] if meta else None,
        current_product_revision=effective_revision,
        stored_decision_signature=meta["decision_signature"] if meta else None,
        current_decision_signature=effective_signature,
        now=effective_now,
    )
    if force or scope is None:
        # force 或旧语义（未传 scope）：总是允许持久化，reason 仍按真实判定给出便于观测
        decision = RefreshDecision(should_write=True, reason=decision.reason)

    if decision.should_write:
        sheets_to_write: dict[str, pd.DataFrame] = {sheet: merged}
        if not decision_sheet_exists:
            # 首次迁移：把决策台账物化到 __flags，之后归用户维护
            sheets_to_write[decision_sheet] = decisions
        if scope is not None:
            meta_row = build_refresh_meta_row(
                scope=scope,
                prod_code=prod_code or sheet,
                generated_at=effective_now,
                product_revision=effective_revision,
                decision_signature=effective_signature,
                detail_row_count=len(merged),
            )
            existing_meta = read_workbook_sheet(decoration_path, REFRESH_META_SHEET_NAME)
            sheets_to_write[REFRESH_META_SHEET_NAME] = _upsert_refresh_meta_row(
                existing_meta, meta_row
            )
        result = replace_workbook_sheets(decoration_path, sheets_to_write)
        if scope is not None:
            _log_refresh_decision(
                scope=scope,
                prod_code=prod_code or sheet,
                decision_signature=effective_signature,
                product_revision=effective_revision,
                last_generated_at=meta["last_generated_at"] if meta else None,
                refresh_reason=decision.reason,
                write_attempted=True,
                write_succeeded=result.written,
                detail_row_count=len(merged),
            )
        if not result.written:
            raise SheetOosDecorationWriteError(
                f"Failed to persist Sheet OOS decoration workbook {decoration_path}: {result.error}"
            )
    elif scope is not None:
        _log_refresh_decision(
            scope=scope,
            prod_code=prod_code or sheet,
            decision_signature=effective_signature,
            product_revision=effective_revision,
            last_generated_at=meta["last_generated_at"] if meta else None,
            refresh_reason=decision.reason,
            write_attempted=False,
            write_succeeded=False,
            detail_row_count=len(merged),
        )
    return _SheetOosPersistOutcome(
        decoration_df=merged,
        decisions_df=decisions,
        refresh_decision=decision,
    )


def persist_sheet_oos_decoration(
    product_dir: Path,
    detail_df: pd.DataFrame,
    file_name: str = OOS_DECORATION_FILE_NAME,
    sheet_name: str | None = None,
    key_columns: Iterable[str] | None = None,
    *,
    scope: str | None = None,
    prod_code: str | None = None,
    product_revision: str | None = None,
    decision_signature: str | None = None,
    now: datetime | None = None,
    force: bool = False,
) -> pd.DataFrame:
    """Refresh the user-maintained Sheet OOS decoration sheet in the shared workbook.

    决策来源于决策 sheet（<产品>__flags，首次运行时自动从旧产品 sheet 迁移）。
    传入 scope 后启用刷新判定（meta 缺失/revision 或决策签名变化/TTL 4h 到期才写）；
    不传 scope 时保持旧语义：总是持久化且不维护 __refresh_meta__。
    写入失败（written=False，如文件被 Excel 占用）抛 SheetOosDecorationWriteError。
    """
    return _persist_sheet_oos_decoration(
        product_dir,
        detail_df,
        file_name,
        sheet_name,
        key_columns,
        scope=scope,
        prod_code=prod_code,
        product_revision=product_revision,
        decision_signature=decision_signature,
        now=now,
        force=force,
    ).decoration_df


def apply_sheet_oos_decoration(
    raw_measurements_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    decoration_df: pd.DataFrame | None = None,
    clip_rules: Iterable[dict[str, object]] | None = None,
) -> pd.DataFrame:
    """Apply Sheet actions: Delete excludes points, True clips OOS points, False keeps them."""
    if raw_measurements_df.empty or "param_value" not in raw_measurements_df.columns:
        return raw_measurements_df

    detail_df = build_sheet_oos_detail(sheet_features_df)
    if detail_df.empty:
        return raw_measurements_df.copy()

    if decoration_df is None or decoration_df.empty:
        decoration_df = merge_detail_with_decoration_flags(detail_df, _empty_decoration_frame())
    else:
        decoration_df = merge_detail_with_decoration_flags(detail_df, decoration_df)

    df = _exclude_delete_flagged_measurements(raw_measurements_df, decoration_df)
    active_df = decoration_df[
        ~decoration_df["flag"].apply(_is_delete_action)
        & decoration_df["flag"].apply(_parse_flag)
    ].copy()
    if active_df.empty or df.empty:
        return df

    spec_cols = [*OOS_KEY_COLUMNS, "usl", "lsl"]
    spec_df = _apply_clip_rules(
        _normalize_key_columns(active_df[spec_cols].copy()),
        clip_rules,
    ).rename(
        columns={"usl": "_oos_usl", "lsl": "_oos_lsl"}
    )
    df = _normalize_key_columns(df)
    df["param_value"] = pd.to_numeric(df["param_value"], errors="coerce")
    df = df.merge(spec_df, on=OOS_KEY_COLUMNS, how="left")

    upper_mask = df["_oos_usl"].notna() & (df["param_value"] > df["_oos_usl"])
    lower_mask = df["_oos_lsl"].notna() & (df["param_value"] < df["_oos_lsl"])
    if upper_mask.any():
        df.loc[upper_mask, "param_value"] = df.loc[upper_mask].apply(_clip_inside_spec, axis=1, side="upper")
    if lower_mask.any():
        df.loc[lower_mask, "param_value"] = df.loc[lower_mask].apply(_clip_inside_spec, axis=1, side="lower")

    return df.drop(columns=["_oos_usl", "_oos_lsl"])


def prepare_sheet_oos_decoration(
    raw_measurements_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    product_dir: Path,
    persist_files: bool = True,
    clip_rules: Iterable[dict[str, object]] | None = None,
    decoration_file_name: str = OOS_DECORATION_FILE_NAME,
    decoration_sheet_name: str | None = None,
    *,
    scope: str | None = None,
    prod_code: str | None = None,
    product_revision: str | None = None,
    decision_signature: str | None = None,
    now: datetime | None = None,
    force: bool = False,
) -> SheetOosDecorationResult:
    """Return chart-ready measurements after applying tri-state Sheet actions.

    不传 scope 等新参数时保持旧语义（persist_files=True 总是持久化）；
    传入 scope 后启用刷新判定，不需要重写时仅返回 merge 结果、不写文件。
    """
    detail_df = build_sheet_oos_detail(sheet_features_df)
    sheet = decoration_sheet_name or "Sheet1"
    if persist_files:
        outcome = _persist_sheet_oos_decoration(
            product_dir,
            detail_df,
            decoration_file_name,
            decoration_sheet_name,
            None,
            scope=scope,
            prod_code=prod_code,
            product_revision=product_revision,
            decision_signature=decision_signature,
            now=now,
            force=force,
        )
        decoration_df = outcome.decoration_df
        decisions_df = outcome.decisions_df
        refresh_reason = outcome.refresh_decision.reason
    else:
        decisions_df = migrate_legacy_flags_if_needed(
            product_dir, decoration_file_name, decoration_sheet_name
        )
        decoration_df = merge_detail_with_decoration_flags(detail_df, decisions_df)
        refresh_reason = ""

    decorated_df = apply_sheet_oos_decoration(
        raw_measurements_df,
        sheet_features_df,
        decoration_df,
        clip_rules=clip_rules,
    )
    return SheetOosDecorationResult(
        raw_measurements_df=decorated_df,
        decoration_df=decoration_df,
        decoration_path=get_sheet_oos_decoration_path(product_dir, decoration_file_name),
        decoration_sheet=sheet,
        decision_sheet=get_decision_sheet_name(sheet),
        decision_df=decisions_df,
        refresh_reason=refresh_reason,
    )
