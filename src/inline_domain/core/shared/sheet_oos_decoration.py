from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd

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


def apply_sheet_oos_decoration(
    raw_measurements_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    decoration_df: pd.DataFrame | None = None,
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
    spec_df = _normalize_key_columns(active_df[spec_cols].copy()).rename(
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
