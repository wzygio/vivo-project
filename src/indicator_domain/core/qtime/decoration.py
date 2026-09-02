"""Pure Q-Time over-spec detection and manual decoration rules."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pandas as pd


QTIME_DECORATION_FILE_NAME = "qtime_oos_decoration.xlsx"
QTIME_KEY_COLUMNS = ["prodcode", "step_desc", "lot_id", "timekey"]
QTIME_DETAIL_COLUMNS = [
    "shop",
    "prodcode",
    "f_step",
    "t_step",
    "step_desc",
    "lot_id",
    "timekey",
    "q_spec",
    "wait_time",
    "over_hours",
]
QTIME_DECORATION_COLUMNS = [*QTIME_DETAIL_COLUMNS, "flag"]
DELETE_ACTION = "Delete"
DECISION_SHEET_NAME = "决策台账"
DECISION_COLUMNS = [*QTIME_KEY_COLUMNS, "flag"]


@dataclass(frozen=True)
class QTimeDecorationResult:
    details: pd.DataFrame
    decoration: pd.DataFrame


def build_qtime_oos_detail(details: pd.DataFrame) -> pd.DataFrame:
    """Return rows whose waiting time exceeds their positive Q-Time limit."""
    required = set(QTIME_DETAIL_COLUMNS) - {"over_hours"}
    if details.empty or not required.issubset(details.columns):
        return pd.DataFrame(columns=QTIME_DETAIL_COLUMNS)

    source_columns = [column for column in QTIME_DETAIL_COLUMNS if column != "over_hours"]
    result = details.loc[:, source_columns].copy()
    result["q_spec"] = pd.to_numeric(result["q_spec"], errors="coerce")
    result["wait_time"] = pd.to_numeric(result["wait_time"], errors="coerce")
    result = result.loc[
        result["q_spec"].gt(0) & result["wait_time"].gt(result["q_spec"])
    ].copy()
    result["over_hours"] = (result["wait_time"] - result["q_spec"]).round(6)
    return result.reindex(columns=QTIME_DETAIL_COLUMNS).reset_index(drop=True)


def _normalize_keys(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for column in QTIME_KEY_COLUMNS:
        if column in result.columns:
            result[column] = result[column].fillna("").astype(str).str.strip()
    return result


def normalize_qtime_flag(value: object) -> bool | str:
    if not pd.isna(value) and str(value).strip().lower() == "delete":
        return DELETE_ACTION
    if pd.isna(value):
        return True
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {
        "false",
        "0",
        "no",
        "n",
        "否",
        "不修饰",
        "不截断",
    }


def _merge_decisions(
    oos_detail: pd.DataFrame,
    decisions: pd.DataFrame | None,
) -> pd.DataFrame:
    if oos_detail.empty:
        return pd.DataFrame(columns=QTIME_DECORATION_COLUMNS)
    result = _normalize_keys(oos_detail)
    if decisions is None or decisions.empty or "flag" not in decisions.columns:
        result["flag"] = True
        return result[QTIME_DECORATION_COLUMNS]

    normalized = _normalize_keys(decisions)
    if not set(QTIME_KEY_COLUMNS).issubset(normalized.columns):
        result["flag"] = True
        return result[QTIME_DECORATION_COLUMNS]
    normalized = normalized[QTIME_KEY_COLUMNS + ["flag"]].copy()
    normalized["flag"] = normalized["flag"].map(normalize_qtime_flag)
    normalized = normalized.drop_duplicates(QTIME_KEY_COLUMNS, keep="last")
    result = result.merge(normalized, on=QTIME_KEY_COLUMNS, how="left")
    result["flag"] = result["flag"].map(normalize_qtime_flag)
    return result[QTIME_DECORATION_COLUMNS]


def _decorated_wait_time(row: pd.Series) -> float:
    seed = "|".join(str(row[column]) for column in QTIME_KEY_COLUMNS)
    fraction = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12], 16)
    ratio = 0.85 + (fraction / float(0xFFFFFFFFFFFF)) * 0.1
    return round(float(row["q_spec"]) * ratio, 6)


def apply_qtime_decoration(
    details: pd.DataFrame,
    decisions: pd.DataFrame | None,
) -> QTimeDecorationResult:
    """Apply True=decorate, False=preserve, and Delete=remove semantics."""
    oos_detail = build_qtime_oos_detail(details)
    decoration = _merge_decisions(oos_detail, decisions)
    if details.empty or decoration.empty:
        return QTimeDecorationResult(details=details.copy(), decoration=decoration)

    actions = decoration[QTIME_KEY_COLUMNS + ["flag"]].rename(
        columns={"flag": "_qtime_flag"}
    )
    result = _normalize_keys(details).merge(
        actions,
        on=QTIME_KEY_COLUMNS,
        how="left",
        validate="many_to_one",
    )
    result = result.loc[result["_qtime_flag"].ne(DELETE_ACTION)].copy()
    decorate_mask = result["_qtime_flag"].eq(True)
    if decorate_mask.any():
        result.loc[decorate_mask, "wait_time"] = result.loc[decorate_mask].apply(
            _decorated_wait_time,
            axis=1,
        )
    return QTimeDecorationResult(
        details=result.drop(columns="_qtime_flag").reset_index(drop=True),
        decoration=decoration,
    )
