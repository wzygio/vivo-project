"""Detect concurrent changes to the decision ledger behind an exported workbook."""
from collections.abc import Sequence
import hashlib
import json
import re

import pandas as pd

VERSION_SHEET = "决策版本"


def decision_revision(scope: str, ledger: pd.DataFrame, keys: Sequence[str], *,
                      controls: Sequence[str] = ("flag",)) -> str:
    columns = [*keys, *controls]
    rows = sorted(tuple("" if pd.isna(value) else str(value) for value in row)
                  for row in ledger[columns].itertuples(index=False, name=None))
    payload = json.dumps([scope, columns, rows], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def upload_revision(sheet: pd.DataFrame | None) -> str | None:
    if sheet is None or (sheet.empty and not len(sheet.columns)):
        return None
    if list(sheet) != ["revision"] or len(sheet) != 1:
        raise ValueError("决策版本无效，请重新下载最新台账。")
    value = str(sheet.iloc[0, 0])
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("决策版本无效，请重新下载最新台账。")
    return value


def validate_decision_update(scope: str, ledger: pd.DataFrame, edits: pd.DataFrame,
                             keys: Sequence[str], revision: str | None, *,
                             controls: Sequence[str] = ("flag",)) -> None:
    if revision is not None:
        if revision != decision_revision(scope, ledger, keys, controls=controls):
            raise ValueError("决策台账已更新，请重新下载并修改。")
    elif len(edits) > 1:
        matched = edits.merge(ledger, on=list(keys), suffixes=("_edit", "_current"))
        for column in controls:
            requested, current = matched[f"{column}_edit"], matched[f"{column}_current"]
            unchanged = requested.eq(current).fillna(False) | (requested.isna() & current.isna())
            if not unchanged.all():
                raise ValueError("旧版台账批量修改缺少版本，请重新下载最新台账。")
