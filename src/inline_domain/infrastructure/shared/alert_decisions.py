"""Product-scoped flag maintenance shared by Inline alarm workbooks."""
from pathlib import Path

import pandas as pd

from src.inline_domain.core.shared.sheet_oos_decoration import (
    _normalize_flag_action, _normalize_key_columns, get_decision_sheet_name,
)
from src.shared_kernel.core.decoration_flags import parse_decoration_flag
from src.shared_kernel.core.decoration_revision import VERSION_SHEET, decision_revision, validate_decision_update
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets, replace_workbook_sheets


def normalize_decision_keys(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    result = _normalize_key_columns(frame, keys)
    if "target_date" in keys:
        result["target_date"] = result["target_date"].map(lambda value: pd.Timestamp(value).date().isoformat())
    return result


def decision_rows(frames: dict, product: str, keys: list[str], default: bool) -> pd.DataFrame:
    columns = [*keys, "flag"]
    ledger = frames.get(get_decision_sheet_name(product), pd.DataFrame(columns=columns))
    detail = frames.get(product, pd.DataFrame(columns=columns)).copy()
    if "Flag" in detail and "flag" not in detail:
        detail = detail.rename(columns={"Flag": "flag"})
    if not detail.empty and set(keys) - set(detail):
        raise ValueError("预警明细缺少业务键")
    if "flag" not in detail:
        detail["flag"] = default
    ledger = normalize_decision_keys(ledger, keys)
    if ledger.duplicated(keys).any():
        raise ValueError("台账中有重复业务键")
    detail = normalize_decision_keys(detail.reindex(columns=columns), keys).drop_duplicates(keys, keep="last")
    combined = pd.concat([ledger.loc[:, columns], detail], ignore_index=True)
    combined = normalize_decision_keys(combined, keys)
    return combined.drop_duplicates(keys, keep="first").reset_index(drop=True)


class AlertDecisionWorkbook:
    def __init__(self, path: Path, keys: list[str], *, default: bool):
        self.path, self.keys, self.default = path, keys, default

    def download(self, product: str) -> dict[str, pd.DataFrame]:
        frames = read_workbook_sheets(self.path, preserve_text=True)
        ledger = decision_rows(frames, product, self.keys, self.default)
        revision = decision_revision(f"{self.path.name}/{product}", ledger, self.keys)
        return {"决策台账": ledger, "当前明细": frames.get(product, pd.DataFrame()),
                VERSION_SHEET: pd.DataFrame({"revision": [revision]})}

    def update(self, product: str, uploaded: pd.DataFrame, *, revision: str | None = None) -> bool:
        keys = self.keys
        if not uploaded.columns.is_unique or set(uploaded) != {*keys, "flag"}:
            raise ValueError("请保留下载表的业务键与 flag 列")
        changes = normalize_decision_keys(uploaded, keys)
        if changes.duplicated(keys).any() or not changes["prod_code"].eq(product).all():
            raise ValueError("产品不匹配或业务键重复")
        changes["flag"] = changes["flag"].map(parse_decoration_flag)
        with publication_lock(self.path):
            frames = read_workbook_sheets(self.path, preserve_text=True)
            ledger = decision_rows(frames, product, keys, self.default)
            validate_decision_update(f"{self.path.name}/{product}", ledger, changes, keys, revision)
            ledger = ledger.set_index(keys)
            updates = changes.set_index(keys)["flag"]
            if not updates.index.isin(ledger.index).all():
                raise ValueError("包含台账中不存在的业务键")
            if updates.empty or ledger["flag"].reindex(updates.index).map(_normalize_flag_action).eq(updates).all():
                return False
            ledger["flag"] = ledger["flag"].astype(object)
            ledger.loc[updates.index, "flag"] = updates.to_numpy()
            sheets = {get_decision_sheet_name(product): ledger.reset_index()}
            detail = frames.get(product)
            if detail is not None and "flag" in detail and not detail.empty:
                current_keys = pd.MultiIndex.from_frame(normalize_decision_keys(detail, keys)[keys])
                sheets[product] = detail.assign(flag=ledger["flag"].reindex(current_keys).to_numpy())
            outcome = replace_workbook_sheets(self.path, sheets)
            if not outcome.written:
                raise OSError(outcome.error or "预警决策写入失败")
            return True
