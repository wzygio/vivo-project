"""Atomic product-scoped density evaluation publication; no raw-data access."""
from pathlib import Path

import pandas as pd

from src.inline_domain.infrastructure.monitor.excel_alarm_store import read_cached_alarm_workbook
from src.inline_domain.infrastructure.shared.alert_decisions import decision_rows, normalize_decision_keys
from src.inline_domain.core.shared.sheet_oos_decoration import get_decision_sheet_name
from src.shared_kernel.core.decoration_flags import parse_decoration_flag
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets, replace_workbook_sheets


class TrendAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def read(self, product: str) -> tuple[pd.DataFrame, dict] | None:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            return None
        frames = read_cached_alarm_workbook(str(self.path.resolve()), stat.st_mtime_ns, stat.st_size)
        if product not in frames or "__meta__" not in frames:
            return None
        meta = frames["__meta__"]
        if "product" not in meta:
            raise ValueError("Missing trend product metadata")
        selected = meta.loc[meta["product"].astype(str).eq(product)]
        if len(selected) != 1:
            return None
        facts = frames[product].copy()
        if not facts.empty and get_decision_sheet_name(product) in frames:
            keys = trend_decision_keys(facts)
            actions = decision_rows(frames, product, keys, False).set_index(keys)["flag"]
            index = pd.MultiIndex.from_frame(normalize_decision_keys(facts, keys)[keys])
            facts["flag"] = actions.reindex(index).map(parse_decoration_flag).to_numpy()
        return facts, selected.iloc[0].to_dict()

    def save(self, product: str, evaluation: pd.DataFrame, metadata: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with publication_lock(self.path):
            frames = read_workbook_sheets(self.path, preserve_text=True) if self.path.exists() else {}
            meta = frames.get("__meta__", pd.DataFrame(columns=["product"]))
            meta = pd.concat([meta.loc[~meta["product"].astype(str).eq(product)], pd.DataFrame([metadata])], ignore_index=True)
            keys = trend_decision_keys(evaluation)
            old = decision_rows(frames, product, keys, False)
            frames[get_decision_sheet_name(product)] = old
            frames[product] = evaluation
            ledger = decision_rows(frames, product, keys, False)
            outcome = replace_workbook_sheets(self.path, {product: evaluation, "__meta__": meta,
                get_decision_sheet_name(product): ledger})
            if not outcome.written:
                raise OSError("Trend result publication failed")

    def signature(self) -> str:
        try:
            stat = self.path.stat()
            return f"{self.path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
        except FileNotFoundError:
            return f"{self.path.resolve()}:missing"


def trend_decision_keys(frame: pd.DataFrame) -> list[str]:
    return ["prod_code", "factory", "step_id", "rs_code" if "rs_code" in frame else "tt_name", "target_date"]
