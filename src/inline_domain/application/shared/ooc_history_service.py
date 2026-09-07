"""Shared OOC history use cases for the automatic-warning dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.inline_domain.application.shared.oos_history_service import (
    COMMON_COLUMNS,
    OosHistoryService,
)
from src.inline_domain.core.shared.sheet_ooc_decoration import (
    SCOPE_OOC_DECORATION_FILE_NAME,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    get_decision_sheet_name,
    merge_detail_with_decoration_flags,
)


@dataclass(frozen=True)
class OocDecisionAdminView:
    detail_df: pd.DataFrame
    decision_df: pd.DataFrame
    key_columns: tuple[str, ...]
    workbook_path: Path
    product_sheet: str
    decision_sheet: str


class OocHistoryService(OosHistoryService):
    """OOC variant of the shared fact-history/decision-ledger service."""

    def _file_name(self, scope: str) -> str:
        return SCOPE_OOC_DECORATION_FILE_NAME[scope]

    def read_product(self, scope: str, prod_code: str):
        return self._read_product_with_file(scope, prod_code, self._file_name(scope))

    def source_signature(self, products: list[str], scopes: list[str]) -> str:
        return self._source_signature_with_files(
            products, scopes, SCOPE_OOC_DECORATION_FILE_NAME
        )

    def build_decision_admin_view(
        self, scope: str, prod_code: str
    ) -> OocDecisionAdminView:
        """Expose raw OOC facts and their mutable decision ledger to admin UI."""
        contract = self._store.contract_for(scope)
        file_name = self._file_name(scope)
        snapshot = self._store.read(scope, prod_code)
        facts = (
            snapshot.frame
            if snapshot is not None
            else self._decisions.load_fallback(file_name, prod_code, contract.key_columns)
        )
        decisions = self._decisions.load_decisions(
            file_name, prod_code, contract.key_columns
        )
        detail = merge_detail_with_decoration_flags(
            facts, decisions, contract.key_columns
        )
        decision_columns = [*contract.key_columns, "flag"]
        editable_decisions = detail.reindex(columns=decision_columns).copy()
        return OocDecisionAdminView(
            detail_df=detail,
            decision_df=editable_decisions,
            key_columns=tuple(contract.key_columns),
            workbook_path=self._decisions.source_path(file_name),
            product_sheet=prod_code,
            decision_sheet=get_decision_sheet_name(prod_code),
        )

    @staticmethod
    def _project(scope: str, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=[*COMMON_COLUMNS, "alarm_type"])
        projected = frame.copy()
        projected["scope"] = scope
        projected["alarm_type"] = "OOC"
        if scope in {"spc", "ctq"}:
            projected["metric_name"] = projected["param_name"]
            projected["item_id"] = projected["sheet_id"]
            projected["event_time"] = projected["sheet_start_time"]
            lower = projected["ooc_type"].astype(str).eq("LCL")
            projected["observed_value"] = projected["sheet_mean"]
            projected["usl"] = projected["ucl"]
            projected["lsl"] = projected["lcl"]
            projected["oos_type"] = projected["ooc_type"]
        elif scope == "aoi_tt":
            projected["metric_name"] = projected["tt_name"]
            projected["item_id"] = projected["sheet_id"]
            projected["event_time"] = projected["start_time"]
            projected["observed_value"] = projected["tt_qty"]
            projected["usl"] = projected["ucl"]
            projected["lsl"] = pd.NA
            projected["oos_type"] = projected["ooc_type"]
        elif scope == "aoi_rs":
            projected["metric_name"] = projected.get("rs_code", pd.Series(dtype="object"))
            projected["item_id"] = projected.get("point_id", pd.Series(dtype="object"))
            projected["event_time"] = projected.get("sheet_start_time", pd.Series(dtype="datetime64[ns]"))
            projected["observed_value"] = projected.get("value", pd.Series(dtype="float64"))
            projected["usl"] = projected.get("ucl", pd.Series(dtype="float64"))
            projected["lsl"] = projected.get("lcl", pd.Series(dtype="float64"))
            projected["oos_type"] = projected.get("ooc_type", pd.Series(dtype="object"))
        projected["event_time"] = pd.to_datetime(projected["event_time"], errors="coerce")
        projected["upper_limit"] = projected["usl"]
        projected["lower_limit"] = projected["lsl"]
        projected["limit_type"] = projected["oos_type"]
        columns = [*COMMON_COLUMNS, "alarm_type"]
        for column in columns:
            if column not in projected.columns:
                projected[column] = pd.NA
        return projected[columns].copy()


__all__ = ["OocDecisionAdminView", "OocHistoryService"]
