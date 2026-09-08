"""Consume authoritative product-sheet decisions, never original measurements."""

from __future__ import annotations

import pandas as pd
from typing import Protocol

from src.inline_domain.application.shared.ooc_history_service import OocHistoryService
from src.inline_domain.application.shared.oos_history_service import OosHistoryService, OosProductRead
from src.inline_domain.core.monitor.excel_contract import ExcelAlarmReadError


class ExcelAlarmStorePort(Protocol):
    """Read-only generated-workbook operations required by the application."""

    def read(self, scope: str, product: str) -> dict | None: ...

    def source_signature(self, products: list[str], scopes: list[str]) -> str: ...


class ExcelAlarmReader:
    def __init__(self, store: ExcelAlarmStorePort, alarm_type: str = "oos") -> None:
        self._store = store
        self.alarm_type = alarm_type.upper()
        if self.alarm_type not in {"OOS", "OOC"}:
            raise ValueError(f"Unsupported alarm type: {alarm_type}")
        self._projector = OosHistoryService if self.alarm_type == "OOS" else OocHistoryService

    def has_history(self, scope: str, prod_code: str) -> bool:
        # Route legacy matrix consumers through this reader even for missing input;
        # they must not fall back to raw-data analysis.
        return True

    def source_signature(self, products: list[str], scopes: list[str]) -> str:
        return self.alarm_type + ":" + self._store.source_signature(products, scopes)

    def read_product(self, scope: str, prod_code: str) -> OosProductRead:
        payload = self._store.read(scope, prod_code)
        if payload is None:
            empty = self._projector.project_detail(scope, pd.DataFrame())
            return OosProductRead(empty, empty.copy(), "missing", None)
        frame = payload["frame"].copy()
        frame.columns = [str(column).strip() for column in frame.columns]
        if "Flag" in frame.columns and "flag" not in frame.columns:
            frame = frame.rename(columns={"Flag": "flag"})
        self._validate(scope, prod_code, frame)
        projected = self._projector.project_detail(scope, frame)
        if scope == "aoi_rs" and "chart_kind" in frame:
            projected["chart_kind"] = frame["chart_kind"].to_numpy()
        mask = projected["flag"].map(self._projector._is_false_flag)
        alerts = projected.loc[mask.astype(bool)].reset_index(drop=True)
        return OosProductRead(
            projected, alerts, "excel", self._refresh_time(payload["refresh_meta"], scope, prod_code)
        )

    def _validate(self, scope: str, product: str, frame: pd.DataFrame) -> None:
        if scope not in {"spc", "ctq", "aoi_tt", "aoi_rs"}:
            raise ValueError(f"Unsupported monitor scope: {scope}")
        if frame.empty:
            return
        required = {"factory", "prod_code", "step_id", "flag"}
        if scope in {"spc", "ctq"}:
            required |= {"param_name", "sheet_id", "sheet_start_time"}
            required |= {"sheet_max", "sheet_min", "usl", "lsl", "oos_type"} if self.alarm_type == "OOS" else {"sheet_mean", "ucl", "lcl", "ooc_type"}
        elif scope == "aoi_tt":
            required |= {"tt_name", "sheet_id", "start_time", "tt_qty"}
            required |= {"usl"} if self.alarm_type == "OOS" else {"ucl", "ooc_type"}
        else:
            required |= {"rs_code", "point_id", "sheet_start_time", "value"}
            required |= {"spec"} if self.alarm_type == "OOS" else {"ucl", "lcl", "ooc_type"}
        missing = required - set(frame.columns)
        if missing:
            raise ExcelAlarmReadError(f"{scope}/{product} {self.alarm_type}: missing columns {sorted(missing)}")
        if not frame["prod_code"].astype(str).str.strip().eq(product).all():
            raise ExcelAlarmReadError(f"{scope}/{product}: product sheet contains another product")
        flags = frame["flag"].map(lambda value: str(value).strip().lower())
        allowed = {"true", "false", "0", "1", "0.0", "1.0", "yes", "no", "y", "n", "delete", "", "nan", "none", "<na>"}
        if not flags.isin(allowed).all():
            raise ExcelAlarmReadError(f"{scope}/{product}: invalid flag values")
        # Normalize Excel's numeric booleans; no decision-ledger merge is allowed.
        frame["flag"] = ~flags.isin({"false", "0", "0.0", "no", "n"})
        event_column = "start_time" if scope == "aoi_tt" else "sheet_start_time"
        if pd.to_datetime(frame[event_column], errors="coerce").isna().any():
            raise ExcelAlarmReadError(f"{scope}/{product}: invalid event time")

    @staticmethod
    def _refresh_time(meta: pd.DataFrame, scope: str, product: str) -> pd.Timestamp | None:
        if meta.empty:
            return None
        required = {"scope", "prod_code", "last_generated_at"}
        if not required.issubset(meta.columns):
            raise ExcelAlarmReadError("Invalid __refresh_meta__ schema")
        rows = meta.loc[meta["scope"].astype(str).str.lower().eq(scope) & meta["prod_code"].astype(str).eq(product)]
        if rows.empty:
            return None
        parsed = pd.to_datetime(rows["last_generated_at"], errors="coerce")
        if parsed.isna().any():
            raise ExcelAlarmReadError("Invalid __refresh_meta__ generation time")
        return parsed.max()
