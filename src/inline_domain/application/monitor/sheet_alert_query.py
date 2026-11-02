"""Read and select published Inline anomalies without a presentation runtime."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Protocol

import pandas as pd

from src.inline_domain.core.shared.sheet_oos_alerts import build_sheet_oos_alerts


class SheetAlertReader(Protocol):
    """Consumer-owned contract implemented by the existing Excel alarm reader."""

    alarm_type: str

    def read_detail(self, scope: str, prod_code: str) -> pd.DataFrame | None: ...


_COMMON_COLUMNS = {
    "factory": "厂别", "step_id": "站点", "param_name": "参数名称",
    "sheet_id": "Sheet ID", "sheet_start_time": "超规时间", "oos_type": "超规类型",
}
_DISPLAY_COLUMNS = {
    "spc": _COMMON_COLUMNS,
    "ctq": _COMMON_COLUMNS,
    "aoi_tt": {
        "factory": "厂别", "step_id": "站点", "tt_name": "TT名称",
        "sheet_id": "Sheet ID", "start_time": "超规时间", "tt_qty": "TT数量",
        "usl": "规格上限",
    },
    "aoi_rs": {
        "factory": "厂别", "step_id": "站点", "rs_code": "RS Code",
        "chart_kind": "图类型", "point_id": "点位ID", "sheet_start_time": "超规时间",
        "value": "实测值", "spec": "规格上限",
    },
}


class SheetAlertQuery:
    """One effective-decision/week selection for report and HTTP consumers.

    The reader owns schema, product, classification and date-exclusion checks.
    Missing input and a valid empty publication are distinct native results;
    adapters decide how to expose each without running a source-data fallback.
    """

    def __init__(
        self,
        reader: SheetAlertReader,
        *,
        excluded_param_name_contains: Sequence[str] = (),
    ) -> None:
        self._reader = reader
        self._excluded_parameters = tuple(excluded_param_name_contains)
        if reader.alarm_type not in {"OOS", "OOC"}:
            raise ValueError("Unsupported Inline alarm type")

    def read(self, scope: str, product: str, reference_date: date) -> dict:
        if scope not in _DISPLAY_COLUMNS:
            raise ValueError("Unsupported Inline alert scope")
        detail = self._reader.read_detail(scope, product)
        columns = self._display_columns(scope)
        if detail is None:
            return self._result(
                "no_data", pd.DataFrame(), pd.DataFrame(columns=list(columns.values())),
                availability="missing", message="当前产品暂无预警记录。",
            )
        rows = build_sheet_oos_alerts(
            detail,
            time_column="start_time" if scope == "aoi_tt" else "sheet_start_time",
            reference_date=reference_date,
            excluded_param_name_contains=self._excluded_parameters if scope == "spc" else (),
        )
        display = rows.rename(columns=columns).reindex(columns=list(columns.values())).copy()
        if not display.empty:
            display["超规时间"] = display["超规时间"].astype(str)
            if scope == "aoi_rs":
                display["图类型"] = display["图类型"].replace({"lot": "By Lot", "sheet": "By Sheet"})
        generated = detail.attrs.get("refreshed_at")
        published_at = pd.Timestamp(generated).isoformat() if generated is not None else None
        return self._result(
            "alert" if len(rows) else "ok", rows, display,
            availability="ready", published_at=published_at,
        )

    def _display_columns(self, scope: str) -> dict[str, str]:
        columns = dict(_DISPLAY_COLUMNS[scope])
        if self._reader.alarm_type == "OOC":
            columns = {
                "ooc_type" if key == "oos_type" else "ucl" if key in {"usl", "spec"} else key:
                "控制上限" if value == "规格上限" else value
                for key, value in columns.items()
            }
        return columns

    @staticmethod
    def _result(
        state: str,
        records: pd.DataFrame,
        alerts: pd.DataFrame,
        *,
        availability: str,
        published_at: str | None = None,
        message: str = "",
    ) -> dict:
        factories = sorted({
            str(value).strip().upper()
            for value in records.get("factory", pd.Series(dtype=str)).dropna()
            if str(value).strip()
        })
        return {
            "state": state, "records": records.copy(), "alerts": alerts.copy(),
            "alert_factories": factories, "message": message,
            "metadata": {"availability": availability, "published_at": published_at},
        }
