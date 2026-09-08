"""Compatibility readers over computed caches; no OOS/OOC filesystem persistence."""

from __future__ import annotations

import pandas as pd

from src.inline_domain.application.monitor.live_source import LiveMonitorSource, monitor_date_window
from src.inline_domain.core.shared.oos_history import (
    OosHistorySnapshot, OosHistoryMetadata, OosScopeContract,
)
from src.inline_domain.core.shared.sheet_oos_decoration import OOS_KEY_COLUMNS, OOS_DETAIL_COLUMNS
from src.inline_domain.core.shared.sheet_ooc_decoration import (
    OOC_KEY_COLUMNS, OOC_DETAIL_COLUMNS, AOI_TT_OOC_KEY_COLUMNS,
    AOI_TT_OOC_DETAIL_COLUMNS, build_aoi_rs_ooc_detail,
)
from src.inline_domain.core.aoi_tt.aoi_tt_decoration import (
    AOI_TT_OOS_KEY_COLUMNS, AOI_TT_OOS_DETAIL_COLUMNS,
)
from src.inline_domain.core.aoi_rs.aoi_rs_decoration import (
    AOI_RS_OOS_KEY_COLUMNS, AOI_RS_OOS_DETAIL_COLUMNS,
)


class LiveHistoryStore:
    """Adapt cached fresh facts to existing monitor/matrix/admin consumer contracts."""

    is_computed_cache = True

    def __init__(self, source: LiveMonitorSource, alarm_type: str) -> None:
        self._source, self._alarm_type = source, alarm_type

    def contract_for(self, scope: str) -> OosScopeContract:
        is_ooc = self._alarm_type == "ooc"
        if scope in {"spc", "ctq"}:
            keys = OOC_KEY_COLUMNS if is_ooc else OOS_KEY_COLUMNS
            columns = OOC_DETAIL_COLUMNS if is_ooc else OOS_DETAIL_COLUMNS
            return OosScopeContract(tuple(keys), "sheet_start_time", tuple(columns))
        if scope == "aoi_tt":
            keys = AOI_TT_OOC_KEY_COLUMNS if is_ooc else AOI_TT_OOS_KEY_COLUMNS
            columns = AOI_TT_OOC_DETAIL_COLUMNS if is_ooc else AOI_TT_OOS_DETAIL_COLUMNS
            return OosScopeContract(tuple(keys), "start_time", tuple(columns))
        if scope == "aoi_rs":
            columns = build_aoi_rs_ooc_detail().columns if is_ooc else AOI_RS_OOS_DETAIL_COLUMNS
            return OosScopeContract(tuple(AOI_RS_OOS_KEY_COLUMNS), "sheet_start_time", tuple(columns))
        raise ValueError(f"Unsupported Inline scope: {scope}")

    def read(self, scope: str, prod_code: str) -> OosHistorySnapshot:
        payload = self._source.read_payload(prod_code, scope)
        # Decisions are joined by the reader; cached facts must not carry a
        # second flag column from the SPC decoration pipeline.
        frame = payload[self._alarm_type].drop(columns=["flag"], errors="ignore")
        start, end = monitor_date_window()
        return OosHistorySnapshot(
            frame=frame,
            metadata=OosHistoryMetadata(
                policy="inline-computed-cache-v1", scope=scope, prod_code=prod_code,
                coverage_start=start, coverage_end=str(pd.Timestamp(end) + pd.Timedelta(days=1)),
                refreshed_at=str(payload["computed_at"]), row_count=len(frame),
            ),
        )

    def source_signature(self, products, scopes) -> str:
        return self._source.source_signature(products, scopes)
