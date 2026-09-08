"""Long-term Parquet storage for Inline OOC facts."""

from __future__ import annotations

from pathlib import Path

from src.inline_domain.core.shared.oos_history import OosScopeContract
from src.inline_domain.core.shared.sheet_ooc_decoration import (
    AOI_TT_OOC_DETAIL_COLUMNS,
    AOI_TT_OOC_KEY_COLUMNS,
    OOC_DETAIL_COLUMNS,
    OOC_KEY_COLUMNS,
    build_aoi_rs_ooc_detail,
)
from src.inline_domain.infrastructure.shared.oos_history_store import (
    OosHistoryContractError,
    OosHistoryStore,
)


OOC_SCOPE_CONTRACTS = {
    "spc": OosScopeContract(tuple(OOC_KEY_COLUMNS), "sheet_start_time", tuple(OOC_DETAIL_COLUMNS)),
    "ctq": OosScopeContract(tuple(OOC_KEY_COLUMNS), "sheet_start_time", tuple(OOC_DETAIL_COLUMNS)),
    "aoi_tt": OosScopeContract(
        tuple(AOI_TT_OOC_KEY_COLUMNS), "start_time", tuple(AOI_TT_OOC_DETAIL_COLUMNS)
    ),
    "aoi_rs": OosScopeContract(
        ("prod_code", "step_id", "rs_code", "point_id"),
        "sheet_start_time",
        tuple(build_aoi_rs_ooc_detail().columns),
    ),
}


class OocHistoryStore(OosHistoryStore):
    POLICY_VERSION = "inline-ooc-history-v1"
    METADATA_KEY = b"inline_ooc_history"

    def __init__(self, snapshot_dir: Path | str = Path("data/inline_domain/ooc_history")) -> None:
        super().__init__(snapshot_dir)

    @staticmethod
    def contract_for(scope: str) -> OosScopeContract:
        normalized = str(scope).strip().lower()
        try:
            return OOC_SCOPE_CONTRACTS[normalized]
        except KeyError as exc:
            raise OosHistoryContractError(f"Unsupported OOC scope: {scope!r}") from exc
