"""Atomic per-product application results and the associated public-release ledger."""
from pathlib import Path
from tempfile import NamedTemporaryFile
import os

import pandas as pd

from src.inline_domain.core.shared.sheet_oos_decoration import get_decision_sheet_name
from src.inline_domain.core.spc.cpk_decoration import CPK_KEY_COLUMNS
from src.inline_domain.infrastructure.shared.alert_decisions import decision_rows
from src.inline_domain.infrastructure.shared.oos_history_store import OosHistoryStore
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.snapshot_paths import snapshot_component


def publication_path(product: str, scope: str, event: str) -> Path:
    directory = ConfigLoader.get_project_root() / "data/inline_domain/monitor/published_alerts"
    return directory / snapshot_component(product) / f"{snapshot_component(scope)}_{snapshot_component(event)}.parquet"


def write_publication(path: Path, rows: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as stream:
        temporary = Path(stream.name)
    try:
        rows.to_parquet(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def release_controls(sheets, product, scope, event):
    capability = event in ("CPK", "CPM")
    sheet = f"{product}_cpm" if event == "CPM" else product
    detail = sheets[sheet]
    if "flag" not in detail and "Flag" in detail:
        detail = detail.rename(columns={"Flag": "flag"})
    keys = CPK_KEY_COLUMNS if capability else list(OosHistoryStore.contract_for(scope).key_columns)
    required = set(keys)
    if scope == "aoi_rs" and detail.empty:
        required.discard("chart_kind")
    if "flag" not in detail or required - set(detail):
        raise ValueError("Missing published alert identity or release decision")
    if detail.empty:
        return keys, pd.DataFrame(columns=[*keys, "flag"])
    if not detail["prod_code"].astype(str).str.strip().eq(product).all():
        raise ValueError("Invalid release ledger product")
    if capability or get_decision_sheet_name(product) not in sheets:
        return keys, detail[[*keys, "flag"]]
    decisions = decision_rows(sheets, product, keys, True)
    current = pd.MultiIndex.from_frame(detail[keys].astype(str))
    return keys, decisions.loc[pd.MultiIndex.from_frame(decisions[keys].astype(str)).isin(current)]
