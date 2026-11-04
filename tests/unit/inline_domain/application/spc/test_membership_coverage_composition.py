"""Source metadata, rather than observed points, proves complete display weeks."""
from datetime import date
import json

import pandas as pd
import pytest

from src.inline_domain.composition import build_spc_sheet_membership_coverage
from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import InlineMeasurementSnapshotRepository
from src.inline_domain.infrastructure.shared.rolling_snapshot import publish_snapshots
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy


def test_raw_coverage_maps_to_display_once_and_validates_generation(tmp_path, monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(ConfigLoader, "get_data_forward_policy", lambda: DataForwardPolicy(True, 4))
    path = tmp_path / "data/inline_domain/shared/inline_measurements_M626.parquet"
    path.parent.mkdir(parents=True)
    publish_snapshots([(path, pd.DataFrame({"start_time": [pd.Timestamp("2026-10-01")]}),
                        InlineMeasurementSnapshotRepository.SNAPSHOT_POLICY_VERSION, "2026-10-31", True)])
    metadata = json.loads(path.with_suffix(".snapshot.json").read_text())
    source_start = date.fromisoformat(metadata["covered_from"])
    coverage = build_spc_sheet_membership_coverage("M626")
    proof = coverage()
    assert proof["start_date"] == (pd.Timestamp(source_start) + pd.Timedelta(days=4)).date().isoformat()
    assert proof["end_date"] == "2026-11-04" and len(proof["source_generation"]) == 64
    # A replaced/unfinished generation cannot advertise the old coverage.
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="coverage unavailable"):
        coverage()


def test_missing_source_manifest_cannot_claim_requested_week(tmp_path, monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    with pytest.raises(ValueError, match="coverage unavailable"):
        build_spc_sheet_membership_coverage("M626")()
