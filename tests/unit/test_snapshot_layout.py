import pytest

from src.shared_kernel.snapshot_paths import (
    aoi_rs_snapshot_directory,
    inline_measurement_directory,
    snapshot_component,
)
from src.inline_domain.infrastructure.shared.throughput_history_store import ThroughputHistoryStore


def test_domain_layout_helpers(tmp_path):
    assert inline_measurement_directory(tmp_path) == tmp_path / "inline_domain/shared"
    assert aoi_rs_snapshot_directory(tmp_path) == tmp_path / "inline_domain/aoi_rs"
    assert ThroughputHistoryStore(tmp_path / "inline_domain").snapshot_path("SPC", "M626") == tmp_path / "inline_domain/spc/throughput_M626.parquet"


@pytest.mark.parametrize("value", ["../M626", "M626/x", "M626\\x", "C:", "", ".."])
def test_reject_unsafe_product(value):
    with pytest.raises(ValueError):
        snapshot_component(value)
