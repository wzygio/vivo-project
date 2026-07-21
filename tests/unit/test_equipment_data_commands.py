from __future__ import annotations

from pathlib import Path

import pytest

from tools.fabricate_equipment_data import main as generate_main
from tools.update_fabricated_equipment_data import main as update_main


def test_generation_and_update_are_separate_commands_and_update_never_bootstraps(
    tmp_path: Path,
) -> None:
    assert generate_main is not update_main
    with pytest.raises(FileNotFoundError, match="fabricated snapshot not found"):
        update_main(
            [
                "--baseline",
                "resources/critical_parts_baseline.csv",
                "--output-dir",
                str(tmp_path),
                "--now",
                "2026-07-21T12:00:00",
            ]
        )
    assert not list(tmp_path.glob("*.parquet"))
