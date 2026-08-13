from __future__ import annotations

from pathlib import Path

import pytest

from tools.fabricate_equipment_data import main as generate_main
from tools.update_fabricated_equipment_data import main as update_main


def test_manual_update_command_does_not_silently_bootstrap(
    tmp_path: Path,
) -> None:
    baseline_path = tmp_path / "baseline.csv"
    baseline_path.write_text(
        "厂别,备件类型,设备类型,膜层,制程,寿命规格,站点,机台号-腔室,参数名称\n"
        "Array,Target,PVD,MO,DEPO,100,S1,EQ1-PM3,%TRGTLIFE%_G_MAX\n",
        encoding="utf-8-sig",
    )
    assert generate_main is not update_main
    with pytest.raises(FileNotFoundError, match="fabricated snapshot not found"):
        update_main(
            [
                "--baseline",
                str(baseline_path),
                "--output-dir",
                str(tmp_path),
                "--now",
                "2026-07-21T12:00:00",
            ]
        )
    assert not list(tmp_path.glob("*.parquet"))
