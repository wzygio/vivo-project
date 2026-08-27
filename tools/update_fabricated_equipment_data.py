"""Update an existing fabricated critical-parts snapshot under its 24-hour TTL."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
from pathlib import Path
import sys

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
for import_path in (REPO_ROOT, REPO_ROOT / "src"):
    path_text = str(import_path)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)

from src.equipment_domain.config import get_equipment_runtime_config
from src.equipment_domain.infrastructure.data_loader import load_spec_baseline
from src.equipment_domain.infrastructure.fake_data_updater import (
    update_fabricated_snapshot_file,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Update an existing fabricated critical-parts snapshot.",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("resources/equipment_domain/critical_parts_baseline.csv"),
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--now",
        help="ISO timestamp used for the 24-hour freshness check; defaults to local time.",
    )
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    runtime = get_equipment_runtime_config()
    spec_df = load_spec_baseline(args.baseline)
    now = pd.Timestamp(args.now) if args.now else pd.Timestamp.now().floor("s")
    outcome = update_fabricated_snapshot_file(
        spec_df,
        runtime.fabrication_policy,
        output_dir=args.output_dir or runtime.snapshot_dir,
        now=now,
        force=args.force,
    )
    summary = {
        **outcome.summary,
        "updated": outcome.updated,
        "output_path": str(outcome.path.resolve()),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
