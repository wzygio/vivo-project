"""Generate a current-value critical-parts snapshot from the specification baseline."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import replace
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
from src.equipment_domain.infrastructure.fake_data import (
    generate_fabricated_snapshot,
    write_fabricated_snapshot,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate an initial fabricated critical-parts snapshot.",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("resources/critical_parts_baseline.csv"),
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--seed", type=int)
    parser.add_argument(
        "--as-of",
        help=(
            "Generation cutoff; each row receives a stable key-derived time "
            "within the configured lookback window."
        ),
    )
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)

    runtime = get_equipment_runtime_config()
    policy = runtime.fabrication_policy
    if args.seed is not None:
        policy = replace(policy, random_seed=args.seed)
    as_of = pd.Timestamp(args.as_of) if args.as_of else pd.Timestamp.now().floor("s")
    output_dir = args.output_dir or runtime.snapshot_dir

    spec_df = load_spec_baseline(args.baseline)
    result = generate_fabricated_snapshot(spec_df, policy, as_of=as_of)
    output_path = write_fabricated_snapshot(
        result.snapshot_df,
        spec_df,
        output_dir=output_dir,
        overwrite=args.overwrite,
    )
    summary = {**result.summary, "output_path": str(output_path.resolve())}
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
