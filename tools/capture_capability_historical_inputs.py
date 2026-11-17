"""Capture explicitly authorized older capability weeks without changing retained facts."""
from pathlib import Path
import argparse
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.inline_domain.infrastructure.monitor.capability_historical_source import capture_historical_capability_inputs
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weeks-json", type=Path, required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--output", type=Path, default=ROOT / "output/test-results/capability-summary/historical-inputs")
    args = parser.parse_args()
    authorized = json.loads(args.weeks_json.read_text(encoding="utf-8"))
    engine = DatabaseManager().engine
    if engine is None:
        raise RuntimeError("Database is unavailable for explicit historical capture")
    result = capture_historical_capability_inputs(engine, snapshot_dir=ROOT / "data/inline_domain/shared",
                                                  output_dir=args.output, authorized_weeks=authorized, as_of=args.as_of)
    print(json.dumps({"output": str(args.output), "products": result["products"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
