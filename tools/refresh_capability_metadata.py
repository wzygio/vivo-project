"""Capture small SPC dimensions read-only for snapshot-only capability summaries."""

from pathlib import Path
import argparse
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from src.inline_domain.infrastructure.monitor.capability_metadata_snapshot import capture_capability_metadata
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--as-of", help="Dimension capture timestamp; defaults to now")
    parser.add_argument("--products", nargs="*")
    arguments = parser.parse_args()
    enabled = ConfigLoader.get_enabled_products()
    selected = arguments.products if arguments.products is not None else enabled
    if not set(selected).issubset(enabled):
        parser.error("Products must belong to the enabled product registry")
    database = DatabaseManager()
    if database.engine is None:
        print(json.dumps({"status": "failed", "error_type": "DATABASE_ENGINE_UNAVAILABLE"}))
        return 1
    try:
        result = capture_capability_metadata(database.engine, selected, directory=arguments.output, as_of=arguments.as_of)
    except Exception as exc:
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__}))
        return 1
    print(json.dumps({"status": "complete", "generation": result["generation"],
                      "read_only": result["read_only"], "products": {
                          product: {kind: entry["rows"] for kind, entry in files.items()}
                          for product, files in result["products"].items()
                      }}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
