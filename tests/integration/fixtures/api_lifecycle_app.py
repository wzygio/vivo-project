"""Minimal real API for process cleanup checks; no data or database adapters."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
for import_path in (ROOT, ROOT / "src"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from api.main import create_app


class EmptyService:
    def read(self, *args):
        raise AssertionError("Lifecycle health checks must not read alert sources")


app = create_app(service=EmptyService(), clients_provider=lambda: (), enabled_products=lambda: [])


def main() -> None:
    import uvicorn

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", required=True, type=int)
    arguments = parser.parse_args()
    if not 1 <= arguments.port <= 65535 or arguments.port == 8890:
        parser.error("Use the caller's unique ephemeral test port.")
    uvicorn.run(app, host="127.0.0.1", port=arguments.port, workers=1,
                log_level="warning", access_log=False, timeout_graceful_shutdown=2)


if __name__ == "__main__":
    main()
