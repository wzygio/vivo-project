"""Compatibility entry for existing Windows startup shortcuts and supervisors."""
from pathlib import Path
import sys


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from api.anomaly.anomaly_api_service import main

    raise SystemExit(main())
