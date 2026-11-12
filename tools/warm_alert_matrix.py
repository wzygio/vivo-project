"""Compatibility entry; matrix warmup now belongs to api.inspection."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from api.inspection.matrix_warmup import main

if __name__ == "__main__":
    import logging
    import os
    os.chdir(ROOT)
    logging.basicConfig(handlers=[logging.NullHandler()], force=True)
    raise SystemExit(main())
