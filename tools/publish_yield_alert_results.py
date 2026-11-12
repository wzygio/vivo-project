"""Compatibility entry; production implementation moved to api.inspection.yield_publication."""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / 'src'):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
from api.inspection import yield_publication as _implementation
if __name__ == '__main__':
    raise SystemExit(_implementation.main())
else:
    sys.modules[__name__] = _implementation
