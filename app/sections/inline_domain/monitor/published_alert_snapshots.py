"""Compatibility import for the shared published-snapshot consumer."""
import sys
from app.sections.shared import published_alert_snapshots as _implementation

# Preserve the public module, monkeypatch seams and exact shared cache identity.
sys.modules[__name__] = _implementation
