"""Stable application-facing Q-Time errors."""


class QTimeDataAccessError(RuntimeError):
    """Raised when the Q-Time database adapter cannot satisfy a read."""
