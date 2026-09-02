"""Stable application-facing Q-Time errors."""


class QTimeDataAccessError(RuntimeError):
    """Raised when the Q-Time database adapter cannot satisfy a read."""


class QTimeDecorationAccessError(RuntimeError):
    """Raised when the user-maintained Q-Time decoration workbook is unavailable."""
