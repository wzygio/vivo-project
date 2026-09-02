"""Stable application-facing IJP overflow errors."""


class IjpDataAccessError(RuntimeError):
    """Raised when the IJP overflow database adapter cannot satisfy a read."""
