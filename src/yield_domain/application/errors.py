"""Stable failures at the Yield source boundary; no driver messages escape."""


class YieldSourceReadError(RuntimeError):
    """The requested source window could not be read completely."""

    def __init__(self) -> None:
        super().__init__("YIELD_SOURCE_UNAVAILABLE")
