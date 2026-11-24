"""Canonical Excel values for manually maintained decoration decisions."""

from numbers import Real
from typing import Literal

DecorationFlag = bool | Literal["Delete"]


def parse_decoration_flag(value: object) -> DecorationFlag:
    """Normalize an explicit decision. Callers own defaults for new keys."""
    if isinstance(value, Real) and value in (0, 1):
        return bool(value)
    token = str(value).strip().casefold()
    if token == "true":
        return True
    if token == "false":
        return False
    if token == "delete":
        return "Delete"
    raise ValueError("Flag 应填写 True、False 或 Delete")
