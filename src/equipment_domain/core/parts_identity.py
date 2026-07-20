"""Stable internal identity for fabricated critical-parts measurements."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Mapping
import unicodedata

import pandas as pd


FABRICATED_PARAM_PREFIX = "__FABRICATED_PART__"
SPEC_IDENTITY_COLUMNS = (
    "厂别",
    "备件类型",
    "设备类型",
    "膜层",
    "制程",
    "寿命规格",
    "站点",
    "机台号-腔室",
)


def _normalize_identity_value(column: str, value: object) -> str:
    """Normalize one scalar without depending on DataFrame row order or dtype."""
    if value is None or bool(pd.isna(value)):
        return ""
    text = unicodedata.normalize("NFKC", str(value)).strip()
    if column != "寿命规格":
        return text
    try:
        numeric = Decimal(text)
    except InvalidOperation:
        return text
    if not numeric.is_finite():
        return text
    return format(numeric.normalize(), "f")


def build_fabricated_param_name(spec_row: Mapping[str, object]) -> str:
    """Build a deterministic, namespaced key for one blank-parameter specification."""
    identity_values = [
        _normalize_identity_value(column, spec_row.get(column))
        for column in SPEC_IDENTITY_COLUMNS
    ]
    payload = json.dumps(identity_values, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"{FABRICATED_PARAM_PREFIX}{digest}"

