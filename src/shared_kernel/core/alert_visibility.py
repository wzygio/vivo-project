"""Pure public-state policy shared by report and HTTP presentation adapters."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd

ROW_COLUMN = "监控参数"


def parse_matrix_config(frame: pd.DataFrame) -> dict[tuple[str, str], bool]:
    """Parse strict True/False cells; nonempty text is never implicitly true."""
    if ROW_COLUMN not in frame or frame.columns.duplicated().any():
        raise ValueError("修饰配置应为监控参数 × 产品矩阵，不能使用旧四维规则表")
    frame = frame.dropna(how="all").copy()
    names = frame[ROW_COLUMN].astype("string").str.strip()
    if names.isna().any() or names.eq("").any() or names.duplicated().any():
        raise ValueError("修饰配置的监控参数不得为空或重复")
    switches = {}
    for index, name in names.items():
        for product in frame.columns:
            if product == ROW_COLUMN:
                continue
            value = str(frame.at[index, product]).strip().lower()
            if value not in ("true", "false"):
                raise ValueError(f"{name} × {product} 只能填写 True 或 False")
            switches[(str(name), str(product))] = value == "true"
    return switches


def apply_matrix_compliance(payload: Mapping[str, Any], switches: Mapping) -> dict:
    """Project copied cells after calculation without changing source/cache data."""
    names = {row["row_key"]: row["display_name"] for row in payload.get("rows", [])}
    cells = {}
    for (row_key, product), cell in payload.get("cells", {}).items():
        if switches.get((names.get(row_key), product), False):
            cells[(row_key, product)] = {**cell, "state": "ok", "message": ""}
        else:
            cells[(row_key, product)] = dict(cell)
    return {**payload, "cells": cells}
