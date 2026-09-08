"""Printer CODE composition and explicit display-only decoration."""

import hashlib
import json
import random

import pandas as pd

PRINTER_CODES = tuple(f"C3DM{index}" for index in range(6))
PRINTER_DIMENSIONS = ["productcode", "line", "printer"]
SUMMARY_COLUMNS = [
    *PRINTER_DIMENSIONS, "rs_code", "code_num", "raw_ratio", "display_ratio",
]


def summarize_printers(
    glass_counts: pd.DataFrame,
    *,
    decorate: bool = True,
    minimum: float = 0.9,
    maximum: float = 0.92,
) -> pd.DataFrame:
    """Sum counts (not Glass percentages), retain six codes per observed group."""
    if glass_counts.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)
    frame = glass_counts[glass_counts["rs_code"].isin(PRINTER_CODES)].copy()
    frame["code_num"] = pd.to_numeric(frame["code_num"], errors="raise")
    if frame["code_num"].isna().any() or (frame["code_num"] < 0).any():
        raise ValueError("CODE 记录数必须为非负数")
    rows = []
    for group, values in frame.groupby(PRINTER_DIMENSIONS, sort=True):
        counts = values.groupby("rs_code")["code_num"].sum().reindex(
            PRINTER_CODES, fill_value=0,
        )
        total = counts.sum()
        if total <= 0:
            continue
        raw = counts / total
        display = raw.copy()
        # Local seeded RNG: stable across reruns, independent of row order and
        # other printers; never changes the global random generator or raw data.
        seed = hashlib.sha256(json.dumps(
            [list(group), counts.astype(float).tolist()], ensure_ascii=False,
        ).encode("utf-8")).digest()
        target = random.Random(seed).uniform(minimum, maximum)
        if decorate and raw["C3DM1"] < target:
            display = raw * ((1 - target) / (1 - raw["C3DM1"]))
            display["C3DM1"] = target
        for code in PRINTER_CODES:
            rows.append(dict(zip(PRINTER_DIMENSIONS, group, strict=True)) | {
                "rs_code": code, "code_num": counts[code],
                "raw_ratio": raw[code], "display_ratio": display[code],
            })
    return pd.DataFrame(rows, columns=SUMMARY_COLUMNS)
