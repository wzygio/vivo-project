"""One-off post-adjustment for output/ppa_raw_measurements_202607.xlsx.

Re-extracts the same PPA data as tools/extract_ppa_raw_measurements.py,
subtracts 2 from every param_value > 8 (all product sheets), and rewrites
the workbook including a stats sheet computed on the adjusted data.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.extract_ppa_raw_measurements import (  # noqa: E402
    DEFAULT_DATA_DIR,
    DEFAULT_END_DATE,
    DEFAULT_OUTPUT_PATH,
    DEFAULT_PRODUCTS,
    DEFAULT_START_DATE,
    VALUE_RANGE_SHEET,
    extract_ppa_measurements,
    summarize_param_value_ranges,
)

GT8_THRESHOLD = 8.0
GT8_OFFSET = -2.0


def main() -> int:
    start = pd.Timestamp(DEFAULT_START_DATE)
    end = pd.Timestamp(DEFAULT_END_DATE)

    frames: dict[str, pd.DataFrame] = {}
    for prod_code in DEFAULT_PRODUCTS:
        snapshot_path = (
            DEFAULT_DATA_DIR / prod_code / f"inline_measurements_{prod_code}.parquet"
        )
        if not snapshot_path.is_file():
            continue
        frame = extract_ppa_measurements(snapshot_path, start, end)
        values = pd.to_numeric(frame["param_value"], errors="coerce")
        mask = values.gt(GT8_THRESHOLD)
        frame.loc[mask, "param_value"] = values[mask] + GT8_OFFSET
        print(f"{prod_code}: >8 adjusted rows = {int(mask.sum())}")
        frames[prod_code] = frame

    summary = summarize_param_value_ranges(frames)
    with pd.ExcelWriter(DEFAULT_OUTPUT_PATH, engine="openpyxl") as writer:
        for prod_code, frame in frames.items():
            frame.to_excel(writer, sheet_name=prod_code, index=False)
        summary.to_excel(writer, sheet_name=VALUE_RANGE_SHEET, index=False)

    print(summary.to_string(index=False))
    print("written:", DEFAULT_OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
