"""Extract PPA-related raw measurements from per-product Parquet snapshots.

Filters each product's snapshot (data/<PROD>/inline_measurements_<PROD>.parquet)
by ``param_name LIKE '%PPA%'`` and a half-open ``start_time`` window, then writes
one Excel workbook with one sheet per product.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "output" / "ppa_raw_measurements_202607.xlsx"
DEFAULT_PRODUCTS = ("M626", "M673", "M678", "Z571", "Z517")
DEFAULT_START_DATE = "2026-07-01"
DEFAULT_END_DATE = "2026-07-31"  # exclusive
PPA_KEYWORD = "PPA"

logger = logging.getLogger(__name__)


def extract_ppa_measurements(
    snapshot_path: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """Read one product snapshot and keep PPA rows inside [start, end)."""
    snapshot = pd.read_parquet(snapshot_path)
    snapshot["start_time"] = pd.to_datetime(snapshot["start_time"], errors="coerce")

    mask = (
        snapshot["param_name"].astype(str).str.contains(PPA_KEYWORD, case=False, na=False)
        & (snapshot["start_time"] >= start)
        & (snapshot["start_time"] < end)
    )
    return snapshot.loc[mask].reset_index(drop=True)


VALUE_RANGE_SHEET = "param_value区间统计"
VALUE_RANGE_BINS = ("<6.5", "6.5-7.5", "7.5-8.5", "8.5-9.5", ">=9.5")


def summarize_param_value_ranges(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """按产品统计 param_value 各取值区间的记录数。

    区间按左闭右开：<6.5 | [6.5,7.5) | [7.5,8.5) | [8.5,9.5) | >=9.5。
    空值或非数值计入「无效/空值数」，不进入任何区间。
    """
    rows = []
    for prod_code, frame in frames.items():
        values = pd.to_numeric(frame["param_value"], errors="coerce")
        valid = values.dropna()
        counts = [
            int((valid < 6.5).sum()),
            int(((valid >= 6.5) & (valid < 7.5)).sum()),
            int(((valid >= 7.5) & (valid < 8.5)).sum()),
            int(((valid >= 8.5) & (valid < 9.5)).sum()),
            int((valid >= 9.5).sum()),
        ]
        rows.append(
            [prod_code, *counts, sum(counts), int(values.isna().sum())]
        )
    return pd.DataFrame(
        rows, columns=["产品", *VALUE_RANGE_BINS, "合计", "无效/空值数"]
    )

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--products", nargs="+", default=list(DEFAULT_PRODUCTS))
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="exclusive")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    start = pd.Timestamp(args.start_date)
    end = pd.Timestamp(args.end_date)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    extracted_frames: dict[str, pd.DataFrame] = {}
    with pd.ExcelWriter(args.output, engine="openpyxl") as writer:
        for prod_code in args.products:
            snapshot_path = (
                args.data_dir / prod_code / f"inline_measurements_{prod_code}.parquet"
            )
            if not snapshot_path.is_file():
                logger.warning("快照不存在，跳过 %s: %s", prod_code, snapshot_path)
                continue
            extracted = extract_ppa_measurements(snapshot_path, start, end)
            extracted_frames[prod_code] = extracted
            extracted.to_excel(writer, sheet_name=prod_code, index=False)
            logger.info("%s: 提取 %d 行 -> sheet '%s'", prod_code, len(extracted), prod_code)

        if extracted_frames:
            summary = summarize_param_value_ranges(extracted_frames)
            summary.to_excel(writer, sheet_name=VALUE_RANGE_SHEET, index=False)
            logger.info(
                "param_value 区间统计 -> sheet '%s'\n%s",
                VALUE_RANGE_SHEET,
                summary.to_string(index=False),
            )

    logger.info("输出完成: %s", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
