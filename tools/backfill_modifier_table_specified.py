"""一次性补全"入库良率修饰表"的"指定良损"列（Task2）。

填充优先级（left join，不新增修饰表中没有的记录、不覆盖已有指定值）：
1. `resources/趋势图人工修正.xlsx` 同 Sheet（<产品>_Group级 / <产品>_Code级）的
   月度数据（周期类型 == "月度"），按 (不良类型, 时间标签) 匹配"期望不良率"；
2. 仍缺失的 Code 级行，用 `resources/codebaseline.xlsx` 对应产品 Sheet，
   按 (defect_desc, baseline_month) 匹配"baseline_rate"（codebaseline 无 Group 级
   数据，Group 级仍缺失的行保持空缺，运行时按"当月良损"兜底）。

同时按 Task2 的小数位约定修正存量数据：当月良损/指定良损按百分数三位小数
（分数 5 位）舍入，缩放倍数整列按三位小数重算。

用法：
    python tools/backfill_modifier_table_specified.py --dry-run   # 只预览
    python tools/backfill_modifier_table_specified.py             # 实际写回
    python tools/backfill_modifier_table_specified.py --product M678
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from src.shared_kernel.utils.excel_tools import (
    _read_all_sheets_via_com,
    replace_workbook_sheet,
)
from yield_domain.core.mwd_trend.modifier_table import (
    COL_DEFECT,
    COL_MONTH,
    COL_PERIOD,
    COL_RAW_LOSS,
    COL_SCALE_FACTOR,
    COL_SPECIFIED_LOSS,
    FACTOR_DECIMALS,
    MODIFIER_TABLE_COLUMNS,
    RATE_DECIMALS,
    compute_scale_factors,
    parse_rate_value,
)

MODIFIER_TABLE_PATH = PROJECT_ROOT / "resources" / "入库良率修饰表.xlsx"
OVERRIDE_PATH = PROJECT_ROOT / "resources" / "趋势图人工修正.xlsx"
CODEBASELINE_PATH = PROJECT_ROOT / "resources" / "codebaseline.xlsx"


def _norm_month(value) -> str:
    """月份标签归一化为 YYYY-MM（兼容 "2026-6" 这类未补零写法）。"""
    text = str(value).strip()
    try:
        year, month = text.split("-", maxsplit=1)
        return f"{int(year):04d}-{int(month):02d}"
    except (TypeError, ValueError):
        return text


def _build_override_map(sheet_df: pd.DataFrame) -> dict[tuple[str, str], float]:
    """趋势图人工修正表 → {(目标名称, 月份): 期望不良率}（仅月度，重复键取最后一行）。"""
    mapping: dict[tuple[str, str], float] = {}
    if sheet_df.empty:
        return mapping
    monthly = sheet_df[sheet_df["周期类型"].astype(str).str.strip() == "月度"]
    for _, row in monthly.iterrows():
        rate = parse_rate_value(row.get("期望不良率"))
        if rate is None:
            continue
        key = (str(row.get("目标名称", "")).strip(), _norm_month(row.get("时间标签", "")))
        if key[0]:
            mapping[key] = rate
    return mapping


def _build_baseline_map(sheet_df: pd.DataFrame) -> dict[tuple[str, str], float]:
    """codebaseline 产品 Sheet → {(defect_desc, baseline_month): baseline_rate}。"""
    mapping: dict[tuple[str, str], float] = {}
    if sheet_df.empty:
        return mapping
    for _, row in sheet_df.iterrows():
        rate = parse_rate_value(row.get("baseline_rate"))
        if rate is None:
            continue
        key = (
            str(row.get("defect_desc", "")).strip(),
            _norm_month(row.get("baseline_month", "")),
        )
        if key[0]:
            mapping[key] = rate
    return mapping


def backfill_sheet(
    table_df: pd.DataFrame,
    override_map: dict[tuple[str, str], float],
    baseline_map: dict[tuple[str, str], float],
) -> tuple[pd.DataFrame, dict[str, int]]:
    """补全单个 Sheet：返回 (更新后的表, 统计计数)。"""
    updated = table_df.copy()
    for column in MODIFIER_TABLE_COLUMNS:
        if column not in updated.columns:
            updated[column] = None
    updated = updated[MODIFIER_TABLE_COLUMNS]

    stats = {"from_override": 0, "from_baseline": 0, "still_missing": 0}

    # 月份标签统一归一化，保证与 compute_scale_factors 的键一致
    updated[COL_MONTH] = updated[COL_MONTH].map(_norm_month)

    # Task2-1：存量"当月良损"按百分数三位小数（分数 5 位）舍入
    updated[COL_RAW_LOSS] = updated[COL_RAW_LOSS].map(
        lambda v: round(v, RATE_DECIMALS) if (v := parse_rate_value(v)) is not None else None
    )

    for idx, row in updated.iterrows():
        if parse_rate_value(row[COL_SPECIFIED_LOSS]) is not None:
            continue  # 已有指定值，不覆盖
        key = (str(row[COL_DEFECT]).strip(), _norm_month(row[COL_MONTH]))
        rate = override_map.get(key)
        if rate is not None:
            stats["from_override"] += 1
        else:
            rate = baseline_map.get(key)
            if rate is not None:
                stats["from_baseline"] += 1
        if rate is not None:
            updated.at[idx, COL_SPECIFIED_LOSS] = round(rate, RATE_DECIMALS)
        else:
            stats["still_missing"] += 1

    # 缩放倍数整列按三位小数重算（口径含上月回退，与运行时一致）
    factors = compute_scale_factors(updated)
    updated[COL_SCALE_FACTOR] = [
        round(
            factors.get((str(d).strip(), _norm_month(m)), 1.0),
            FACTOR_DECIMALS,
        )
        for d, m in zip(updated[COL_DEFECT], updated[COL_MONTH])
    ]
    # 周期类型缺失的默认补"月度"
    updated[COL_PERIOD] = updated[COL_PERIOD].fillna("月度")
    return updated, stats


def main() -> int:
    parser = argparse.ArgumentParser(description='补全入库良率修饰表的"指定良损"列')
    parser.add_argument("--product", default=None, help="只处理指定产品，如 M678")
    parser.add_argument("--dry-run", action="store_true", help="只预览统计，不写回")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    modifier_sheets = _read_all_sheets_via_com(MODIFIER_TABLE_PATH)
    override_sheets = _read_all_sheets_via_com(OVERRIDE_PATH)
    baseline_sheets = _read_all_sheets_via_com(CODEBASELINE_PATH)

    report_lines: list[str] = []
    for sheet_name, table_df in modifier_sheets.items():
        if args.product and not sheet_name.startswith(f"{args.product}_"):
            continue
        product = sheet_name.split("_", maxsplit=1)[0]
        override_map = _build_override_map(override_sheets.get(sheet_name, pd.DataFrame()))
        # codebaseline 仅 Code 级（defect_desc）；Group 级传入空 map，自然不命中
        baseline_map = (
            _build_baseline_map(baseline_sheets.get(product, pd.DataFrame()))
            if sheet_name.endswith("_Code级")
            else {}
        )

        updated, stats = backfill_sheet(table_df, override_map, baseline_map)
        line = (
            f"[{sheet_name}] 行数={len(updated)} 修正表填充={stats['from_override']} "
            f"codebaseline填充={stats['from_baseline']} 仍缺失={stats['still_missing']}"
        )
        logging.info("%s%s", line, "（dry-run，未写回）" if args.dry_run else "")
        report_lines.append(line)

        if not args.dry_run:
            try:
                replace_workbook_sheet(MODIFIER_TABLE_PATH, sheet_name, updated)
            except Exception as error:
                logging.error("写回 %s 失败: %s", sheet_name, error)
                return 1

    report_path = PROJECT_ROOT / "output" / "tmp" / "backfill_modifier_table_report.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    logging.info("报告已写入 %s", report_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
