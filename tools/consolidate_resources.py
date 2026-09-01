# -*- coding: utf-8 -*-
"""[一次性迁移] 将 resources/<产品>/ 下的配置 Excel 汇总为 resources/ 根目录的单一工作簿。

规则：每个产品一个 sheet；主数据 sheet（原名 Sheet1）→ 产品号；
辅助 sheet → <产品号>_<原sheet名>（如 M678_Group级、M678_metadata）。

用法：
    python -X utf8 tools/consolidate_resources.py            # 迁移并校验
    python -X utf8 tools/consolidate_resources.py --verify   # 仅校验

迁移范围（7 类，detail 明细文件不迁移）：
    codebaseline / 入库不良率规格 / 趋势图人工修正 / override_rates /
    spc_cpk_cpm_decoration / spc_sheet_oos_decoration / ctq_sheet_oos_decoration
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("consolidate")

PRODUCTS = ["M626", "M673", "M678", "Z517", "Z571"]

# (汇总文件名, 源文件名模板, 是否强制 sheet 名=产品号)
# 强制产品号用于单 sheet 的 decoration 文件（代码按产品号定位 sheet）；
# 其余文件 Sheet1→产品号，辅助 sheet→<产品号>_<原名>
TARGETS = [
    ("codebaseline.xlsx", "{p}_codebaseline.xlsx", False),
    ("入库不良率规格.xlsx", "{p}_入库不良率规格.xlsx", False),
    ("趋势图人工修正.xlsx", "{p}_趋势图人工修正.xlsx", False),
    ("override_rates.xlsx", "{p}_override_rates.xlsx", False),
    ("spc_cpk_cpm_decoration.xlsx", "spc_cpk_cpm_decoration.xlsx", True),
    ("spc_sheet_oos_decoration.xlsx", "spc_sheet_oos_decoration.xlsx", True),
    ("ctq_sheet_oos_decoration.xlsx", "ctq_sheet_oos_decoration.xlsx", True),
]


def target_sheet_name(prod_code: str, source_sheet_name: str) -> str:
    """源 sheet 名 -> 汇总工作簿 sheet 名。"""
    if source_sheet_name == "Sheet1":
        return prod_code
    return f"{prod_code}_{source_sheet_name.lstrip('_')}"


def read_all_sheets_raw(path: Path) -> dict[str, pd.DataFrame]:
    """以 header=None 原样读取全部 sheets；企业加密文件回退 Excel COM。"""
    try:
        return pd.read_excel(path, sheet_name=None, header=None, dtype=object)
    except Exception as exc:
        logger.warning("openpyxl 读取 %s 失败（%s），尝试 Excel COM…", path.name, type(exc).__name__)
        from src.shared_kernel.utils.excel_tools import _read_all_sheets_via_com

        sheets = _read_all_sheets_via_com(path)
        # COM 读出的 DataFrame 首行是表头；还原为 header=None 的原始矩阵
        raw: dict[str, pd.DataFrame] = {}
        for name, df in sheets.items():
            header_row = pd.DataFrame([list(df.columns)])
            body = df.reset_index(drop=True)
            body.columns = range(len(body.columns))
            header_row.columns = range(len(header_row.columns))
            raw[name] = pd.concat([header_row, body], ignore_index=True)
        return raw


def check_lock_files(resources_dir: Path) -> list[Path]:
    return sorted(resources_dir.rglob("~$*.xlsx"))


def migrate(resources_dir: Path) -> dict[str, dict[str, pd.DataFrame]]:
    """执行迁移，返回 {汇总文件名: {sheet名: 原始矩阵}}。"""
    consolidated: dict[str, dict[str, pd.DataFrame]] = {}
    for target_name, source_pattern, force_prod_sheet in TARGETS:
        sheets: dict[str, pd.DataFrame] = {}
        for prod in PRODUCTS:
            source_path = resources_dir / prod / source_pattern.format(p=prod)
            if not source_path.exists():
                logger.info("跳过缺失文件: %s", source_path)
                continue
            for src_sheet, df in read_all_sheets_raw(source_path).items():
                tgt_sheet = prod if force_prod_sheet else target_sheet_name(prod, src_sheet)
                df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
                sheets[tgt_sheet] = df
                logger.info("迁移 %s [%s] -> %s [%s] (%d 行)", source_path.name, src_sheet, target_name, tgt_sheet, len(df))
        consolidated[target_name] = sheets

    for target_name, sheets in consolidated.items():
        if not sheets:
            continue
        target_path = resources_dir / target_name
        with pd.ExcelWriter(target_path, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
        logger.info("✅ 写出 %s（%d 个 sheet）", target_path, len(sheets))
    return consolidated


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    df = df.reset_index(drop=True)
    df.columns = range(len(df.columns))

    def _cell(value: object) -> str:
        if pd.isna(value):
            return ""
        if isinstance(value, (int, float)):
            # 消除 COM 与 openpyxl 之间的浮点表示差异（如 0.028700000000000003 vs 0.0287）
            return repr(round(float(value), 9))
        return str(value)

    return df.map(_cell)


def verify(resources_dir: Path) -> bool:
    """逐 sheet 比对汇总工作簿与源文件内容，返回是否全部一致。"""
    ok = True
    for target_name, source_pattern, force_prod_sheet in TARGETS:
        target_path = resources_dir / target_name
        if not target_path.exists():
            logger.error("汇总文件不存在: %s", target_path)
            ok = False
            continue
        target_sheets = pd.read_excel(target_path, sheet_name=None, header=None, dtype=object)
        for prod in PRODUCTS:
            source_path = resources_dir / prod / source_pattern.format(p=prod)
            if not source_path.exists():
                continue
            for src_sheet, src_df in read_all_sheets_raw(source_path).items():
                tgt_sheet = prod if force_prod_sheet else target_sheet_name(prod, src_sheet)
                if tgt_sheet not in target_sheets:
                    logger.error("❌ %s 缺少 sheet %s（源: %s [%s]）", target_name, tgt_sheet, source_path.name, src_sheet)
                    ok = False
                    continue
                a = _normalize(src_df)
                b = _normalize(target_sheets[tgt_sheet])
                if a.shape != b.shape or not a.equals(b):
                    logger.error(
                        "❌ 内容不一致: %s [%s] vs %s [%s]（shape %s vs %s）",
                        source_path.name, src_sheet, target_name, tgt_sheet, a.shape, b.shape,
                    )
                    ok = False
                else:
                    logger.info("✔ 一致: %s [%s] == %s [%s]", source_path.name, src_sheet, target_name, tgt_sheet)
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true", help="仅校验，不执行迁移")
    args = parser.parse_args()

    from src.shared_kernel.config import ConfigLoader

    resources_dir = ConfigLoader.get_project_root() / "resources"

    locks = check_lock_files(resources_dir)
    if locks:
        logger.error("检测到 Excel 锁文件，请先关闭所有 Excel 窗口：%s", [str(p) for p in locks])
        return 2

    if not args.verify:
        migrate(resources_dir)

    if verify(resources_dir):
        logger.info("✅ 全部校验通过")
        return 0
    logger.error("❌ 校验存在不一致")
    return 1


if __name__ == "__main__":
    sys.exit(main())
