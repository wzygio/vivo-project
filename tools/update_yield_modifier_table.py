"""手动刷新入库良率修饰表：回写当月良损与缩放倍数。

与页面内逻辑共用同一入口（`modifier_table.sync_modifier_table`）：
仅更新目标月份的"当月良损"（缺失行追加），并在"指定良损"签名变化时
重算"缩放倍数"（三位小数，口径含上月回退）。

用法：
    python tools/update_yield_modifier_table.py --product M678
    python tools/update_yield_modifier_table.py --product M678 --month 2026-08
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="刷新入库良率修饰表（当月良损 + 缩放倍数）")
    parser.add_argument("--product", required=True, help="产品型号，如 M678")
    parser.add_argument(
        "--month",
        default=None,
        help="目标月份（YYYY-MM），默认当前月",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager
    from src.yield_domain.application.yield_service import YieldAnalysisService
    from yield_domain.core.mwd_trend.modifier_table import sync_modifier_table

    config = ConfigLoader.load_config(args.product)
    product_dir = ConfigLoader.get_project_root() / "resources" / args.product
    table_path = YieldAnalysisService.resolve_modifier_table_path(config, product_dir)
    current_month = args.month or datetime.now().strftime("%Y-%m")

    logging.info("读取 %s 的 panel 明细（快照/数据库）...", args.product)
    panel_df = YieldAnalysisService.get_modified_panel_details(
        config, DatabaseManager(), ""
    )
    if panel_df.empty:
        logging.error("未获取到 panel 明细，终止。")
        return 1

    table = sync_modifier_table(
        table_path,
        args.product,
        panel_df,
        current_month,
    )
    for level in ("group", "code"):
        rows = table[level]
        month_rows = rows[rows["时间标签"].astype(str) == current_month]
        logging.info(
            "%s 级：%s 月共 %d 行（全表 %d 行）已同步至 %s",
            level,
            current_month,
            len(month_rows),
            len(rows),
            table_path,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
