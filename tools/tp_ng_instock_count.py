"""统计 26 年在库 TP NG 不良 panel 数，并输出 Excel 报告。

对应任务文档: docs/dev_docs/dev_spec/others/task-tp_ng_count.md

统计口径（四个关键点）:
  1. 26年   : dwp.date_timekey > '20260101'
  2. TP NG  : dwp.first_defect_code 关联 imp_ct_dft_group 后 icdg.factory = 'TP'
  3. 在库   : panel 行(DWR_WMS_TBLRECLABEL T, LABELLEVEL=1)通过 T.PACKBOXNO
              关联到箱行(T1, LABELLEVEL=3)，箱行满足
              T1.ACCOUTSTATUS = 'lm_deliveried'（不筛选库位 INVENTORYCODE）
  4. panel  : 按 panel_id 去重，保留 event_timekey 最新的一条记录
              （已探查验证：等价于 last_flag = 'Y'，26年范围内两者不一致的 panel 数为 0）

panel_id 与 ACCOUTSTATUS 的关联方式（任务关键问题，已探查确认）:
  DWR_WMS_TBLRECLABEL 是自关联标签层级表:
    - LABELLEVEL = 1: panel 级行，LABELSN 即 panel_id（15 位、L 开头），
      PACKBOXNO 指向所属箱号；
    - LABELLEVEL = 3: 箱级行，LABELSN 即箱号（W 开头），
      库存状态 ACCOUTSTATUS 与库位 INVENTORYCODE 只记录在箱行上。

输出: output/reports/tp_ng_instock_count_<YYYYMMDD_HHMMSS>.xlsx
  - Sheet "总数"      : 26年 TP NG panel 总数及在库数
  - Sheet "按库存状态" : 各箱 ACCOUTSTATUS（含库位）的 panel 数
  - Sheet "按不良类型" : 各 defect_code/defect_desc 的 panel 数（区分在库/非在库）

用法:
    .venv/Scripts/python tools/tp_ng_instock_count.py
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# 允许从仓库根目录直接运行
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORT_DIR = Path("output/reports")

# 在库判定条件：箱状态为 lm_deliveried 即视为在库，不筛选库位
INSTOCK_STATUS = "lm_deliveried"

# 明细查询：26年 TP NG panel（按 event_timekey 最新去重）关联箱行状态
DETAIL_SQL = """
WITH latest AS (
    -- 按 panel_id 去重：保留 event_timekey 最新的一条记录
    SELECT panel_id, first_defect_code
    FROM (
        SELECT dwp.panel_id,
               dwp.first_defect_code,
               ROW_NUMBER() OVER (
                   PARTITION BY dwp.panel_id
                   ORDER BY dwp.event_timekey DESC
               ) AS rn
        FROM dwt_warehousing_pnl dwp
        WHERE dwp.date_timekey > '20260101'
    ) ranked
    WHERE rn = 1
)
SELECT
    latest.panel_id,
    latest.first_defect_code AS defect_code,
    icdg.defect_desc,
    t1.accoutstatus  AS box_status,
    t1.inventorycode AS inventory_code
FROM latest
INNER JOIN imp_ct_dft_group icdg
    ON latest.first_defect_code = icdg.defect_code
   AND icdg.factory = 'TP'
LEFT JOIN dwr_wms_tblreclabel t
    ON latest.panel_id = t.labelsn
   AND t.labellevel = 1
LEFT JOIN dwr_wms_tblreclabel t1
    ON t.packboxno = t1.labelsn
   AND t1.labellevel = 3
"""


def load_detail(db_manager: DatabaseManager) -> pd.DataFrame:
    """提取 26年 TP NG panel 明细（含箱状态）。"""
    if db_manager.engine is None:
        raise RuntimeError("数据库引擎未初始化。")

    logger.info("正在执行明细查询（26年 TP NG，按 event_timekey 去重）...")
    df = pd.read_sql(text(DETAIL_SQL), db_manager.engine)
    df.columns = df.columns.str.lower()

    # 防御：同一 panel 关联到多条 WMS 记录时去重（探查显示不会发生，双保险）
    before = len(df)
    df = df.drop_duplicates(subset=["panel_id"])
    if len(df) != before:
        logger.warning(f"WMS 关联产生重复 panel，已去重: {before} -> {len(df)}")

    df["in_stock"] = df["box_status"].eq(INSTOCK_STATUS)

    logger.info(f"共 {len(df)} 片 TP NG panel，其中在库 {int(df['in_stock'].sum())} 片。")
    return df


def build_reports(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """构建三个统计维度。"""
    total = len(df)
    instock = int(df["in_stock"].sum())
    summary = pd.DataFrame(
        {
            "指标": ["26年 TP NG panel 总数（去重后）", "其中：在库 panel 数", "其中：非在库/无库存记录 panel 数"],
            "数量": [total, instock, total - instock],
        }
    )

    by_status = (
        df.assign(
            box_status=df["box_status"].fillna("(无WMS记录)"),
            inventory_code=df["inventory_code"].fillna("(无)"),
        )
        .groupby(["box_status", "inventory_code"], as_index=False)
        .agg(panel_cnt=("panel_id", "nunique"))
        .sort_values("panel_cnt", ascending=False)
    )
    by_status.insert(
        0,
        "是否在库",
        by_status["box_status"].eq(INSTOCK_STATUS),
    )

    by_defect = (
        df.groupby(["defect_code", "defect_desc"], as_index=False, dropna=False)
        .agg(
            panel_cnt=("panel_id", "nunique"),
            instock_cnt=("in_stock", "sum"),
        )
        .sort_values("panel_cnt", ascending=False)
    )
    by_defect["非在库数"] = by_defect["panel_cnt"] - by_defect["instock_cnt"]
    by_defect = by_defect.rename(
        columns={
            "defect_code": "不良代码",
            "defect_desc": "不良类型",
            "panel_cnt": "panel数",
            "instock_cnt": "在库数",
        }
    )

    return {"总数": summary, "按库存状态": by_status, "按不良类型": by_defect}


def main() -> None:
    db_manager = DatabaseManager()
    detail_df = load_detail(db_manager)
    if detail_df.empty:
        logger.warning("查询结果为空，未生成报告。")
        return

    sheets = build_reports(detail_df)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORT_DIR / f"tp_ng_instock_count_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for name, sheet_df in sheets.items():
            sheet_df.to_excel(writer, sheet_name=name, index=False)
    logger.info(f"报告已输出: {out_path}")

    # 控制台同时打印摘要，便于直接查看
    for name, sheet_df in sheets.items():
        print(f"\n===== {name} =====")
        print(sheet_df.head(30).to_string(index=False))


if __name__ == "__main__":
    main()
