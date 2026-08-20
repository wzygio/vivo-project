"""探查 panel_id 跨记录重复（不限 last_flag）与箱行 labelsn 唯一性。"""
import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logging.basicConfig(level=logging.ERROR)


def run(engine, title: str, sql: str) -> pd.DataFrame:
    print(f"\n{'='*70}\n## {title}\n{'='*70}")
    try:
        df = pd.read_sql(text(sql), engine)
        print(df.to_string(max_rows=60, max_cols=20))
        return df
    except Exception as e:
        print(f"!! FAILED: {e}")
        return pd.DataFrame()


def main() -> None:
    db = DatabaseManager()
    if db.engine is None:
        print("db engine init failed")
        return
    engine = db.engine

    # 1. 26年范围内（不限 TP NG、不限 last_flag）panel_id 重复情况
    run(engine, "26年全量：行数 vs distinct panel_id", """
        SELECT COUNT(*) AS total_rows, COUNT(DISTINCT panel_id) AS distinct_panels
        FROM dwt_warehousing_pnl
        WHERE date_timekey > '20260101'
    """)

    # 2. 同一 panel 多条记录时 last_flag 分布
    run(engine, "重复 panel 的 last_flag 分布（26年）", """
        SELECT dup.cnt AS rows_per_panel, COUNT(*) AS panel_cnt
        FROM (
            SELECT panel_id, COUNT(*) AS cnt,
                   COUNT(*) FILTER (WHERE last_flag = 'Y') AS y_cnt
            FROM dwt_warehousing_pnl
            WHERE date_timekey > '20260101'
            GROUP BY panel_id
            HAVING COUNT(*) > 1
        ) dup
        GROUP BY dup.cnt
        ORDER BY dup.cnt
        LIMIT 20
    """)

    # 3. 一个重复 panel 的全部记录（看 event_timekey / defect / factory 变化）
    run(engine, "重复 panel 的全部记录样本", """
        SELECT dwp.panel_id, dwp.date_timekey, dwp.event_timekey, dwp.last_flag,
               dwp.first_defect_code, icdg.factory
        FROM dwt_warehousing_pnl dwp
        LEFT JOIN imp_ct_dft_group icdg ON dwp.first_defect_code = icdg.defect_code
        WHERE dwp.panel_id IN (
            SELECT panel_id FROM dwt_warehousing_pnl
            WHERE date_timekey > '20260101'
            GROUP BY panel_id HAVING COUNT(*) > 1
            LIMIT 5
        )
        ORDER BY dwp.panel_id, dwp.event_timekey
    """)

    # 4. last_flag='Y' 但 icdg.factory 非 TP 的重复 panel：去重后口径变化量
    #    即：panel 有多条记录，其中 last_flag=Y 的那条不一定是 event_timekey 最新的
    run(engine, "last_flag=Y 与 event_timekey 最新不一致的 panel 数（26年）", """
        WITH ranked AS (
            SELECT panel_id, last_flag, event_timekey,
                   ROW_NUMBER() OVER (PARTITION BY panel_id ORDER BY event_timekey DESC) AS rn
            FROM dwt_warehousing_pnl
            WHERE date_timekey > '20260101'
        )
        SELECT COUNT(*) AS mismatch_panels
        FROM ranked
        WHERE rn = 1 AND last_flag <> 'Y'
    """)

    # 5. 箱行 labelsn 唯一性（labellevel=3）
    run(engine, "箱行 labelsn 是否唯一（labellevel=3）", """
        SELECT COUNT(*) AS total_rows, COUNT(DISTINCT labelsn) AS distinct_box
        FROM dwr_wms_tblreclabel
        WHERE labellevel = 3
    """)


if __name__ == "__main__":
    main()
