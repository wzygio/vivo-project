"""探查 dwt_warehousing_pnl 的 event_timekey 字段与 panel_id 重复情况。"""
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

    # 1. 确认 dwt_warehousing_pnl 是否含 event_timekey
    run(engine, "dwt_warehousing_pnl 列（含 timekey/flag）", """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE lower(table_name) = 'dwt_warehousing_pnl'
          AND (column_name LIKE '%timekey%' OR column_name LIKE '%flag%'
               OR column_name LIKE '%panel%' OR column_name LIKE '%defect%')
        ORDER BY ordinal_position
    """)

    # 2. last_flag='Y' 下 panel_id 是否有重复（26年 TP NG 范围）
    run(engine, "last_flag=Y 范围内的 panel_id 重复情况", """
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT dwp.panel_id) AS distinct_panels
        FROM dwt_warehousing_pnl dwp
        LEFT JOIN imp_ct_dft_group icdg ON dwp.first_defect_code = icdg.defect_code
        WHERE dwp.date_timekey > '20260101'
          AND icdg.factory = 'TP'
          AND dwp.last_flag = 'Y'
    """)

    # 3. 同一 panel_id 是否存在多条 last_flag='Y' 的记录
    run(engine, "重复 panel_id 样本（last_flag=Y 且同 panel 多行）", """
        SELECT dwp.panel_id, COUNT(*) AS cnt
        FROM dwt_warehousing_pnl dwp
        LEFT JOIN imp_ct_dft_group icdg ON dwp.first_defect_code = icdg.defect_code
        WHERE dwp.date_timekey > '20260101'
          AND icdg.factory = 'TP'
          AND dwp.last_flag = 'Y'
        GROUP BY dwp.panel_id
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 10
    """)

    # 4. 一个重复 panel 的所有记录长什么样（看 event_timekey / date_timekey 差异）
    run(engine, "某个重复 panel 的全部记录", """
        SELECT dwp.panel_id, dwp.date_timekey, dwp.event_timekey,
               dwp.last_flag, dwp.first_defect_code
        FROM dwt_warehousing_pnl dwp
        WHERE dwp.panel_id = (
            SELECT dwp2.panel_id
            FROM dwt_warehousing_pnl dwp2
            LEFT JOIN imp_ct_dft_group icdg2 ON dwp2.first_defect_code = icdg2.defect_code
            WHERE dwp2.date_timekey > '20260101'
              AND icdg2.factory = 'TP'
              AND dwp2.last_flag = 'Y'
            GROUP BY dwp2.panel_id
            HAVING COUNT(*) > 1
            LIMIT 1
        )
        ORDER BY dwp.event_timekey
    """)

    # 5. 同一 panel 在 WMS 表(LABELLEVEL=1) 是否也有多行
    run(engine, "WMS panel 行重复情况（同 panel 多行 labellevel=1）", """
        SELECT t.labelsn, COUNT(*) AS cnt
        FROM dwr_wms_tblreclabel t
        WHERE t.labellevel = 1 AND t.labelsn LIKE 'L%' AND length(t.labelsn) = 15
        GROUP BY t.labelsn
        HAVING COUNT(*) > 1
        LIMIT 10
    """)


if __name__ == "__main__":
    main()
