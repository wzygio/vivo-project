# -*- coding: utf-8 -*-
"""补充探查2：spot_eda 系列视图 + TP 候选视图结构。"""
import sys
sys.path.insert(0, ".")
import pandas as pd
from sqlalchemy import text
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

eng = DatabaseManager().engine

def show(title, sql, max_rows=90):
    print(f"\n===== {title} =====")
    print(sql)
    try:
        with eng.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        print(df.to_string(max_rows=max_rows))
    except Exception as e:
        print(f"ERROR: {e}")

show(
    "eda 中 spot_eda 系列视图/表",
    "SELECT table_name, table_type FROM information_schema.tables "
    "WHERE table_schema='eda' AND table_name ILIKE 'spot_eda%' ORDER BY 1",
)
show(
    "spot_eda_tsp_dv_v 列",
    "SELECT ordinal_position, column_name, data_type FROM information_schema.columns "
    "WHERE table_schema='eda' AND table_name='spot_eda_tsp_dv_v' ORDER BY 1",
)
show(
    "spot_eda_tsp_dv_v 样例",
    "SELECT * FROM eda.spot_eda_tsp_dv_v LIMIT 3",
)
