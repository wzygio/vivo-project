# -*- coding: utf-8 -*-
"""补充探查3：确认 eda.spot_eda_tp_view_gls_v 结构与数据。"""
import sys
sys.path.insert(0, ".")
import pandas as pd
from sqlalchemy import text
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

eng = DatabaseManager().engine

def show(title, sql, max_rows=90):
    print(f"\n===== {title} =====")
    try:
        with eng.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        print(df.to_string(max_rows=max_rows))
    except Exception as e:
        print(f"ERROR: {e}")

show(
    "spot_eda_tp_view_gls_v 列",
    "SELECT ordinal_position, column_name, data_type FROM information_schema.columns "
    "WHERE table_schema='eda' AND table_name='spot_eda_tp_view_gls_v' ORDER BY 1",
)
show(
    "spot_eda_tp_view_gls_v 关键字段样例",
    "SELECT glass_id, glass_start_time, lot_id, step_id, product_spec "
    "FROM eda.spot_eda_tp_view_gls_v ORDER BY glass_start_time DESC LIMIT 5",
)
show(
    "spot_eda_tp_view_gls_v step_id distinct",
    "SELECT DISTINCT step_id FROM eda.spot_eda_tp_view_gls_v ORDER BY 1",
)
show(
    "TP RS 明细 step_id=43629 近一个月 distinct glass 数 vs 过货视图同站点同期间 distinct glass 数",
    "SELECT "
    "(SELECT COUNT(DISTINCT glass_id) FROM eda.spc_tzbjx_rs_tsp WHERE glass_start_time >= now() - interval '1 month') AS rs_glass_cnt, "
    "(SELECT COUNT(DISTINCT glass_id) FROM eda.spot_eda_tp_view_gls_v WHERE step_id='43629' AND glass_start_time >= now() - interval '1 month') AS view_glass_cnt",
)
