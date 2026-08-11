# -*- coding: utf-8 -*-
"""AOI_TT 报表数据源探查脚本。

用法（Git Bash，项目根目录下）：
    .venv/Scripts/python.exe .scratch/probe_aoi_tt.py

输出：.scratch/probe_aoi_tt_result.md
"""
import sys
import traceback
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from sqlalchemy import text

from src.shared_kernel.infrastructure.db_handler import DatabaseManager

OUT_PATH = Path(__file__).resolve().parent / "probe_aoi_tt_result.md"

engine = DatabaseManager().engine

out = []


def flush():
    OUT_PATH.write_text("\n".join(out), encoding="utf-8")


def md(line=""):
    out.append(line)


def df_to_md(df, max_rows=80):
    if df is None:
        return "_(查询失败)_"
    if df.empty:
        return "_(0 行)_"
    df = df.head(max_rows)
    cols = [str(c) for c in df.columns]
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for _, row in df.iterrows():
        vals = []
        for v in row.values:
            s = str(v).replace("|", "\\|").replace("\n", " ")
            if len(s) > 120:
                s = s[:117] + "..."
            vals.append(s)
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def run(sql, label):
    md(f"\n## {label}\n")
    md("```sql")
    md(sql.strip())
    md("```\n")
    try:
        df = pd.read_sql(text(sql), engine)
        md(df_to_md(df))
        md(f"\n_({len(df)} 行)_")
    except Exception:
        md("```")
        md(traceback.format_exc())
        md("```")
    flush()


md(f"# AOI_TT 数据源探查结果 — {datetime.now():%Y-%m-%d %H:%M:%S}")
flush()

if engine is None:
    md("\n**数据库引擎初始化失败，终止。**")
    flush()
    sys.exit(1)

TABLES = {
    "array": ("eda.spc_tzbjx_array", "sheet_start_time"),
    "oled": ("eda.spc_tzbjx_oled", "glass_start_time"),
    "tsp": ("eda.spc_tzbjx_tsp", "glass_start_time"),
}

# 1) 三表列结构
for fac, (table, _) in TABLES.items():
    schema, tbl = table.split(".")
    run(
        f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{tbl}'
        ORDER BY ordinal_position
        """,
        f"列结构: {table}",
    )

# 2) 近40天 step_id × param_name 分布（识别 AOI TT 参数）
for fac, (table, tcol) in TABLES.items():
    run(
        f"""
        SELECT step_id, param_name, COUNT(*) AS cnt,
               MIN({tcol}) AS min_t, MAX({tcol}) AS max_t
        FROM {table}
        WHERE {tcol} >= '2026-07-01'
        GROUP BY step_id, param_name
        ORDER BY step_id, cnt DESC
        LIMIT 300
        """,
        f"step×param 分布（2026-07-01 起）: {table}",
    )

# 3) 样例行
for fac, (table, tcol) in TABLES.items():
    run(
        f"""
        SELECT * FROM {table}
        WHERE {tcol} >= '2026-08-01'
        LIMIT 5
        """,
        f"样例行: {table}",
    )

# 4) 规格表结构 + AOI 相关行
run(
    """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'mdw' AND table_name = 'dwd_imp_dv_param_spec'
    ORDER BY ordinal_position
    """,
    "列结构: mdw.dwd_imp_dv_param_spec",
)

run(
    """
    SELECT * FROM mdw.dwd_imp_dv_param_spec
    WHERE prod_code = 'M626'
    LIMIT 60
    """,
    "规格表样例: prod_code=M626",
)

# 5) 产品字典确认（RS 已知，抽样确认）
run(
    """
    SELECT productspecname, productcode
    FROM mdw.dwr_mes_productspec
    WHERE productcode IN ('M626','M673','M678','Z517','Z571')
    LIMIT 20
    """,
    "产品字典抽样",
)

# 6) 过货视图（分母）列确认——沿用 RS 结论，仅验证三视图存在
for view in ["spot_eda_array_view_sht_v", "spot_eda_oled_view_gls_v", "spot_eda_tp_view_gls_v"]:
    run(
        f"""
        SELECT COUNT(*) AS cnt FROM eda.{view}
        WHERE 1=0
        """,
        f"视图存在性: eda.{view}",
    )

md("\n_探查完成_")
flush()
print("done ->", OUT_PATH)
