# -*- coding: utf-8 -*-
"""AOI_TT 补充探查：TT 参数识别规则（param_type IS NULL 假说验证）。"""
import sys
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from sqlalchemy import text

from src.shared_kernel.infrastructure.db_handler import DatabaseManager

OUT_PATH = Path(__file__).resolve().parent / "probe_aoi_tt_supp_result.md"
engine = DatabaseManager().engine
out = []


def flush():
    OUT_PATH.write_text("\n".join(out), encoding="utf-8")


def md(line=""):
    out.append(line)


def df_to_md(df, max_rows=100):
    if df is None or df.empty:
        return "_(0 行)_"
    df = df.head(max_rows)
    cols = [str(c) for c in df.columns]
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for _, row in df.iterrows():
        vals = [str(v).replace("|", "\\|")[:100] for v in row.values]
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def run(sql, label):
    md(f"\n## {label}\n")
    try:
        df = pd.read_sql(text(sql), engine)
        md(df_to_md(df))
        md(f"\n_({len(df)} 行)_")
    except Exception:
        md("```\n" + traceback.format_exc() + "\n```")
    flush()


md("# AOI_TT 补充探查")
flush()

# 1) 规格表中 param_type 分布
run(
    """
    SELECT param_type, COUNT(*) AS cnt
    FROM mdw.dwd_imp_dv_param_spec
    GROUP BY param_type ORDER BY cnt DESC
    """,
    "spec 表 param_type 分布",
)

# 2) param_type IS NULL 的 (prod, step, param) 组合
run(
    """
    SELECT prod_code, step_id, param_name, usl, ucl
    FROM mdw.dwd_imp_dv_param_spec
    WHERE param_type IS NULL
    ORDER BY prod_code, step_id
    """,
    "spec 表 param_type IS NULL 组合",
)

# 3) 测量表中近40天出现的 (step, param) 全量 distinct（三厂合并，不含大 count）
for fac, table, tcol in [
    ("ARRAY", "eda.spc_tzbjx_array", "sheet_start_time"),
    ("OLED", "eda.spc_tzbjx_oled", "glass_start_time"),
    ("TP", "eda.spc_tzbjx_tsp", "glass_start_time"),
]:
    run(
        f"""
        SELECT DISTINCT step_id, param_name
        FROM {table}
        WHERE {tcol} >= '2026-07-01'
          AND param_name ILIKE '%SUM%'
        ORDER BY step_id, param_name
        """,
        f"{fac} 名含 SUM 的参数",
    )

# 4) TDSUM/DSUM 行的 param_value 分布（确认是整数计数）
run(
    """
    SELECT 'array' AS fac, param_value, COUNT(*) FROM eda.spc_tzbjx_array
    WHERE sheet_start_time >= '2026-07-20' AND param_name = 'TDSUM'
    GROUP BY param_value ORDER BY param_value LIMIT 30
    """,
    "array TDSUM param_value 分布",
)
run(
    """
    SELECT param_name, param_value, COUNT(*) FROM eda.spc_tzbjx_oled
    WHERE glass_start_time >= '2026-07-20' AND param_name IN ('DSUM_L','DSUM_O')
    GROUP BY param_name, param_value ORDER BY param_name, param_value LIMIT 40
    """,
    "oled DSUM param_value 分布",
)

# 5) AOI 站点过货视图 step 覆盖确认（分母口径）
run(
    """
    SELECT step_id, COUNT(DISTINCT sheet_id) AS sheets
    FROM eda.spot_eda_array_view_sht_v
    WHERE sheet_start_time >= '2026-07-20' AND step_id IN ('11620','12620','13620','15620','18620')
    GROUP BY step_id
    """,
    "array 过货视图 AOI 站点覆盖",
)
run(
    """
    SELECT step_id, COUNT(DISTINCT glass_id) AS glasses
    FROM eda.spot_eda_oled_view_gls_v
    WHERE glass_start_time >= '2026-07-20' AND step_id IN ('21320')
    GROUP BY step_id
    """,
    "oled 过货视图 AOI 站点覆盖",
)
run(
    """
    SELECT step_id, COUNT(DISTINCT glass_id) AS glasses
    FROM eda.spot_eda_tp_view_gls_v
    WHERE glass_start_time >= '2026-07-20' AND step_id IN ('43620')
    GROUP BY step_id
    """,
    "tp 过货视图 AOI 站点覆盖",
)

md("\n_完成_")
flush()
print("done ->", OUT_PATH)
