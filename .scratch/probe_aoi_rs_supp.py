# -*- coding: utf-8 -*-
"""AOI_RS 补充探查：TSP 过货视图定位、规格表语义细化、RS code 码表。

输出追加到 .scratch/probe_aoi_rs_result.md
"""
import sys
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from sqlalchemy import text

from src.shared_kernel.infrastructure.db_handler import DatabaseManager

OUT_PATH = Path(__file__).resolve().parent / "probe_aoi_rs_result.md"

engine = DatabaseManager().engine
out = []


def md(line=""):
    out.append(line)


def df_to_md(df, max_rows=60):
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
                s = s[:120] + "..."
            vals.append(s)
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def run(sql, timeout_ms=90000):
    try:
        with engine.connect() as conn:
            conn.execute(text(f"SET statement_timeout = '{timeout_ms}'"))
            result = conn.execute(text(sql))
            rows = result.fetchall()
            return pd.DataFrame(rows, columns=list(result.keys()))
    except Exception as e:
        md(f"> **SQL 报错**: `{type(e).__name__}: {str(e)[:300]}`")
        return None


def show(sql, note=None, max_rows=60, timeout_ms=90000):
    if note:
        md(note)
    md(f"```sql\n{sql}\n```")
    df = run(sql, timeout_ms=timeout_ms)
    md(df_to_md(df, max_rows))
    md()
    return df


md("\n---\n\n# 补充探查（第二轮）\n")

# ---- A. TSP 过货视图定位 ----
md("\n## 6. TSP 过货视图定位\n")
tsp_views = show(
    "SELECT table_schema, table_name, table_type FROM information_schema.tables "
    "WHERE table_name ILIKE '%tsp%' ORDER BY 1, 2 LIMIT 60",
    "**所有表名含 tsp 的表/视图：**",
)
# 挑选可能的过货视图
cand = None
if tsp_views is not None:
    for _, r in tsp_views.iterrows():
        n = str(r["table_name"]).lower()
        if "view" in n and "gls" in n:
            cand = (r["table_schema"], r["table_name"])
            break
    if cand is None:
        for _, r in tsp_views.iterrows():
            n = str(r["table_name"]).lower()
            if "view" in n or "spot" in n:
                cand = (r["table_schema"], r["table_name"])
                break
if cand:
    full = f"{cand[0]}.{cand[1]}"
    md(f"\n**选定 TSP 过货候选: `{full}`**\n")
    show(
        "SELECT ordinal_position, column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = '{cand[0]}' AND table_name = '{cand[1]}' ORDER BY ordinal_position",
        "**列清单：**",
    )
    show(f"SELECT * FROM {full} LIMIT 3", "**样例（LIMIT 3）：**")
    show(f"SELECT DISTINCT step_id FROM {full} ORDER BY 1", "step_id 全部 distinct 值：", timeout_ms=120000)
else:
    md("**未找到任何 TSP 过货视图候选。**")

# ---- B. 规格表语义细化 ----
md("\n## 7. 规格表 `mdw.dwd_imp_rs_code_xishu_fo_tzsbjx` 语义细化\n")
T = "mdw.dwd_imp_rs_code_xishu_fo_tzsbjx"
show(f"SELECT DISTINCT type_flag FROM {T} ORDER BY 1", "**type_flag 全部取值：**")
show(f"SELECT DISTINCT factory FROM {T} ORDER BY 1", "**factory 全部取值：**")
show(f"SELECT DISTINCT step_id FROM {T} ORDER BY 1", "**step_id 全部取值：**")
show(
    f"SELECT prod_code, factory, step_id, rs_code, type_flag, COUNT(*) AS cnt FROM {T} "
    "GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1 LIMIT 20",
    "**粒度校验：(prod_code, factory, step_id, rs_code, type_flag) 是否有重复（有重复说明粒度更细）：**",
)
show(
    f"SELECT type_flag, MIN(spec) AS min_v, MAX(spec) AS max_v, COUNT(DISTINCT spec) AS distinct_spec "
    f"FROM {T} GROUP BY type_flag ORDER BY 1",
    "**各 type_flag 下 spec 数值范围（判断语义：比例/张数/系数）：**",
)
show(
    f"SELECT DISTINCT prod_code, rs_code FROM {T} ORDER BY 1, 2 LIMIT 60",
    "**prod_code × rs_code 组合样例（判断规格是否按产品区分）：**",
)

# 对比表 mdw.dwd_imp_rs_code_xishu_fo_int
md("\n### 7.b 对比表 `mdw.dwd_imp_rs_code_xishu_fo_int`\n")
show(
    "SELECT ordinal_position, column_name, data_type FROM information_schema.columns "
    "WHERE table_schema = 'mdw' AND table_name = 'dwd_imp_rs_code_xishu_fo_int' ORDER BY ordinal_position",
    "**列清单：**",
)
show("SELECT * FROM mdw.dwd_imp_rs_code_xishu_fo_int LIMIT 5", "**样例（LIMIT 5）：**")
show("SELECT COUNT(*) AS cnt FROM mdw.dwd_imp_rs_code_xishu_fo_int", "**总行数：**")

# ---- C. RS code 名称码表 ----
md("\n## 8. RS Code 名称/描述码表（mdw 候选）\n")
for t in ["imp_tp_rs_code_remark", "imp_tp_rs_code_spec_new"]:
    md(f"\n### `mdw.{t}`\n")
    show(
        "SELECT ordinal_position, column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = 'mdw' AND table_name = '{t}' ORDER BY ordinal_position",
        "**列清单：**",
    )
    show(f"SELECT * FROM mdw.{t} LIMIT 5", "**样例（LIMIT 5）：**")
    show(f"SELECT COUNT(*) AS cnt FROM mdw.{t}", "**总行数：**")

with OUT_PATH.open("a", encoding="utf-8") as f:
    f.write("\n".join(out) + "\n")
print(f"补充探查完成，已追加到: {OUT_PATH}")
