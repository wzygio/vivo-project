# -*- coding: utf-8 -*-
"""AOI_RS 报表数据源探查脚本。

用法（Git Bash，项目根目录下）：
    .venv/Scripts/python.exe .scratch/probe_aoi_rs.py

输出：.scratch/probe_aoi_rs_result.md
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

OUT_PATH = Path(__file__).resolve().parent / "probe_aoi_rs_result.md"

engine = DatabaseManager().engine

out = []


def flush():
    """增量落盘，防止长查询中途被杀死导致全部进度丢失。"""
    OUT_PATH.write_text("\n".join(out), encoding="utf-8")


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
    """执行 SQL 返回 DataFrame；失败时记录异常并返回 None。"""
    try:
        with engine.connect() as conn:
            conn.execute(text(f"SET statement_timeout = '{timeout_ms}'"))
            result = conn.execute(text(sql))
            rows = result.fetchall()
            return pd.DataFrame(rows, columns=list(result.keys()))
    except Exception as e:
        md(f"> **SQL 报错**: `{type(e).__name__}: {str(e)[:300]}`")
        md(f"> SQL: `{sql[:300]}`")
        return None


def section(title, fn):
    md(f"\n## {title}\n")
    try:
        fn()
    except Exception:
        md("```\n" + traceback.format_exc()[-1500:] + "\n```")
    flush()


def show(sql, note=None, max_rows=60, timeout_ms=90000):
    if note:
        md(note)
    md(f"```sql\n{sql}\n```")
    df = run(sql, timeout_ms=timeout_ms)
    md(df_to_md(df, max_rows))
    md()
    return df


def get_columns(schema, table):
    return run(
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position"
    )


def find_col(cols_df, candidates):
    """在列清单中按候选名（小写）找第一个存在的列名。"""
    if cols_df is None:
        return None
    names = [str(c).lower() for c in cols_df["column_name"]]
    for cand in candidates:
        if cand in names:
            return cand
    # 模糊包含
    for cand in candidates:
        for n in names:
            if cand in n:
                return n
    return None


def table_exists(schema, table):
    df = run(
        "SELECT 1 FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    )
    return df is not None and not df.empty


# ============================================================
# 0. 环境
# ============================================================
md(f"# AOI_RS 数据库探查结果\n")
md(f"- 生成时间: {datetime.now().isoformat(timespec='seconds')}")
if engine is None:
    md("\n**ENGINE 创建失败，终止探查。**")
    OUT_PATH.write_text("\n".join(out), encoding="utf-8")
    sys.exit(1)
ver = run("SELECT version() AS v")
md(f"- 数据库版本: `{ver.iloc[0, 0] if ver is not None and not ver.empty else '未知'}`")


# ============================================================
# 1. RS Code 明细表
# ============================================================
RS_TABLES = [
    ("eda", "spc_tzbjx_rs_array"),
    ("eda", "spc_tzbjx_rs_oled"),
    ("eda", "spc_tzbjx_rs_tsp"),
]


def probe_rs_table(schema, table):
    full = f"{schema}.{table}"
    md(f"\n### 1.x `{full}`\n")
    if not table_exists(schema, table):
        md(f"**表 `{full}` 不存在，模糊搜索相近表名：**")
        kw = table.split("_")[-1]
        show(
            "SELECT table_schema, table_name FROM information_schema.tables "
            f"WHERE table_name ILIKE '%{kw}%' OR table_name ILIKE '%rs%' "
            "ORDER BY 1, 2 LIMIT 50"
        )
        return

    md("**列清单（information_schema.columns）：**")
    cols_df = show(
        "SELECT ordinal_position, column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position"
    )

    md("**样例（LIMIT 5）：**")
    show(f"SELECT * FROM {full} LIMIT 5")

    id_col = find_col(cols_df, ["sheet_id", "glass_id", "sheetid", "glassid"])
    time_col = find_col(
        cols_df,
        ["sheet_start_time", "glass_start_time", "start_time", "pass_time", "event_time", "create_time"],
    )
    md(f"- 识别到 ID 字段: `{id_col}`；过货时间字段: `{time_col}`\n")

    # code_qty 是否恒为 1（基于 10 万行采样）
    if find_col(cols_df, ["code_qty"]):
        show(
            f"SELECT code_qty, COUNT(*) AS cnt FROM (SELECT code_qty FROM {full} LIMIT 100000) s "
            "GROUP BY code_qty ORDER BY cnt DESC",
            "code_qty 取值分布（10 万行采样）：",
        )
    else:
        md("**未找到 code_qty 字段。**\n")

    # lot 字段
    if cols_df is not None:
        lot_cols = [c for c in cols_df["column_name"] if "lot" in str(c).lower()]
        if lot_cols:
            md(f"**存在 lot 相关字段: {lot_cols}**")
            show(f"SELECT DISTINCT {lot_cols[0]} FROM {full} LIMIT 10", f"{lot_cols[0]} 样例：")
        else:
            md("**未发现任何 lot 相关字段。** 以下为 ID 字段样例（供判断是否可从中提取 lot）：")
            if id_col:
                show(
                    f"SELECT DISTINCT {id_col} FROM (SELECT {id_col} FROM {full} LIMIT 5000) s LIMIT 10",
                    f"{id_col} 的 distinct 样例（5000 行采样取 10 个）：",
                )
        md()

    # rs_code 样例
    rs_col = find_col(cols_df, ["rs_code"])
    if rs_col:
        show(
            f"SELECT DISTINCT {rs_col} FROM (SELECT {rs_col} FROM {full} LIMIT 100000) s LIMIT 20",
            f"{rs_col} distinct 样例（10 万行采样取 20 个，用于判断是否五位代码）：",
        )
    else:
        md("**未找到 rs_code 字段。**\n")

    # step_id distinct
    step_col = find_col(cols_df, ["step_id"])
    if step_col:
        show(f"SELECT DISTINCT {step_col} FROM {full} ORDER BY 1", "step_id 全部 distinct 值：", timeout_ms=120000)
    else:
        md("**未找到 step_id 字段。**\n")

    # 最近一个月行数
    if time_col:
        show(
            f"SELECT COUNT(*) AS cnt_last_month FROM {full} "
            f"WHERE {time_col} >= now() - interval '1 month'",
            f"最近一个月行数（按 `{time_col}` 过滤）：",
            timeout_ms=120000,
        )
        show(
            f"SELECT MIN({time_col}) AS min_t, MAX({time_col}) AS max_t FROM {full}",
            f"`{time_col}` 时间范围：",
            timeout_ms=120000,
        )

    # productcode 样例
    pc_col = find_col(cols_df, ["productcode", "product_code", "product"])
    if pc_col:
        show(
            f"SELECT DISTINCT {pc_col} FROM (SELECT {pc_col} FROM {full} LIMIT 100000) s LIMIT 10",
            f"{pc_col} distinct 样例（10 个）：",
        )
    else:
        md("**未找到 productcode 字段。**\n")


for schema, table in RS_TABLES:
    section(f"1. RS 明细表 `{schema}.{table}`", lambda s=schema, t=table: probe_rs_table(s, t))


# ============================================================
# 2. 过货明细视图
# ============================================================
VIEWS = [
    ("eda", "spot_eda_array_view_sht_v"),
    ("eda", "spot_eda_oled_view_gls_v"),
    ("eda", "spot_eda_tsp_view_gls_v"),
]


def probe_view(schema, view):
    full = f"{schema}.{view}"
    md(f"\n### `{full}`\n")
    if not table_exists(schema, view):
        md(f"**视图 `{full}` 不存在，模糊搜索：**")
        show(
            "SELECT table_schema, table_name FROM information_schema.tables "
            f"WHERE table_name ILIKE '%{view[:18]}%' ORDER BY 1, 2 LIMIT 30"
        )
        return

    md("**列清单：**")
    cols_df = show(
        "SELECT ordinal_position, column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{view}' ORDER BY ordinal_position"
    )

    md("**样例（LIMIT 3）：**")
    show(f"SELECT * FROM {full} LIMIT 3")

    id_col = find_col(cols_df, ["sheet_id", "glass_id"])
    time_col = find_col(cols_df, ["sheet_start_time", "glass_start_time", "start_time"])
    md(f"- 识别到 ID 字段: `{id_col}`；时间字段: `{time_col}`；"
       f"step_id: `{find_col(cols_df, ['step_id'])}`；product_spec: `{find_col(cols_df, ['product_spec', 'productspec'])}`\n")

    step_col = find_col(cols_df, ["step_id"])
    if step_col:
        show(f"SELECT DISTINCT {step_col} FROM {full} ORDER BY 1", "step_id 全部 distinct 值：", timeout_ms=120000)


for schema, view in VIEWS:
    section(f"2. 过货视图 `{schema}.{view}`", lambda s=schema, v=view: probe_view(s, v))


# ============================================================
# 3. 产品字典
# ============================================================
def probe_productspec():
    full = "mdw.dwr_mes_productspec"
    if not table_exists("mdw", "dwr_mes_productspec"):
        md(f"**表 `{full}` 不存在，模糊搜索：**")
        show(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_name ILIKE '%productspec%' OR table_name ILIKE '%product_spec%' "
            "ORDER BY 1, 2 LIMIT 30"
        )
        return
    cols_df = show(
        "SELECT ordinal_position, column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'mdw' AND table_name = 'dwr_mes_productspec' ORDER BY ordinal_position",
        "**列清单：**",
    )
    show(f"SELECT * FROM {full} LIMIT 5", "**样例（LIMIT 5）：**")
    if cols_df is not None:
        names = [str(c).lower() for c in cols_df["column_name"]]
        spec_col = next((n for n in names if "specname" in n or "spec_name" in n), None)
        code_col = next((n for n in names if "productcode" in n or "product_code" in n), None)
        md(f"- productspecname 候选字段: `{spec_col}`；productcode 候选字段: `{code_col}`\n")
        if spec_col and code_col:
            show(
                f"SELECT DISTINCT {spec_col}, {code_col} FROM {full} LIMIT 20",
                f"映射样例 `{spec_col}` → `{code_col}`：",
            )


section("3. 产品字典 `mdw.dwr_mes_productspec`", probe_productspec)


# ============================================================
# 4. RS 规格表
# ============================================================
def probe_spec():
    md("**定位规格表（rs_code / xishu 模糊搜索）：**")
    loc = show(
        "SELECT table_schema, table_name, table_type FROM information_schema.tables "
        "WHERE table_name ILIKE '%rs_code%' OR table_name ILIKE '%xishu%' ORDER BY 1, 2"
    )
    if loc is None or loc.empty:
        md("**未找到任何匹配表。**")
        return

    target_schema, target_table = None, None
    for _, r in loc.iterrows():
        if "xishu" in str(r["table_name"]).lower() and "tzsbjx" in str(r["table_name"]).lower():
            target_schema, target_table = r["table_schema"], r["table_name"]
            break
    if target_table is None:
        target_schema, target_table = loc.iloc[0]["table_schema"], loc.iloc[0]["table_name"]
    full = f"{target_schema}.{target_table}"
    md(f"\n**选定目标表: `{full}`**\n")

    cols_df = show(
        "SELECT ordinal_position, column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = '{target_schema}' AND table_name = '{target_table}' ORDER BY ordinal_position",
        "**列清单：**",
    )
    show(f"SELECT * FROM {full} LIMIT 5", "**样例（LIMIT 5）：**")
    show(f"SELECT COUNT(*) AS total_rows FROM {full}", "**总行数：**")

    if cols_df is not None:
        for c in cols_df["column_name"]:
            cl = str(c).lower()
            if "code" in cl:
                show(
                    f"SELECT DISTINCT {c} FROM {full} ORDER BY 1 LIMIT 50",
                    f"code 类字段 `{c}` 全部 distinct 值（≤50）：",
                )
        # 数值列分布，辅助判断是限值还是系数
        num_cols = [c for c, t in zip(cols_df["column_name"], cols_df["data_type"])
                    if str(t).lower() in ("numeric", "integer", "bigint", "double precision", "real", "smallint")]
        for c in num_cols[:6]:
            show(
                f"SELECT MIN({c}) AS min_v, MAX({c}) AS max_v, AVG({c}) AS avg_v, COUNT(DISTINCT {c}) AS distinct_cnt FROM {full}",
                f"数值字段 `{c}` 分布（辅助判断限值 vs 系数）：",
            )


section("4. RS 规格表 `dwd_imp_rs_code_xishu_fo_tzsbjx`", probe_spec)


# ============================================================
# 5. RS Code 名称/描述码表搜索
# ============================================================
def probe_codename():
    md("**搜索表名含 rs/code 的表：**")
    cands = show(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_name ILIKE '%rs%' OR table_name ILIKE '%code%' "
        "ORDER BY 1, 2 LIMIT 100",
        max_rows=100,
    )
    if cands is None or cands.empty:
        md("未找到候选表。")
        return

    md("\n**筛选其中含 name/desc 类字段的表：**\n")
    hits = []
    for _, r in cands.iterrows():
        s, t = r["table_schema"], r["table_name"]
        cols = run(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{s}' AND table_name = '{t}' "
            "AND (column_name ILIKE '%name%' OR column_name ILIKE '%desc%')"
        )
        if cols is not None and not cols.empty:
            hits.append((s, t, list(cols["column_name"])))
    if not hits:
        md("（无匹配）")
        return
    md("| schema | table | name/desc 字段 |")
    md("|---|---|---|")
    for s, t, cs in hits:
        md(f"| {s} | {t} | {', '.join(map(str, cs))} |")
    md()

    # 优先抽 rs 相关的表取样
    rs_hits = [h for h in hits if "rs" in h[1].lower()] or hits
    for s, t, cs in rs_hits[:5]:
        show(f"SELECT * FROM {s}.{t} LIMIT 5", f"候选码表 `{s}.{t}` 样例：")


section("5. RS Code 名称/中文描述码表搜索", probe_codename)


OUT_PATH.write_text("\n".join(out), encoding="utf-8")
print(f"探查完成，结果写入: {OUT_PATH}")
