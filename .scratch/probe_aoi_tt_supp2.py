# -*- coding: utf-8 -*-
"""AOI_TT 补充探查2：趋势图分母口径（过货视图无 AOI 站点记录的替代方案）。"""
import sys
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from sqlalchemy import text

from src.shared_kernel.infrastructure.db_handler import DatabaseManager

OUT_PATH = Path(__file__).resolve().parent / "probe_aoi_tt_supp2_result.md"
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


md("# AOI_TT 补充探查2 — 分母口径")
flush()

# 1) 过货视图近20天出现的全部 step_id（看是否有 AOI 相邻站点）
for fac, view, idcol, tcol in [
    ("ARRAY", "eda.spot_eda_array_view_sht_v", "sheet_id", "sheet_start_time"),
    ("OLED", "eda.spot_eda_oled_view_gls_v", "glass_id", "glass_start_time"),
    ("TP", "eda.spot_eda_tp_view_gls_v", "glass_id", "glass_start_time"),
]:
    run(
        f"""
        SELECT step_id, COUNT(DISTINCT {idcol}) AS ids
        FROM {view}
        WHERE {tcol} >= '2026-07-20'
        GROUP BY step_id ORDER BY step_id
        """,
        f"{fac} 过货视图 step 分布",
    )

# 2) TDSUM 每 sheet 行数（验证"每片一条"假说）
run(
    """
    SELECT rows_per_sheet, COUNT(*) AS sheets FROM (
        SELECT sheet_id, COUNT(*) AS rows_per_sheet
        FROM eda.spc_tzbjx_array
        WHERE sheet_start_time >= '2026-07-20' AND param_name = 'TDSUM'
        GROUP BY sheet_id
    ) t GROUP BY rows_per_sheet ORDER BY rows_per_sheet
    """,
    "array TDSUM 每 sheet 行数分布",
)
run(
    """
    SELECT param_name, rows_per_glass, COUNT(*) AS glasses FROM (
        SELECT glass_id, param_name, COUNT(*) AS rows_per_glass
        FROM eda.spc_tzbjx_oled
        WHERE glass_start_time >= '2026-07-20' AND param_name IN ('DSUM_L','DSUM_O')
        GROUP BY glass_id, param_name
    ) t GROUP BY param_name, rows_per_glass ORDER BY param_name, rows_per_glass
    """,
    "oled DSUM 每 glass 行数分布",
)

# 3) TDSUM distinct sheet 与 RS 站点过货量对比（同产品同日口径粗校验）
run(
    """
    SELECT DATE(sheet_start_time) AS d, COUNT(*) AS rows, COUNT(DISTINCT sheet_id) AS sheets
    FROM eda.spc_tzbjx_array
    WHERE sheet_start_time >= '2026-08-01' AND param_name = 'TDSUM'
    GROUP BY 1 ORDER BY 1
    """,
    "array TDSUM 每日行数 vs distinct sheet",
)

# 4) RS 站点(xx629)在过货视图中的量级（若改用 RS 站点过货作分母的备选）
run(
    """
    SELECT step_id, COUNT(DISTINCT sheet_id) AS sheets
    FROM eda.spot_eda_array_view_sht_v
    WHERE sheet_start_time >= '2026-08-01' AND step_id IN ('11629','12629','13629','15629','18629')
    GROUP BY step_id ORDER BY step_id
    """,
    "array 过货视图 RS 站点覆盖（备选分母）",
)

md("\n_完成_")
flush()
print("done ->", OUT_PATH)
