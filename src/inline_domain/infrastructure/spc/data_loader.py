import logging
import pandas as pd
import numpy as np
from sqlalchemy import text
from typing import TYPE_CHECKING, Optional
from pydantic import BaseModel, Field

from src.shared_kernel.utils.data_inspector import export_probed_details
# 仅在类型检查时导入，避免运行时产生循环依赖或强耦合 yield_domain
if TYPE_CHECKING:
    # 假设使用已存在的 DB Manager，实际传入的只要带有 .engine 属性的实例即可
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

# MT_CH_* parameters are governed by the whitelist data_type and must remain eligible for SPC.
SPC_EXCLUDED_PARAM_NAME_KEYWORDS = ("LOSS",)


def build_excluded_param_sql_predicates(column_expr: str) -> str:
    """Build SQL predicates that exclude SPC parameters before extraction."""
    return "\n".join(
        f"              AND UPPER({column_expr}) NOT LIKE '%{keyword}%'"
        for keyword in SPC_EXCLUDED_PARAM_NAME_KEYWORDS
    )


def filter_excluded_spc_param_names(df: pd.DataFrame) -> pd.DataFrame:
    """Remove SPC parameters that should never be displayed or calculated."""
    if df.empty or "param_name" not in df.columns:
        return df

    param_upper = df["param_name"].fillna("").astype(str).str.upper()
    excluded_mask = pd.Series(False, index=df.index)
    for keyword in SPC_EXCLUDED_PARAM_NAME_KEYWORDS:
        excluded_mask |= param_upper.str.contains(keyword, regex=False, na=False)

    if not excluded_mask.any():
        return df

    logging.info(
        "[SPC] 已屏蔽参数名包含 %s 的量测记录: %s -> %s",
        ", ".join(SPC_EXCLUDED_PARAM_NAME_KEYWORDS),
        len(df),
        len(df) - int(excluded_mask.sum()),
    )
    return df[~excluded_mask].copy()


# =============================================================================
#  涉及数据库表说明
# =============================================================================
# eda.spc_tzbjx_array  — ARRAY 厂 SPC 测量明细表（时序数据，每片玻璃每次测量的记录）
#                       主键: sheet_id + step_id + param_name + site_name
# eda.spc_tzbjx_oled   — OLED 厂 SPC 测量明细表（同上结构，ID 列为 glass_id）
# eda.spc_tzbjx_tsp    — TP 厂 SPC 测量明细表（同上结构，ID 列为 glass_id）
# eda.IMP_SPC_TZBJX    — SPC 参数大类表：参数名称，参数大类（元数据，定义当前产品哪些参数受控）
#                       列: parmtername(参数名), data_type(SPC/CTQ/空), productspecname
# dwd_imp_dv_param_spec — 管控规格基准表：参数名称，参数类型，规格线（定义每个参数的 USL/LSL/UCL/LCL）
# DWR_MES_PRODUCTSPEC  — 产品号字典表（productspecname → productcode 翻译）
# =============================================================================

class SpcQueryConfig(BaseModel):
    """SPC 报表查询的强类型配置模型，用于控制数据提取范围"""
    start_date: str = Field(..., description="开始日期, 格式 YYYY-MM-DD")
    end_date: str = Field(..., description="结束日期, 格式 YYYY-MM-DD")
    prod_code: str = Field(..., description="产品代码 (必须精确指定以避免全表扫)")
    factory: Optional[str] = Field(None, description="工厂分类 (如 ARRAY, OLED)")
    step_id: Optional[str] = Field(None, description="特定站点ID")
    param_name: Optional[str] = Field(None, description="特定参数名称")
    data_type_filter: Optional[str] = Field('SPC', description="数据类型筛选: SPC, CTQ, AOI, ALL")

def load_spc_measurements(
    db_manager: 'DatabaseManager', 
    start_str: str, 
    end_str: str,
    prod_code: str
) -> pd.DataFrame:
    """
    [纯粹的数据访问对象 DAO]
    处理多厂别分表逻辑，解决 sheet_id/glass_id 及时间戳列名不一致的问题，
    并关联 MES 字典表翻译产品名称。
    """
    logging.info(f"==> [DAO] 开始从底层数据库 (eda 模式) 抽取产品 {prod_code} 的 SPC 数据...")
    
    start_time_fmt = f"{start_str} 00:00:00"
    end_time_fmt = f"{end_str} 23:59:59"

    # [架构升级] 三元组映射字典：(物理表名, ID列名, 时间戳列名)
    factory_meta = {
        'ARRAY': ('spc_tzbjx_array', 'sheet_id', 'sheet_start_time'),
        'OLED': ('spc_tzbjx_oled', 'glass_id', 'glass_start_time'),
        'TP': ('spc_tzbjx_tsp', 'glass_id', 'glass_start_time')
    }

    sql_queries = []
    excluded_param_predicates = build_excluded_param_sql_predicates("T.param_name")
    
    # 动态构建包含 Schema 路由、列名抹平、和字典表 JOIN 的大一统 SQL
    # [优化] 使用窗口函数在SQL层完成去重：每组保留最新 sheet_start_time 的记录
    group_columns = ['factory', 'prod_code', 'sheet_id', 'step_id', 'param_name', 'site_name']
    
    for fac, (table_name, id_col, time_col) in factory_meta.items():
        q = f"""
        SELECT 
            factory,
            prod_code,
            sheet_start_time,
                sheet_id,
                step_id,
                param_name,
                site_name,
                unit_id,
                param_value
        FROM (
            SELECT 
                '{fac}' AS factory,
                P.PRODUCTCODE AS prod_code, 
                T.{time_col} AS sheet_start_time, 
                T.{id_col} AS sheet_id, 
                T.step_id, 
                T.param_name,
                T.site_name,
                T.unit_id,
                T.param_value,
                ROW_NUMBER() OVER (
                    PARTITION BY P.PRODUCTCODE, T.{id_col}, T.step_id, T.param_name, T.site_name 
                    ORDER BY T.{time_col} DESC
                ) as rn
            FROM eda.{table_name} T
            JOIN DWR_MES_PRODUCTSPEC P ON T.product_spec = P.PRODUCTSPECNAME
            WHERE T.{time_col} >= '{start_time_fmt}' 
              AND T.{time_col} <= '{end_time_fmt}' 
              AND P.PRODUCTCODE = '{prod_code}'
{excluded_param_predicates}
        ) t
        WHERE rn = 1
        """
        sql_queries.append(q)

    final_sql_query = " UNION ALL ".join(sql_queries)

    try:
        if db_manager.engine is None:
            raise ValueError("数据库引擎未初始化。")

        logging.info("执行大一统多厂别 (带字典表翻译与多态映射) 的 UNION SPC SQL 查询...")
        measure_df = pd.read_sql(text(final_sql_query), db_manager.engine)
        measure_df.columns = measure_df.columns.str.lower() 
        
        if not measure_df.empty:
            measure_df['param_value'] = pd.to_numeric(measure_df['param_value'], errors='coerce') 
            measure_df = measure_df.dropna(subset=['param_value']) 
            measure_df = filter_excluded_spc_param_names(measure_df)

        # ==============================================================
        # 🚨 [通用探针] 检查刚执行完的 SQL 真实提取了多少条记录
        # ==============================================================
        export_probed_details(measure_df, "01_DAO层-SQL真实返回")
        
        logging.info(f"[DAO] 成功提取并清洗 {len(measure_df)} 条底层大宽表数据。")
        return measure_df
        
    except Exception as e:
        logging.error(f"[DAO] 提取 SPC 底层量测数据失败: {e}")
        return pd.DataFrame()
    
def load_spc_spec_limits(
    db_manager: 'DatabaseManager', 
    prod_code: str
) -> pd.DataFrame:
    """
    提取产品的管控规则与规格界限，执行数值清洗。
    [BugFix] 严格依据 'prod_code', 'step_id', 'param_name' 三者拉取数据。
    """
    logging.info(f"开始提取产品 {prod_code} 的管控规格基准数据...")

    # [BugFix] 移除 main_eqp_type 字段的查询，确保返回的粒度是严格的 (产品+站点+参数) 级别，防止合并出多余记录
    sql_query = f"""
    SELECT 
        prod_code, 
        step_id, 
        param_name, 
        usl, 
        lsl, 
        ucl, 
        lcl 
    FROM dwd_imp_dv_param_spec 
    WHERE prod_code = '{prod_code}'
    """

    try:
        if db_manager.engine is None:
            raise ValueError("数据库引擎未初始化。")

        logging.info("执行管控规格基准 SQL 查询...")
        spec_df = pd.read_sql(text(sql_query), db_manager.engine)
        spec_df.columns = spec_df.columns.str.lower()
        
        if not spec_df.empty:
            limit_cols = ['usl', 'lsl', 'ucl', 'lcl']
            # 防呆处理：遍历规则列，确保数据库中的 NULL 值正确映射为 Pandas np.nan 类型
            for col in limit_cols:
                if col in spec_df.columns:
                    spec_df[col] = pd.to_numeric(spec_df[col], errors='coerce') 

        logging.info(f"成功提取 {len(spec_df)} 条管控规格规则。")
        return spec_df
        
    except Exception as e:
        logging.error(f"提取管控规格基准数据失败: {e}")
        return pd.DataFrame()


def load_param_whitelist(
    db_manager: 'DatabaseManager', 
    prod_code: str
) -> Optional[pd.DataFrame]:
    """
    [纯 DAO] 提取 IMP_SPC_TZBJX 表中当前产品的参数白名单（裸查询，无分类、无筛选）。

    通过 DWR_MES_PRODUCTSPEC 关联，精确定位当前产品的白名单。
    返回列: ref_param_name (参数名), data_type (DB 原始值，未经分类映射)

    Args:
        db_manager: 数据库管理器
        prod_code: 产品代码

    Returns:
        DataFrame with columns [ref_param_name, data_type]，失败返回 None
    """
    excluded_param_predicates = build_excluded_param_sql_predicates("T1.parmtername")
    sql_query = f"""
    SELECT DISTINCT 
        T1.parmtername AS ref_param_name, 
        T1.data_type
    FROM eda.IMP_SPC_TZBJX T1
    JOIN DWR_MES_PRODUCTSPEC T2 ON T1.productspecname = T2.PRODUCTSPECNAME
    WHERE T2.PRODUCTCODE = '{prod_code}'
{excluded_param_predicates}
    """

    try:
        if db_manager.engine is None:
            raise ValueError("数据库引擎未初始化。")

        logging.info(f"[DAO] 提取参数白名单 (裸查询)，产品: {prod_code}")
        df = pd.read_sql(text(sql_query), db_manager.engine)

        if not df.empty:
            df["ref_param_name"] = df["ref_param_name"].astype(str).str.strip().str.upper()
            logging.info(f"[DAO] 产品 {prod_code} 白名单: {len(df)} 个参数")
            return df

        logging.warning(f"[DAO] 产品 {prod_code} 未查询到任何参数映射数据")
        return pd.DataFrame()

    except Exception as e:
        logging.error(f"[DAO] 提取参数白名单失败: {e}")
        return None
