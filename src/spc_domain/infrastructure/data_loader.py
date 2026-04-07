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
    from shared_kernel.infrastructure.db_handler import DatabaseManager

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


def load_valid_spc_params(
    db_manager: 'DatabaseManager', 
    prod_code: str,
    data_type_filter: str = 'ALL'
) -> Optional[pd.DataFrame]:
    """
    提取 IMP_SPC_TZBJX 表中所有数据，并按 data_type 分类：
    - SPC: data_type = 'SPC'
    - CTQ: data_type = 'CTQ'  
    - AOI: data_type 为 Null 或空字符串
    
    通过 DWR_MES_PRODUCTSPEC 关联，精准定位当前产品的白名单。
    
    Args:
        db_manager: 数据库管理器
        prod_code: 产品代码
        data_type_filter: 筛选类型: 'SPC', 'CTQ', 'AOI', 'ALL'(默认)
    """
    # [核心修改] 去掉 data_type = 'SPC' 筛选，提取所有类型数据
    # 使用 COALESCE 将 Null/空字符串统一标记为 'AOI'
    sql_query = f"""
    SELECT DISTINCT 
        T1.parmtername AS ref_param_name, 
        CASE 
            WHEN T1.data_type IS NULL OR TRIM(T1.data_type) = '' THEN 'AOI'
            ELSE UPPER(TRIM(T1.data_type))
        END AS data_type
    FROM eda.IMP_SPC_TZBJX T1
    JOIN DWR_MES_PRODUCTSPEC T2 ON T1.productspecname = T2.PRODUCTSPECNAME
    WHERE T2.PRODUCTCODE = '{prod_code}'
    """
    
    # 根据筛选条件添加额外过滤（在 Python 层处理，保持 SQL 简单）
    try:
        if db_manager.engine is None:
            raise ValueError("数据库引擎未初始化。")
        
        logging.info(f"[DAO] 开始提取参数映射表，产品: {prod_code}, 筛选类型: {data_type_filter}")
        df = pd.read_sql(text(sql_query), db_manager.engine)
        
        if not df.empty:
            # 清洗首尾空格，并统一转大写，防止后续 merge 时遭遇大小写暗坑
            df['ref_param_name'] = df['ref_param_name'].astype(str).str.strip().str.upper()
            
            # [核心修改] 根据 data_type_filter 进行内存筛选
            filter_upper = data_type_filter.upper() if data_type_filter else 'ALL'
            if filter_upper != 'ALL':
                before_count = len(df)
                df = df[df['data_type'] == filter_upper].copy()
                logging.info(f"[DAO] 内存筛选: {filter_upper}, 过滤前 {before_count} 条, 过滤后 {len(df)} 条")
            else:
                logging.info(f"[DAO] 返回所有类型参数，共 {len(df)} 条")
            
            return df
        
        logging.warning(f"[DAO] 产品 {prod_code} 未查询到任何参数映射数据")
        return pd.DataFrame()
        
    except Exception as e:
        logging.error(f"[DAO] 提取参数映射表失败: {e}")
        return None