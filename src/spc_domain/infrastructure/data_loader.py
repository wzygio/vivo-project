import logging
import pandas as pd
import numpy as np
from sqlalchemy import text
from typing import TYPE_CHECKING, Optional
from pydantic import BaseModel, Field

# 仅在类型检查时导入，避免运行时产生循环依赖或强耦合 yield_domain
if TYPE_CHECKING:
    # 假设使用已存在的 DB Manager，实际传入的只要带有 .engine 属性的实例即可
    from yield_domain.infrastructure.db_handler import DatabaseManager

class SpcQueryConfig(BaseModel):
    """SPC 报表查询的强类型配置模型，用于控制数据提取范围"""
    start_date: str = Field(..., description="开始日期, 格式 YYYY-MM-DD")
    end_date: str = Field(..., description="结束日期, 格式 YYYY-MM-DD")
    prod_code: str = Field(..., description="产品代码 (必须精确指定以避免全表扫)")
    factory: Optional[str] = Field(None, description="工厂分类 (如 ARRAY, OLED)")
    step_id: Optional[str] = Field(None, description="特定站点ID")
    param_name: Optional[str] = Field(None, description="特定参数名称")

def load_spc_measurements(
    db_manager: 'DatabaseManager', 
    start_str: str, # [架构调整] 剥离 config 强耦合，直接接收标量参数
    end_str: str,
    prod_code: str
) -> pd.DataFrame:
    """
    [纯粹的数据访问对象 DAO]
    无视具体的站点/参数过滤要求，强制全量拉取该产品在指定时间段内的三厂全部原始量测数据。
    """
    logging.info(f"==> [DAO] 开始从底层数据库全量抽取产品 {prod_code} 的 SPC 数据 ({start_str} 至 {end_str})...")
    
    start_time_fmt = f"{start_str} 00:00:00"
    end_time_fmt = f"{end_str} 23:59:59"

    factory_meta = {
        'ARRAY': ('SPC_TZBJX_ARRAY', 'sheet_id'),
        'OLED': ('SPC_TZBJX_OLED', 'glass_id'),
        'TP': ('SPC_TZBJX_TSP', 'glass_id')
    }

    sql_queries = []
    
    # 无差别构建全量 UNION ALL 语句
    for fac, (table_name, id_col) in factory_meta.items():
        q = f"""
        SELECT 
            '{fac}' AS factory,
            prod_code, 
            sheet_start_time, 
            {id_col} AS sheet_id, 
            step_id, 
            param_name, 
            param_value
        FROM {table_name}
        WHERE sheet_start_time >= '{start_time_fmt}' 
          AND sheet_start_time <= '{end_time_fmt}' 
          AND prod_code = '{prod_code}'
        """
        sql_queries.append(q)

    final_sql_query = " UNION ALL ".join(sql_queries)

    try:
        if db_manager.engine is None:
            raise ValueError("数据库引擎未初始化。")

        logging.info("执行大一统多厂别 UNION SPC 明细 SQL 查询...")
        measure_df = pd.read_sql(text(final_sql_query), db_manager.engine)
        measure_df.columns = measure_df.columns.str.lower() 
        
        if not measure_df.empty:
            measure_df['param_value'] = pd.to_numeric(measure_df['param_value'], errors='coerce') 
            measure_df = measure_df.dropna(subset=['param_value']) 

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