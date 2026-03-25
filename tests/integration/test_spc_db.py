import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text
import pytest # 引入 pytest 用于断言和测试控制
from dotenv import load_dotenv

# 动态将项目根目录加入 sys.path，确保能够正常 import src 模块
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# =========================================================================
# [核心修复] 在导入 DatabaseManager 之前，强制把 .env 文件注入到环境变量中
# =========================================================================
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)

from shared_kernel.infrastructure.db_handler import DatabaseManager

def test_spc_database_queries():
    """
    [集成测试] 验证带 eda schema 和 产品字典表 Join 的最新 SPC SQL 逻辑。
    """
    print("\n" + "="*50)
    print("🚀 启动 SPC 数据库底层查询逻辑测试...")
    print("="*50)

    db = DatabaseManager()
    engine = db.engine
    
    assert engine is not None, "❌ 数据库引擎初始化失败，请检查 .env 配置。"

    # 同步更新测试用例中的元数据映射字典
    factory_meta = {
        'ARRAY': ('spc_tzbjx_array', 'sheet_id', 'sheet_start_time'),
        'OLED': ('spc_tzbjx_oled', 'glass_id', 'glass_start_time'),
        'TP': ('spc_tzbjx_tsp', 'glass_id', 'glass_start_time')
    }

    with engine.connect() as conn:
        for fac, (table_name, id_col, time_col) in factory_meta.items():
            print(f"\n🧪 正在测试 [{fac}] 厂区表 (eda.{table_name}) 的读取与 Join 翻译...")
            
            # 使用真实的时间列名，并强行 AS 成 sheet_start_time
            test_query = text(f"""
                SELECT 
                    '{fac}' AS factory,
                    P.PRODUCTCODE AS prod_code, 
                    T.{time_col} AS sheet_start_time, 
                    T.{id_col} AS sheet_id, 
                    T.step_id, 
                    T.param_name, 
                    T.param_value
                FROM eda.{table_name} T
                JOIN DWR_MES_PRODUCTSPEC P ON T.product_spec = P.PRODUCTSPECNAME
                LIMIT 5
            """)
            
            try:
                df = pd.read_sql(test_query, conn)
                
                print(f"✅ 查询成功！获取到 {len(df)} 条数据。")
                if not df.empty:
                    print(f"👉 数据预览 (前2行):\n{df.head(2).to_string()}")
                else:
                    print("⚠️ 表连接成功，但该表目前为空 (或没有能和字典表匹配上的数据)。")
                    
            except Exception as e:
                pytest.fail(f"❌ 查表 eda.{table_name} 失败！错误详情: {e}")