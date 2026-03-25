# 🎯 Target File: tests/integration/diagnose_spc_table.py

import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text

# 将项目根目录加入 sys.path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from shared_kernel.infrastructure.db_handler import DatabaseManager

def run_diagnostics():
    print("="*50)
    print("🚀 启动 SPC 数据库探针...")
    print("="*50)
    
    db = DatabaseManager()
    engine = db.engine
    if not engine:
        print("❌ 数据库连接失败，请检查 .env")
        return

    with engine.connect() as conn:
        # 1. 打印当前连接的数据库信息 (隐藏密码)
        print(f"🔗 当前 Python 连接的数据库: {engine.url.render_as_string(hide_password=True)}")

        # 2. 全库无死角扫描该表到底在哪个 Schema 下
        print("\n🔍 正在全库扫描 'spc_tzbjx_array' 表...")
        query = text("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_name ILIKE '%spc_tzbjx_array%';
        """)
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print("\n❌ 结论：当前 Python 连接的数据库中【绝对不存在】这张表！")
            print("👉 请核对 .env 文件中的 DB_HOST, DB_NAME 是否与 DBeaver 的连接配置完全一致。")
            return
            
        print("\n✅ 结论：表存在！探针发现了以下物理表：")
        for _, row in df.iterrows():
            schema = row['table_schema']
            table = row['table_name']
            print(f"   -> Schema: [{schema}] | Table: [{table}]")
            
            # 3. 尝试带上 Schema 前缀去查询
            print(f"\n🧪 正在尝试通过带 Schema 的全名查询: {schema}.{table} ...")
            try:
                test_q = text(f"SELECT * FROM {schema}.{table} LIMIT 1")
                res = pd.read_sql(test_q, conn)
                print("✅ 查询成功！获取到的真实列名如下：")
                print(list(res.columns))
            except Exception as e:
                print(f"❌ 查询依然失败: {e}")

if __name__ == "__main__":
    run_diagnostics()