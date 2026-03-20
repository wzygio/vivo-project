# tests/diagnose_new_product.py
import sys
import logging
from pathlib import Path
import pandas as pd

# 引入核心组件
from config import ConfigLoader
from yield_domain.utils.session_manager import SessionManager
from yield_domain.application.yield_service import YieldAnalysisService

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(message)s')

def test_new_product_integration(target_product: str = "C472"):
    print("\n" + "="*60)
    print(f"🚀 新产品集成诊断: {target_product}")
    print("="*60)

    # 1. 检查 SessionManager 注册情况
    print(f"\n[Step 1] 检查 SessionManager 注册列表...")
    registered_products = SessionManager.AVAILABLE_PRODUCTS
    if target_product not in registered_products:
        print(f"❌ 失败: '{target_product}' 未在 SessionManager.AVAILABLE_PRODUCTS 中注册！")
        print(f"   当前列表: {registered_products}")
        print("   -> 请修改 src/vivo_project/utils/session_manager.py 添加该产品。")
        return
    else:
        print(f"✅ SessionManager 已注册: {registered_products}")

    # 2. 检查配置文件是否存在
    print(f"\n[Step 2] 检查 YAML 配置文件...")
    root = ConfigLoader.get_project_root()
    yaml_path = root / "config" / "products" / f"{target_product}.yaml"
    
    if not yaml_path.exists():
        print(f"❌ 失败: 配置文件未找到！期望路径: {yaml_path}")
        print("   -> 请确保文件名为 C472.yaml (区分大小写) 且位于 config/products/ 目录下。")
        return
    else:
        print(f"✅ 配置文件存在: {yaml_path}")

    # 3. 加载配置
    print(f"\n[Step 3] 尝试加载配置...")
    try:
        config = ConfigLoader.load_config(target_product)
        print(f"   - Config Loaded: {config.data_source.product_code}")
        
        # [新增] 检查快照路径是否隔离
        snap_path = config.processing.get('snapshot_path', '')
        print(f"   - Snapshot Path: {snap_path}")
        if target_product not in snap_path:
            print(f"⚠️ 警告: 快照文件名 '{snap_path}' 似乎不包含产品名 '{target_product}'。")
            print("   -> 这可能导致不同产品共用同一个缓存文件，出现数据串味！")
            
    except Exception as e:
        print(f"❌ 配置加载崩溃: {e}")
        return

    # 4. 测试 Service 数据
    print(f"\n[Step 4] 测试 Service 层数据连通性...")
    try:
        panel_df = YieldAnalysisService.get_modified_panel_details(config)
        
        if panel_df.empty:
            print(f"❌ 数据查询返回空 DataFrame！请检查数据库。")
        else:
            print(f"✅ 成功查询到 {len(panel_df)} 行数据。")
            
            # [修改] 安全地打印数据，不假设 product_code 列存在
            cols_to_show = [c for c in ['panel_id', 'prod_code', 'defect_group'] if c in panel_df.columns]
            print(f"   数据样例 (Columns: {cols_to_show}):")
            print(panel_df[cols_to_show].head(2))

            # [新增] 数据量合理性检查
            if len(panel_df) > 4000000 and target_product == "C472":
                print(f"\n⚠️ 严重警告: C472 的数据量 ({len(panel_df)}) 与 M678 惊人地相似！")
                print("   -> 极大概率是读取了 M678 的旧快照文件。请务必检查 Step 3 的 Snapshot Path。")
                print("   -> 建议删除 data/ 目录下的 .parquet 文件后重试。")
    except Exception as e:
        print(f"❌ Service 查询崩溃: {e}")

if __name__ == "__main__":
    # 您可以在这里修改要测试的产品型号
    test_new_product_integration("C472")