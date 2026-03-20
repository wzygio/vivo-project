# tests/diagnose_config.py
import sys
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

# 1. 确保能导入 src 目录
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.append(str(project_root / "src"))

# 配置日志输出到控制台
logging.basicConfig(level=logging.INFO, format='%(message)s')

print(f"📂 项目根目录检测为: {project_root}")

try:
    from src.shared_kernel.config import ConfigLoader
    # 模拟 mwd_trend_processor 中的覆盖函数逻辑
    # 为了避免依赖过多，我们直接把该函数的逻辑复制过来测试，或者导入它
    from yield_domain.core.mwd_trend_processor import _apply_manual_overrides
except ImportError as e:
    print(f"❌ 导入失败，请检查环境: {e}")
    sys.exit(1)

def test_config_injection():
    print("\n" + "="*50)
    print("🕵️‍♂️ 开始配置加载与覆盖逻辑集成测试")
    print("="*50)

    # --- 步骤 1: 测试 ConfigLoader 加载 ---
    product_code = "M678"
    print(f"\n[Step 1] 尝试加载产品配置: {product_code}")
    
    try:
        config = ConfigLoader.load_config(product_code)
        print("✅ ConfigLoader 加载成功")
    except Exception as e:
        print(f"❌ ConfigLoader 加载崩溃: {e}")
        return

    # --- 步骤 2: 深度检查 processing 字典 ---
    print(f"\n[Step 2] 检查 config.processing 内容")
    proc_conf = config.processing
    
    # 检查 Key 是否存在
    override_key = 'group_monthly_values'
    if override_key not in proc_conf:
        print(f"❌ 严重问题: processing 中缺少 '{override_key}'！")
        print(f"   当前 processing 的 Keys: {list(proc_conf.keys())}")
        return
    else:
        print(f"✅ 找到 '{override_key}' 配置")

    # 打印具体值，检查解析结果
    monthly_vals = proc_conf[override_key]
    print(f"   数据快照 (Array_Line): {monthly_vals.get('Array_Line', '未找到')}")
    
    # 检查数据类型
    # YAML 中的 '2025-10' 有时会被解析为 date 对象，有时是 str，这很关键
    if 'Array_Line' in monthly_vals:
        sample_keys = list(monthly_vals['Array_Line'].keys())
        first_key = sample_keys[0]
        print(f"   Key 类型检查: '{first_key}' is type {type(first_key)}")
        if not isinstance(first_key, str):
            print("⚠️ 警告: YAML 中的日期 Key 没有被解析为字符串！这会导致匹配失败。")

    # --- 步骤 3: 模拟覆盖逻辑 (Mock Test) ---
    print(f"\n[Step 3] 模拟 _apply_manual_overrides 执行")
    
    # 构造一个模拟的 DataFrame
    # 假设有三个月的数据
    dates = [
        datetime(2025, 10, 31),
        datetime(2025, 11, 30),
        datetime(2025, 12, 31),
        datetime(2026, 1, 31)
    ]
    mock_df = pd.DataFrame({
        'total_panels': [1000, 1000, 1000, 1000],
        'Array_Line': [50, 50, 50, 50] # 初始 Rate 都是 5%
    }, index=dates)
    
    target_defects = ['Array_Line']
    
    print("   覆盖前数据 (Array_Line):")
    print(mock_df['Array_Line'].values)
    
    # 调用核心函数
    try:
        result_df = _apply_manual_overrides(
            mock_df, 
            monthly_vals, 
            target_defects, 
            'monthly'
        )
        print("✅ 函数执行完成")
        
        print("   覆盖后数据 (Array_Line):")
        print(result_df['Array_Line'].values)
        
        # 验证是否生效
        # 2025-10 配置为 0.0117 -> 1000 * 0.0117 = 11.7 -> round 12
        expected_val = 12 
        actual_val = result_df.iloc[0]['Array_Line']
        
        if actual_val == expected_val:
            print(f"\n🎉 测试通过！逻辑正确生效 (Expect {expected_val}, Got {actual_val})。")
        elif actual_val == 50:
            print(f"\n❌ 测试失败：数值未发生变化，覆盖未生效。")
            print("   可能原因：Key 格式不匹配 (如 YYYY-MM vs YYYY/MM)")
        else:
            print(f"\n⚠️ 测试存疑：数值变了但不对 (Expect {expected_val}, Got {actual_val})。")
            
    except Exception as e:
        print(f"❌ 覆盖逻辑执行崩溃: {e}")

if __name__ == "__main__":
    test_config_injection()