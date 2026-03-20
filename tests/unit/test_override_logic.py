# tests/test_override_logic.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from src.shared_kernel.config import ConfigLoader
from yield_domain.core.mwd_trend_processor import create_mwd_trend_data

# 1. 模拟输入数据 (Panel Level)
@pytest.fixture
def mock_panel_df():
    """
    构造一个包含 2025-10, 2025-11, 2025-12 数据的基础 DataFrame
    """
    # 生成日期序列 (每天 100 片)
    dates = pd.date_range(start='2025-10-01', end='2026-01-15', freq='D')
    
    data = []
    for date in dates:
        # 每天构造 100 行数据
        for i in range(100):
            data.append({
                'panel_id': f"PNL_{date.strftime('%Y%m%d')}_{i}",
                'warehousing_time': date.strftime('%Y%m%d'),
                # 构造一个必然存在的 Defect Group
                'defect_group': 'Array_Line',
                'defect_desc': 'S向亮线' if i < 5 else 'NoDefect' # 5% 自然不良率
            })
            
    return pd.DataFrame(data)

def test_integration_override_logic(mock_panel_df):
    """
    [集成测试] 验证从 create_mwd_trend_data 入口调用时，配置覆盖是否生效。
    这将检测：数据清洗 -> Shadow EMA -> 聚合 -> 覆盖 的全流程。
    """
    print("\n" + "="*60)
    print("🧪 开始全链路覆盖逻辑测试 (Test Pipeline)")
    print("="*60)

    # 1. 加载真实配置 (M678)
    # 我们直接读取硬盘上的 M678.yaml，确保配置源头是真实的
    try:
        config = ConfigLoader.load_config("M678")
        print("✅ 配置加载成功")
    except Exception as e:
        pytest.fail(f"配置加载失败: {e}")

    # 2. 验证配置中是否存在覆盖项
    override_val = config.processing.get('group_monthly_values', {}).get('Array_Line', {}).get('2025-10')
    print(f"📋 配置文件中 'Array_Line' 在 '2025-10' 的目标覆盖值: {override_val}")
    
    if override_val is None:
        pytest.fail("❌ 测试前提失败：配置文件 M678.yaml 中未找到 Array_Line 2025-10 的覆盖值！")

    # 3. 执行核心处理函数
    # 模拟 Service 层的调用方式
    resource_dir = ConfigLoader.get_project_root() / "resources"
    
    try:
        results = create_mwd_trend_data(
            panel_details_df=mock_panel_df,
            config=config,
            resource_dir=resource_dir,
            ema_span=7
        )
    except Exception as e:
        pytest.fail(f"❌ create_mwd_trend_data 执行崩溃: {e}")

    if results is None:
        pytest.fail("❌ create_mwd_trend_data 返回了 None (可能是数据量被过滤了)")

    # 4. 检查结果 (Monthly)
    df_monthly = results.get('monthly')
    if df_monthly is None or df_monthly.empty:
        pytest.fail("❌ 未生成月度数据")

    print("\n📊 计算结果快照 (Monthly):")
    # 打印出来人工看一眼
    print(df_monthly[['time_period', 'defect_group', 'defect_rate', 'total_panels']])

    # 5. 断言验证
    # 找到 2025-10月, Array_Line 的数据
    target_row = df_monthly[
        (df_monthly['time_period'] == '2025-10月') & 
        (df_monthly['defect_group'] == 'Array_Line')
    ]
    
    if target_row.empty:
        # 可能是日期格式化问题 (%Y-%m月 vs %Y-%m)
        print("⚠️ 未找到 '2025-10月' 的行，尝试模糊匹配...")
        print("当前所有的 time_period:", df_monthly['time_period'].unique())
        pytest.fail("❌ 结果中缺失 2025-10 数据行")

    actual_rate = target_row.iloc[0]['defect_rate']
    total_panels = target_row.iloc[0]['total_panels']
    
    # 覆盖逻辑是：Count = Rate_Override * Total
    # 所以反推 Rate 应该等于 Override 值 (允许微小误差)
    
    print(f"\n🔍 验证比对:")
    print(f"   - 目标值 (Config): {override_val}")
    print(f"   - 实际值 (Result): {actual_rate}")
    
    # 允许 0.1% 的误差 (因为 int 取整)
    assert abs(actual_rate - override_val) < 0.001, \
        f"❌ 覆盖失败！期望 {override_val}, 实际 {actual_rate}"

    print("\n🎉 全链路测试通过！说明 Core 层逻辑和配置读取都没有问题。")
    print("👉 如果前端依然显示不对，问题一定出在 Streamlit 缓存或图表绘制代码上。")

if __name__ == "__main__":
    # 允许直接运行脚本
    try:
        # 构造 mock 数据
        mock_df = pd.DataFrame([{
             'panel_id': f"P{i}", 
             'warehousing_time': '20251001', 
             'defect_group': 'Array_Line',
             'defect_desc': 'Test'
        } for i in range(100)])
        # ... 这里为了简便，直接用 pytest 运行即可
        print("请使用 'uv run pytest tests/test_override_logic.py -s' 运行此脚本以查看详细输出。")
    except Exception as e:
        print(e)