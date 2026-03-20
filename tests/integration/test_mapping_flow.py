# tests/diagnose_mapping_deep_dive.py
import pandas as pd
import sys
import logging
from config import ConfigLoader
from yield_domain.application.yield_service import YieldAnalysisService

# 为了测试私有方法，我们需要一些 Hack
from yield_domain.core.mapping_processor import (
    prepare_mapping_data, 
    _parse_panel_id_to_coords
)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(message)s')

def test_deep_dive():
    print("\n" + "="*80)
    print("🔬 Mapping 核心数据“核磁共振”诊断")
    print("="*80)

    # 1. 获取最底层的 Panel 数据
    print("\n[Step 1] 从 Service 层提取原始 DataFrame...")
    try:
        config = ConfigLoader.load_config("M678")
        # 强制设置一个极低的目标，确保不是因为过滤导致的
        config.data_source.target_defect_groups = ["OLED_Mura", "Array_Line", "Array_Pixel"]
        
        panel_df = YieldAnalysisService.get_modified_panel_details(config)
        
        if panel_df.empty:
            print("❌ 致命错误：底层数据为空！请检查数据库连接。")
            return
        
        print(f"✅ 成功获取 {len(panel_df)} 行数据。")
    except Exception as e:
        print(f"❌ 数据获取失败: {e}")
        return

    # 2. 检查是否有我们关心的不良数据
    target_code = '彩斑Mura' 
    print(f"\n[Step 2] 检查目标不良 '{target_code}' 是否存在...")
    
    defect_df = panel_df[panel_df['defect_desc'] == target_code].copy()
    if defect_df.empty:
        print(f"❌ 警告：当前数据中没有任何 '{target_code}' 的记录！")
        print(f"   现有 Code 列表 (Top 10): {panel_df['defect_desc'].unique()[:10]}")
        return
    else:
        print(f"✅ 找到 {len(defect_df)} 条目标不良记录。")

    # 3. 【核心嫌疑 A】Panel ID 格式诊断
    print("\n[Step 3] 诊断 Panel ID 坐标解析能力...")
    sample_row = defect_df.iloc[0]
    sample_id = sample_row['panel_id']
    print(f"   - 样本 Panel ID: '{sample_id}' (长度: {len(str(sample_id))})")
    
    # 调用 Core 层的解析函数
    coords = _parse_panel_id_to_coords(sample_id)
    if coords is None:
        print(f"❌ 解析失败！_parse_panel_id_to_coords 返回 None。")
        print("   -> 原因推测：Panel ID 格式变化，导致切片取不到行/列代码。")
        print("   -> 逻辑硬编码位置：panel_id[11:13] 和 panel_id[13:15]")
    else:
        print(f"✅ 解析成功：坐标 {coords}")

    # 4. 【核心嫌疑 B】批次号格式诊断
    print("\n[Step 4] 诊断批次号 (Batch No) 格式...")
    sample_batch = str(sample_row['batch_no'])
    print(f"   - 样本 Batch No: '{sample_batch}'")
    
    # 模拟 mapping_processor 中的日期解析逻辑
    try:
        dt = pd.to_datetime(sample_batch, format='%Y/%m/%d', errors='raise')
        print(f"✅ 批次号可被解析为日期: {dt}")
    except Exception:
        print(f"❌ 警告：批次号 '{sample_batch}' 无法按照 '%Y/%m/%d' 格式解析！")
        print("   -> 影响：prepare_mapping_data 会因此无法通过日期排序找到“最新批次”，从而返回空结果。")

    # 5. 【核心嫌疑 C】数量阈值诊断
    print("\n[Step 5] 诊断批次数量阈值...")
    batch_counts = defect_df.groupby('batch_no')['panel_id'].nunique()
    max_count = batch_counts.max()
    print(f"   - 单个批次最大 Panel 数: {max_count}")
    
    # 模拟 prepare_mapping_data 调用 (使用我们之前的 0 阈值修改)
    print("\n[Step 6] 尝试强行执行 prepare_mapping_data...")
    try:
        # 此时如果您还没修改代码，这里依然是硬编码的 50000
        # 如果您已经修改了代码支持参数，请传入 min_panel_threshold=0
        # 为了兼容性，我们先尝试直接调用，看它默认行为
        import inspect
        sig = inspect.signature(prepare_mapping_data)
        if 'min_panel_threshold' in sig.parameters:
            print("   -> 检测到已支持 min_panel_threshold 参数，设置为 0...")
            result_df = prepare_mapping_data(panel_df, min_panel_threshold=0)
        else:
            print("   -> 函数未修改签名，使用默认逻辑 (可能含 50000 阈值)...")
            result_df = prepare_mapping_data(panel_df)
            
        if result_df.empty:
            print("❌ 最终结果为空！即使数据存在，Processor 也拒绝了处理。")
            if max_count < 50000:
                print(f"   -> 根本原因确认为：最大批次数量 ({max_count}) 小于硬编码阈值 (50000)。")
        else:
            print(f"🎉 成功生成了 {len(result_df)} 行 Mapping 数据！")
            
    except Exception as e:
        print(f"❌ 执行崩溃: {e}")

if __name__ == "__main__":
    test_deep_dive()