import logging                                                                     # 导入日志模块
import pandas as pd
import numpy as np   
from unittest.mock import patch

from vivo_project.core.mwd_trend_processor import create_code_level_mwd_trend_data # 导入趋势处理器
from vivo_project.core.sheet_lot_processor import (
   _add_daily_base_rate_to_df, 
   _apply_defect_capping, 
   calculate_sheet_defect_rates,
   calculate_lot_defect_rates,
   _calculate_raw_rates,\
   _calculate_lot_base_info_with_median_time
)      

def test_ema_smoothing_logic(sample_panel_df):                                     # 测试EMA平滑与中值钳制逻辑
    """验证Code级EMA平滑是否生效且稳定"""                                             # 函数文档说明
    logging.info("正在执行EMA平滑逻辑测试...")                                        # 记录日志
    
    # 执行我们优化后的函数 (EMA_SPAN=10)
    results = create_code_level_mwd_trend_data(sample_panel_df, ema_span=10)        # 调用待测函数
    
    assert results is not None                                                     # 断言结果不能为空
    assert 'daily_full' in results                                                 # 断言必须包含全量日度数据
    assert not results['daily_full'].empty                                         # 断言数据表不能为空
    
    # 验证平滑后的不良率是否在合理区间
    df_daily = results['daily_full']                                               # 获取日度数据
    assert df_daily['defect_rate'].max() <= 1.0                                    # 不良率不能超过100%
    logging.info("EMA平滑逻辑测试通过。")                                             # 记录测试通过

def test_daily_base_rate_mapping(sample_mwd_data):                                 # 测试日度基准映射逻辑
    """验证Lot能否根据入库日期精准匹配日度EMA基准"""                                   # 函数文档说明
    # 构造一个模拟的待覆盖Code明细表
    df_code = pd.DataFrame({                                                       # 构造测试表
        'sheet_id': ['S01', 'S02'],                                                # 单元ID
        'lot_id': ['L01', 'L01']                                                   # 批次ID
    })                                                                             # DataFrame结束
    
    # 构造基础信息表，包含日期 (对应 sample_mwd_data 中的日期)
    base_info = pd.DataFrame({                                                     # 构造关联表
        'sheet_id': ['S01', 'S02'],                                                # 单元ID
        'warehousing_time': ['1216', '1216']                                       # 入库日期字符串
    }).set_index('sheet_id')                                                       # 设定索引以便匹配
    
    # 注入全量日度数据 (模拟 sample_mwd_data)
    # 修改日期键以匹配 1216
    sample_mwd_data['daily_full'].loc[0, 'warehousing_time'] = pd.to_datetime('2025-12-16') # 强制对齐测试日期
    sample_mwd_data['daily_full']['defect_desc'] = '群亮点'                          # 确保描述匹配
    
    # 执行映射函数 (V1.3 日度丝滑映射版)
    mapped_df = _add_daily_base_rate_to_df(                                      # 调用待测函数
        df_code=df_code,                                                           # 传入测试表
        code_desc='群亮点',                                                         # 传入代码描述
        entity_id_col='sheet_id',                                                  # 传入ID列名
        base_info_df=base_info,                                                    # 传入关联信息
        mwd_code_data=sample_mwd_data                                              # 传入MWD全量数据
    )                                                                              # 函数调用结束
    
    assert 'daily_base_rate' in mapped_df.columns                                # 断言基准率列已生成
    assert not mapped_df['daily_base_rate'].isnull().any()                       # 断言没有缺失值
    logging.info("日度基准映射测试通过。")                                             # 记录测试通过

# [修改] 这里的 Patch 路径要指向模块中的函数名，而不是类方法
@patch('vivo_project.core.sheet_lot_processor._load_override_excel')
def test_cap_then_override_priority(mock_load_excel, mock_processing_config, sample_panel_df):
    """
    [Test Case 2] 测试 '先截断，再覆盖' 的流程优先级 (Integration Test)
    """
    logging.info("正在测试 '先截断 -> 再覆盖' 的优先级顺序...")

    # --- 1. 准备 Mock ---
    # [关键修复] 补全 'lot_id' 列。这是 override 模块内部校验所必需的。
    mock_override_df = pd.DataFrame({
        'sheet_id': ['S001'],
        'lot_id': ['L1'],  # <--- 新增此列
        'defect_desc': ['Code_High'],
        'override_rate': [0.50]
    })
    mock_load_excel.return_value = (mock_override_df, None)

    # --- 2. 准备输入数据 ---
    total_panels = 200 
    defect_count = 180 # 90% 不良率
    
    input_df = pd.DataFrame({
        'sheet_id': ['S001'] * total_panels,
        'panel_id': [f'P{i}' for i in range(total_panels)],
        'lot_id': ['L1'] * total_panels,
        'warehousing_time': [pd.to_datetime('2025-12-01')] * total_panels,
        'defect_group': ['OLED_Mura'] * defect_count + ['NoDefect'] * (total_panels - defect_count),
        'defect_desc': ['Code_High'] * defect_count + ['NoDefect'] * (total_panels - defect_count)
    })

    warning_lines = {'Code_High': 0.10}

    # --- 3. 执行核心计算函数 ---
    final_results = calculate_sheet_defect_rates(
        panel_details_df=input_df,
        target_defects=['OLED_Mura'],
        array_input_times_df=pd.DataFrame(),
        mwd_code_data=None,
        start_date=pd.to_datetime('2025-01-01'),
        warning_lines=warning_lines
    )

    # --- 4. 验证结果 ---
    assert final_results is not None, "计算结果为None"
    
    code_details = final_results['code_level_details'].get('OLED_Mura')
    assert code_details is not None
    
    target_row = code_details[
        (code_details['sheet_id'] == 'S001') & 
        (code_details['defect_desc'] == 'Code_High')
    ]
    
    assert not target_row.empty
    final_rate = target_row['defect_rate'].iloc[0]
    
    logging.info(f"最终结果: {final_rate}")
    
    # 断言覆盖生效 (0.50)
    assert np.isclose(final_rate, 0.50, atol=0.001), \
        f"优先级错误！期望覆盖值 0.50，实际得到 {final_rate}。"
        
    logging.info("优先级顺序测试通过。")

def test_precision_spec_capping():
    """
    [Test Case 1] 测试精确截断逻辑 (Unit Test)
    """
    logging.info("正在测试 Spec 精确截断逻辑...")
    
    df_code = pd.DataFrame({
        'sheet_id': ['S1'] * 100,
        'defect_desc': ['Code_X'] * 100,
        'defect_rate': [0.20] * 100,
        'total_panels': [1000] * 100
    })
    
    results_dict = {
        "group_level_summary_for_chart": pd.DataFrame(),
        "code_level_details": {'GroupA': df_code}
    }
    
    warning_lines = {'Code_X': 0.05}
    
    # [修改] 直接调用函数
    capped_results = _apply_defect_capping(
        results_dict=results_dict,
        group_thresholds={'upper': 1.0, 'lower': 0.0},
        code_thresholds={'upper': 1.0, 'lower': 0.0},
        warning_lines=warning_lines
    )
    
    final_rates = capped_results['code_level_details']['GroupA']['defect_rate'].values
    
    assert np.all(final_rates <= 0.05), "截断上限验证失败"
    assert np.all(final_rates >= 0.04), "软截断下限验证失败" # 0.05 * 0.8 = 0.04
    
    logging.info("Spec 精确截断逻辑测试通过。")

def test_strict_zero_filtering_isolated(mock_processing_config):
    """
    [Test Case 4 - 隔离验证版] 验证“零高度柱体剔除”逻辑
    
    我们绕过 calculate_lot_defect_rates 中的硬编码阈值干扰，
    直接测试核心函数 _calculate_raw_rates 的 Join 逻辑。
    
    场景：
    - Lot_Bad: 有不良记录 -> 必须保留
    - Lot_Good: 无不良记录 -> 必须消失 (不能生成 rate=0 的占位行)
    """
    logging.info("正在验证严格零值剔除逻辑 (隔离环境)...")

    # 1. 构造测试数据
    data = []
    
    # Lot_Bad: 有 'Code_X' 不良
    for i in range(100):
        data.append({
            'sheet_id': f'S_Bad_{i}', 'panel_id': f'P_Bad_{i}', 'lot_id': 'Lot_Bad',
            'warehousing_time': '20250101',
            'defect_group': 'Group_Target',
            'defect_desc': 'Code_X' 
        })
        
    # Lot_Good: 完全是良品 (NoDefect)
    for i in range(100):
        data.append({
            'sheet_id': f'S_Good_{i}', 'panel_id': f'P_Good_{i}', 'lot_id': 'Lot_Good',
            'warehousing_time': '20250102',
            'defect_group': 'NoDefect', 
            'defect_desc': 'NoDefect'
        })
        
    panel_df = pd.DataFrame(data)
    
    # 2. 手动构建基础信息 (模拟 _calculate_lot_base_info_with_median_time 的结果)
    lot_base = pd.DataFrame({
        'lot_id': ['Lot_Bad', 'Lot_Good'],
        'total_panels': [100, 100],
        'warehousing_time': ['20250101', '20250102'],
        'array_input_time': [pd.NaT, pd.NaT] # 不重要
    })
    
    # 3. 直接调用核心计算逻辑
    # 这里的关键是：如果是 Left Join (Numerator左表)，Lot_Good 应该直接消失
    raw_results = _calculate_raw_rates(
        panel_details_df_filtered=panel_df,
        base_info_df_filtered=lot_base.set_index('lot_id'),
        target_defects=['Group_Target'],
        entity_id_col='lot_id'
    )
    
    assert raw_results is not None, "原始计算结果不应为空"
    
    # 获取 Group_Target 的结果
    # 注意：我们只关心 Group_Target，Lot_Good 在这个 Group 下应该是没有数据的
    code_df = raw_results['code_level_details'].get('Group_Target', pd.DataFrame())
    
    # 4. 验证 Lot_Bad 存在
    bad_lot_row = code_df[code_df['lot_id'] == 'Lot_Bad']
    assert not bad_lot_row.empty, "严重错误：有不良的 Lot_Bad 丢失了！"
    assert bad_lot_row['defect_rate'].iloc[0] > 0, "Lot_Bad 的良损率应大于 0"
    
    # 5. [核心验证] 验证 Lot_Good 不存在
    # 如果代码逻辑正确 (剔除0值)，这里应该是空的
    good_lot_row = code_df[code_df['lot_id'] == 'Lot_Good']
    
    if not good_lot_row.empty:
        actual_rate = good_lot_row['defect_rate'].values[0]
        assert False, f"测试失败！发现 '无不良的 Lot_Good' 残留在结果中 (Rate={actual_rate})，这将导致图表出现空白断档。"
        
    logging.info("✅ 验证通过：结果中只包含有不良记录的 Lot，零值 Lot 已被彻底剔除。")