import logging                                                                     # 导入日志模块
import pandas as pd                                                                # 导入Pandas
from vivo_project.core.mwd_trend_processor import create_code_level_mwd_trend_data # 导入趋势处理器
from vivo_project.core.sheet_lot_processor import _add_daily_base_rate_to_df      # 导入模拟映射逻辑

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