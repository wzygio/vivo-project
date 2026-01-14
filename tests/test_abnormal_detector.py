import pytest
import pandas as pd
import numpy as np
import logging
from vivo_project.core.abnormal_detector import AbnormalDetector

# ==============================================================================
#  测试 1: 系统内部月度趋势检测 (detect_system_trend_alerts)
# ==============================================================================

def test_system_trend_normal():
    """测试正常波动，不应触发报警"""
    # 构造平稳数据: 0.01 -> 0.015 (未翻倍，差值<0.2)
    df = pd.DataFrame({
        'time_period': ['2025-01', '2025-02'],
        'defect_group': ['GroupA', 'GroupA'],
        'defect_rate': [0.01, 0.015]
    })
    
    alerts = AbnormalDetector.detect_system_trend_alerts(group_monthly=df, code_monthly=pd.DataFrame())
    assert len(alerts) == 0, "平稳数据不应触发报警"

def test_system_trend_doubling():
    """测试环比翻倍报警"""
    # 构造翻倍数据: 0.01 -> 0.03 (翻倍)
    df = pd.DataFrame({
        'time_period': ['2025-01', '2025-02'],
        'defect_group': ['GroupBad', 'GroupBad'],
        'defect_rate': [0.01, 0.03]
    })
    
    alerts = AbnormalDetector.detect_system_trend_alerts(group_monthly=df, code_monthly=pd.DataFrame())
    assert len(alerts) == 1
    assert "环比翻倍" in alerts[0]
    assert "Group 预警 [GroupBad]" in alerts[0]

def test_system_trend_surge():
    """测试激增 > 20% 报警"""
    # 构造激增数据: 0.1 -> 0.35 (差值 0.25 > 0.2)
    df = pd.DataFrame({
        'time_period': ['2025-01', '2025-02'],
        'defect_desc': ['CodeX', 'CodeX'],
        'defect_rate': [0.1, 0.35]
    })
    
    alerts = AbnormalDetector.detect_system_trend_alerts(group_monthly=pd.DataFrame(), code_monthly=df)
    assert len(alerts) == 1
    assert "增幅>20%" in alerts[0]
    assert "Code 预警 [CodeX]" in alerts[0]

# ==============================================================================
#  测试 2: 外部基准报表批次比对 (detect_benchmark_batch_alerts)
# ==============================================================================

@pytest.fixture
def mock_benchmark_report():
    """
    构造一个模拟的 Excel 原始 DataFrame (无表头，按位置索引)
    结构模拟:
    Row 0: 批次号
    Row 1: (空)
    Row 2: C列='批次产出率', E列以后是数据
    Row 3: C列='GroupA' (正常)
    Row 4: C列='GroupB' (异常)
    Row 5: D列='CodeX' (正常)
    Row 6: D列='CodeY' (异常)
    """
    data = [
        # Col 0, 1, 2(C), 3(D), 4, 5(Old), 6(New), 7(BadYield)
        [None, None, "BatchID", None, None, "B001", "B002", "B003"],  # Row 0
        [None, None, None, None, None, None, None, None],             # Row 1
        [None, None, "批次产出率", None, None, 0.95, 0.98, 0.10],     # Row 2 (Yield) -> Valid: Col 6, 5
        [None, None, "GroupA", None, None, 0.01, 0.015, 0.0],       # Row 3 (Normal)
        [None, None, "GroupB", None, None, 0.01, 0.03, 0.0],        # Row 4 (Doubled: 0.01->0.03)
        [None, None, None, "CodeX", None, 0.05, 0.06, 0.0],         # Row 5 (Normal)
        [None, None, None, "CodeY", None, 0.10, 0.35, 0.0],         # Row 6 (Surge: 0.1->0.35)
    ]
    return pd.DataFrame(data)

def test_benchmark_logic_basic(mock_benchmark_report):
    """测试基准比对的核心逻辑：能否正确找到行列并判定"""
    # 目标: GroupB (Row 4) 和 CodeY (Row 6) 应该报警
    target_groups = ['GroupA', 'GroupB']
    target_codes = ['CodeX', 'CodeY']
    
    alerts = AbnormalDetector.detect_benchmark_batch_alerts(
        mock_benchmark_report, target_groups, target_codes
    )
    
    # 验证数量
    assert len(alerts) == 2, f"预期2个报警，实际 {len(alerts)} 个: {alerts}"
    
    # 验证内容
    alert_text = " ".join(alerts)
    assert "Group 真实报表预警 [GroupB]" in alert_text
    assert "Code 真实报表预警 [CodeY]" in alert_text
    assert "批次B002" in alert_text # 确认取到了正确的批次号

def test_benchmark_missing_yield_row():
    """测试健壮性：如果报表里找不到'批次产出率'行"""
    df = pd.DataFrame([
        [None, None, "错误行名", None, 0.9, 0.9]
    ])
    alerts = AbnormalDetector.detect_benchmark_batch_alerts(df, ['G1'], [])
    assert len(alerts) == 0 # 应该优雅返回空列表，不报错

def test_benchmark_insufficient_batches():
    """测试健壮性：有效批次不足2个"""
    df = pd.DataFrame([
        [None, None, "BatchID", None, "B1", "B2"],
        # 只有一个批次产出率 > 0.2
        [None, None, "批次产出率", None, 0.1, 0.95] 
    ])
    alerts = AbnormalDetector.detect_benchmark_batch_alerts(df, ['G1'], [])
    assert len(alerts) == 0

def test_benchmark_dirty_data():
    """测试健壮性：数据单元格包含非数值垃圾数据"""
    df = pd.DataFrame([
        [None, None, "BatchID", None, "B1", "B2"],
        [None, None, "批次产出率", None, 0.9, 0.9], # Valid cols: 5, 4
        # GroupC 的数据是字符串 "N/A"，不应报错
        [None, None, "GroupC", None, 0.01, "N/A"] 
    ])
    
    alerts = AbnormalDetector.detect_benchmark_batch_alerts(df, ['GroupC'], [])
    assert len(alerts) == 0