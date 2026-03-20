import pytest
import numpy as np
import logging
# 请根据实际项目结构导入函数
# 假设该函数位于 vivo_project.core.mwd_trend_processor 模块中
from yield_domain.core.mwd_trend_processor import _calculate_adaptive_shadow_ema

class TestShadowEMA:
    """
    测试 'Shadow EMA' (影子基准) 算法的核心特性。
    重点验证：在单日剧烈波动（Spike）后，次日是否能迅速回归正常，而不是被拉高。
    """

    def test_spike_rejection_logic(self):
        """
        [核心场景测试]
        Day 1~5:  1.0% (平稳期，建立基准)
        Day 6:   16.0% (事故！单日暴雷)
        Day 7:    1.0% (修复，回归正常)
        
        预期结果：
        Day 6 的计算值应该很高 (报警)。
        Day 7 的计算值应该瞬间回落到 ~1.0% (无拖尾)。
        """
        # 1. 构造数据
        # 每天入库 10,000 片
        totals = np.array([10000] * 10)
        
        # 不良数：
        # Day 0-4: 100 (1%)
        # Day 5: 1600 (16%) -> 突发异常
        # Day 6-9: 100 (1%) -> 恢复
        counts = np.array([100, 100, 100, 100, 100, 1600, 100, 100, 100, 100])
        
        # 2. 运行 Shadow EMA (Span=7)
        # alpha ≈ 0.25
        smoothed = _calculate_adaptive_shadow_ema(counts, totals, span=7)
        
        # 3. 验证结果
        logging.info(f"Shadow EMA 计算结果: {[f'{x:.2%}' for x in smoothed]}")

        # 验证 A: 平稳期 (Day 4) 应该接近 1%
        assert 0.009 <= smoothed[4] <= 0.011, f"平稳期基准建立失败: {smoothed[4]}"

        # 验证 B: 爆发期 (Day 5) 应该如实反应上涨
        # 标准 EMA 更新: 0.25 * 0.16 + 0.75 * 0.01 = 0.0475 (4.75%)
        # 只要它显著大于 1%，说明报警功能正常
        spike_val = smoothed[5]
        assert spike_val > 0.03, f"异常当天未能显示高峰: {spike_val} (太低了，没报警？)"
        logging.info(f"Day 6 异常日显示值: {spike_val:.2%} (成功报警)")

        # 验证 C: 恢复期 (Day 6) [这是您最关心的！]
        # 如果是普通 EMA: 
        #   昨日EMA=4.75%，今日Rate=1%
        #   今日EMA = 0.25*0.01 + 0.75*0.0475 = 0.0025 + 0.0356 = 3.81%
        #   (3.81% 依然远高于 1%，这就是"拖尾")
        #
        # 如果是 Shadow EMA:
        #   昨日内部基准 ≈ 1% (忽略了16%), 今日Rate=1%
        #   今日EMA ≈ 1%
        recovery_val = smoothed[6]
        
        # 我们断言它必须小于 1.5% (给一点点浮动空间)
        # 如果它还在 3% 以上，说明 Shadow 机制失效了
        assert recovery_val < 0.015, \
            f"拖尾消除失败！Day 7 依然高达 {recovery_val:.2%} (普通EMA约为3.8%)，说明昨天的16%污染了基准。"
            
        logging.info(f"Day 7 恢复日显示值: {recovery_val:.2%} (成功回落，无拖尾)")

    def test_normal_fluctuation(self):
        """
        测试正常波动是否被错误剔除。
        如果波动很小 (例如 1.0% -> 1.2%)，不应该触发 Spike 逻辑，应该正常平滑。
        """
        totals = np.array([10000] * 5)
        # 1.0% -> 1.2% -> 1.0%
        counts = np.array([100, 100, 120, 100, 100]) 
        
        smoothed = _calculate_adaptive_shadow_ema(counts, totals, span=7)
        
        # Day 2 (1.2%) 的计算结果应该略微上升
        # 如果被判定为 Spike 剔除了，它可能就不会动，或者动得很奇怪
        # 普通 EMA: 0.25 * 0.012 + 0.75 * 0.01 = 0.003 + 0.0075 = 1.05%
        assert smoothed[2] > 0.0101, "正常波动应该被平滑更新，而不是完全忽略"
        
    def test_empty_input(self):
        """边界测试：空输入"""
        assert _calculate_adaptive_shadow_ema(np.array([]), np.array([]), 7) == []

    def test_zero_denominator(self):
        """边界测试：分母为0"""
        counts = np.array([10, 10])
        totals = np.array([100, 0]) # 第二天没入库
        smoothed = _calculate_adaptive_shadow_ema(counts, totals, 7)
        assert smoothed[1] == 0.0, "分母为0时应返回0"