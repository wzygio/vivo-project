import pytest
import numpy as np
import logging
# 请确保引用路径正确，根据之前的上下文，类名应为 SheetLotProcessor
# 如果代码不在类里，请去掉前面的类名直接引用函数
from yield_domain.core.sheet_lot_processor import _apply_random_cap_and_floor

class TestCappingMechanism:
    """
    测试 'Spec 截断' 机制的核心逻辑
    目标：证明超规数据会被'软着陆'到 Spec 线下方，而不是硬切在 Spec 线上。
    """

    def test_capping_upper_bound_strictness(self):
        """
        测试 1: 严格验证上限
        输入: 良率 1.5% (严重超标)
        Spec: 1.0%
        预期: 结果必须 <= 0.95% (即 0.0095)，且 >= 0.8% (0.008)
        """
        input_rate = 0.015  # 1.5%
        spec_limit = 0.010  # 1.0%
        rng = np.random.default_rng(42) # 固定种子方便复现

        # 模拟运行 100 次，确保每一次都落在安全区间
        for i in range(100):
            capped_val = _apply_random_cap_and_floor(
                rate=input_rate,
                upper_threshold=spec_limit,
                lower_threshold=0.001,
                rng=rng
            )
            
            # 验证 A: 必须小于 Spec (绝不能齐平)
            assert capped_val < spec_limit, \
                f"第 {i} 次失败: 结果 {capped_val} 没有低于 Spec {spec_limit}"
                
            # 验证 B: 必须在 80% ~ 95% 的区间内
            lower_bound = spec_limit * 0.8
            upper_bound_internal = spec_limit * 0.95
            
            assert lower_bound <= capped_val <= upper_bound_internal + 1e-9, \
                f"第 {i} 次失败: 结果 {capped_val:.5f} 超出了内部安全区 [{lower_bound:.5f}, {upper_bound_internal:.5f}]"

    def test_capping_randomness(self):
        """
        测试 2: 验证随机性
        输入: 同样的超标数据
        预期: 连续调用产生的数值应该不同 (方差 > 0)
        """
        input_rate = 0.02
        spec_limit = 0.01
        rng = np.random.default_rng(123)
        
        results = []
        for _ in range(50):
            val = _apply_random_cap_and_floor(
                rate=input_rate,
                upper_threshold=spec_limit,
                lower_threshold=0.0001,
                rng=rng
            )
            results.append(val)
        
        # 检查是否所有值都一样 (如果逻辑写死或者 Spec 没生效，这里会由 unique=1)
        unique_count = len(set(results))
        assert unique_count > 1, \
            f"随机性失效！运行50次得到了完全相同的结果: {results[0]}。说明代码可能返回了固定值。"
            
        # 打印一下均值，让人放心
        avg = sum(results) / len(results)
        logging.info(f"随机性测试通过。50次截断的平均值: {avg:.5f} (约为 Spec 的 {avg/spec_limit:.1%})")

    def test_normal_value_pass_through(self):
        """
        测试 3: 正常值直通
        输入: 0.5% (未超标)
        Spec: 1.0%
        预期: 保持原值不变
        """
        input_rate = 0.005
        spec_limit = 0.01
        rng = np.random.default_rng(42)
        
        val = _apply_random_cap_and_floor(
            rate=input_rate,
            upper_threshold=spec_limit,
            lower_threshold=0.001,
            rng=rng
        )
        
        assert val == input_rate, f"正常值不应被修改。输入: {input_rate}, 输出: {val}"