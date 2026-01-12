import pandas as pd                                                                # 导入Pandas用于生成测试表格数据
import numpy as np                                                                 # 导入NumPy用于生成随机数
from datetime import datetime, timedelta                                           # 导入时间处理工具

class DataFactory:                                                                 # 定义测试数据生成工厂类
    @staticmethod                                                                  # 定义静态方法
    def create_mock_panel_details(n_rows: int = 100, lot_id: str = "L3MR5A0B0") -> pd.DataFrame: # 生成模拟Panel明细数据
        """生成符合系统要求的原始Panel级明细数据"""                                    # 函数文档说明
        data = []                                                                  # 初始化数据列表
        base_date = datetime(2025, 12, 1)                                          # 设定起始日期
        
        for i in range(n_rows):                                                    # 循环生成指定行数的数据
            sheet_suffix = f"{i // 20:02d}"                                        # 每20片一个Sheet
            sheet_id = f"{lot_id}{sheet_suffix}"                                   # 组合生成Sheet ID
            panel_id = f"{sheet_id}{i % 20:02d}"                                   # 组合生成Panel ID
            date_str = (base_date + timedelta(days=i // 50)).strftime('%Y%m%d')    # 模拟日期递增
            
            data.append({                                                          # 将一行记录加入列表
                'batch_no': 'BATCH001',                                            # 批次号
                'lot_id': lot_id,                                                  # 批次ID
                'sheet_id': sheet_id,                                              # 单元ID
                'panel_id': panel_id,                                              # 玻璃ID
                'prod_code': 'M678',                                               # 产品代码
                'warehousing_time': date_str,                                      # 入库时间 (YYYYMMDD)
                'defect_group': 'OLED_Mura' if i % 10 == 0 else 'NoDefect',        # 模拟缺陷分组
                'defect_desc': '群亮点' if i % 10 == 0 else 'None'                 # 模拟缺陷描述
            })                                                                     # 数据行构建结束
        return pd.DataFrame(data)                                                  # 返回构造好的DataFrame

    @staticmethod                                                                  # 定义静态方法
    def create_mock_mwd_data() -> dict:                                            # 生成模拟的月周天趋势字典
        """生成模拟的趋势分析结果数据结构"""                                           # 函数文档说明
        dates = pd.date_range(start='2025-10-01', periods=90, freq='D')            # 生成90天日期序列
        df = pd.DataFrame({                                                        # 构造日度基础表
            'time_period': dates.strftime('%m-%d'),                                # 格式化日期列
            'warehousing_time': dates,                                             # 原始日期列
            'defect_group': 'OLED_Mura',                                           # 固定缺陷组
            'defect_desc': '群亮点',                                               # 固定缺陷描述
            'defect_rate': np.random.uniform(0.01, 0.05, size=90),                 # 生成随机不良率
            'total_panels': 10000                                                  # 固定入库数
        })                                                                         # DataFrame构建结束
        return {'daily_full': df, 'monthly': df, 'weekly': df}                      # 返回模拟的MWD字典结构