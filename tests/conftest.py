import pytest                                                                      # 导入pytest框架
import pandas as pd                                                                # 导入Pandas
from tests.factories import DataFactory                                            # 导入刚才创建的工厂
from vivo_project.config import CONFIG                                             # 导入项目配置

@pytest.fixture(scope="session")                                                   # 定义会话级固件
def mock_config():                                                                 # 模拟配置信息
    """提供测试用的配置镜像"""                                                        # 函数文档说明
    return CONFIG                                                                  # 返回全局配置

@pytest.fixture                                                                    # 定义固件
def sample_panel_df():                                                             # 提供样本明细数据
    """提供通用的Panel明细DataFrame"""                                              # 函数文档说明
    return DataFactory.create_mock_panel_details(n_rows=200)                       # 调用工厂生成200行数据

@pytest.fixture                                                                    # 定义固件
def sample_mwd_data():                                                             # 提供趋势图数据
    """提供模拟的趋势分析数据集"""                                                   # 函数文档说明
    return DataFactory.create_mock_mwd_data()                                      # 调用工厂生成MWD字典