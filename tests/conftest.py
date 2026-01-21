import pytest                                                                      
import pandas as pd
from vivo_project.config import CONFIG                                                                  
from tests.factories import DataFactory                                            # 导入刚才创建的工厂
                                           
@pytest.fixture(scope="session")                                                   # 定义会话级固件
def mock_config():                                                                 # 模拟配置信息
    """提供测试用的配置镜像"""                                                       
    return CONFIG                                                                  # 返回全局配置

@pytest.fixture                                                                    # 定义固件
def sample_panel_df():                                                             # 提供样本明细数据
    """提供通用的Panel明细DataFrame"""                                              
    return DataFactory.create_mock_panel_details(n_rows=200)                       # 调用工厂生成200行数据

@pytest.fixture                                                                    # 定义固件
def sample_mwd_data():                                                             # 提供趋势图数据
    """提供模拟的趋势分析数据集"""                                                   
    return DataFactory.create_mock_mwd_data()                                      # 调用工厂生成MWD字典

@pytest.fixture
def mock_processing_config(monkeypatch):
    """
    [Fixture] 强制开启截断和覆盖功能的配置
    """
    new_config = CONFIG.copy()
    new_config['processing'] = {
        'defect_capping': {
            'enable': True,
            'group_thresholds': {'upper': 1.0, 'lower': 0.0}, # 放宽全局限制，专注于测 Spec
            'code_thresholds': {'upper': 1.0, 'lower': 0.0}
        },
        'rate_override_config': {
            'enable': True, # 实际上代码里没用这个开关，是靠有没有文件，但保持配置一致
            'override_file': 'dummy.xlsx',
            'override_sheet_name': 'Sheet1'
        },
        'target_defect_groups': ['OLED_Mura']
    }
    # 使用 monkeypatch 临时替换 CONFIG 中的 processing
    monkeypatch.setitem(CONFIG, 'processing', new_config['processing'])
    return new_config