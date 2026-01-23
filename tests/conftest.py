# tests/conftest.py
import pytest
import pandas as pd
from pathlib import Path

# [Refactor] 引入新的配置加载器
from vivo_project.config import ConfigLoader
from vivo_project.config_model import AppConfig
from tests.factories import DataFactory

@pytest.fixture(scope="session")
def project_root():
    """获取项目根目录"""
    return ConfigLoader.get_project_root()

@pytest.fixture(scope="session")
def real_app_config():
    """
    [Fixture] 加载真实的 M678 配置作为基准。
    Scope 为 session，整个测试过程只加载一次文件，提高速度。
    """
    # 确保能找到 M678.yaml，如果找不到会抛错
    try:
        return ConfigLoader.load_config("M678")
    except Exception as e:
        pytest.fail(f"测试初始化失败：无法加载 M678 配置: {e}")

@pytest.fixture
def mock_config(real_app_config):
    """
    [Fixture] 提供一个配置对象的深拷贝。
    每个测试用例都会获得一个独立的副本，修改它不会影响其他测试。
    """
    # Pydantic v2 使用 model_copy(deep=True)，v1 使用 copy(deep=True)
    # 如果您使用的是 Pydantic V2:
    if hasattr(real_app_config, "model_copy"):
        return real_app_config.model_copy(deep=True)
    # 如果是 Pydantic V1:
    else:
        return real_app_config.copy(deep=True)

@pytest.fixture
def mock_processing_config(mock_config):
    """
    [Fixture] 在标准配置基础上，开启特定开关的配置对象。
    不再需要 monkeypatch 全局变量，直接修改传入的对象即可。
    """
    # 直接修改对象的属性 (AppConfig.processing 是一个字典)
    mock_config.processing['defect_capping']['enable'] = True
    # 放宽阈值以便测试
    mock_config.processing['defect_capping']['group_thresholds'] = {'upper': 100.0, 'lower': 0.0}
    mock_config.processing['defect_capping']['code_thresholds'] = {'upper': 100.0, 'lower': 0.0}
    
    # 模拟覆盖文件配置
    mock_config.processing['rate_override_config'] = {
        'enable': True,
        'override_file': 'dummy.xlsx',
        'override_sheet_name': 'Sheet1'
    }
    
    # 确保目标 Group 包含测试数据中用到的
    mock_config.data_source.target_defect_groups = ['OLED_Mura', 'Array_Line']
    
    return mock_config

@pytest.fixture
def sample_panel_df():
    """提供通用的Panel明细DataFrame"""
    return DataFactory.create_mock_panel_details(n_rows=200)

@pytest.fixture
def sample_mwd_data():
    """提供模拟的趋势分析数据集"""
    return DataFactory.create_mock_mwd_data()

@pytest.fixture
def resource_dir(project_root):
    """提供资源目录路径"""
    return project_root / "resources"