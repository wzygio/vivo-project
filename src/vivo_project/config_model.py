# src/vivo_project/config_model.py
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

# ==============================================================================
# 1. 基础原子模型 (Building Blocks)
# ==============================================================================

class FileResource(BaseModel):
    """
    通用的文件资源描述。
    不管是 warning_lines 还是 override_config，本质上都是指向一个文件。
    """
    file_name: str = Field(..., description="文件名，需位于 resources 目录下")
    sheet_name: Optional[str] = Field(None, description="Excel Sheet名称，可选")
    
# ==============================================================================
# 2. 核心严格模型 (Core Strict Sections)
#    这些配置是所有产品都必须有的，否则程序无法启动。
# ==============================================================================

class ApplicationConfig(BaseModel):
    cache_ttl_hours: int = Field(..., gt=0, description="缓存有效期必须为正整数")

class DataSourceConfig(BaseModel):
    product_code: str = Field(..., min_length=1)
    # 虽然 target_defect_groups 是列表，但我们允许它为空（某些产品可能不按 Group 过滤）
    target_defect_groups: List[str] = Field(default_factory=list)
    work_order_types: List[str] = Field(default_factory=list)
    
    model_config = ConfigDict(extra="allow")

class UIConfig(BaseModel):
    icons: Dict[str, str] = Field(default_factory=dict)

# ==============================================================================
# 3. 弹性主模型 (Flexible Root Config)
# ==============================================================================

class AppConfig(BaseModel):
    """
    应用主配置。
    特点：Processing 和 Paths 极其灵活，适应不同产品的差异化配置。
    """
    # 1. 核心部分 (Strict)
    application: ApplicationConfig
    data_source: DataSourceConfig
    ui: UIConfig = Field(default_factory=UIConfig)

    # 2. 弹性扩展部分 (Flexible)
    
    # paths: 只要 Value 符合 FileResource 结构，Key 可以叫 'warning_lines', 'thresholds' 等任意名字
    paths: Dict[str, FileResource] = Field(default_factory=dict)
    
    # processing: 这是一个完全自由的字典。
    # M678 可以有 'defect_capping'，M626 可以有 'ai_adjustment'，互不冲突。
    # 只要符合 JSON/YAML 结构即可。
    processing: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="ignore")