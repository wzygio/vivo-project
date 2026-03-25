from pydantic import BaseModel, Field
from typing import Optional

class SpcQueryConfig(BaseModel):
    """
    [SPC 域] 动态查询契约 (DTO)
    封装前端发起的 SPC 量测数据查询请求参数。
    """
    start_date: str = Field(..., description="查询起始日期 (格式 YYYY-MM-DD)")
    end_date: str = Field(..., description="查询截止日期 (格式 YYYY-MM-DD)")
    prod_code: str = Field(..., description="目标产品代码 (如 M626)")
    factory: Optional[str] = Field(None, description="工厂分类 (如 ARRAY, OLED)")
    step_id: Optional[str] = Field(None, description="特定站点ID")
    param_name: Optional[str] = Field(None, description="特定参数名称")