from pydantic import BaseModel, Field
from typing import List

class YieldQueryConfig(BaseModel):
    """
    [Yield 域] 动态查询契约 (DTO)
    封装前端发起的良率数据查询请求参数。
    """
    start_date: str = Field(..., description="查询起始日期 (格式 YYYY-MM-DD)")
    end_date: str = Field(..., description="查询截止日期 (格式 YYYY-MM-DD)")
    product_code: str = Field(..., description="目标产品代码 (如 M626)")
    work_order_types: List[str] = Field(default_factory=list, description="目标工单类型 (如 ESLC, P)")
    target_defect_groups: List[str] = Field(default_factory=list, description="特定缺陷大类过滤")