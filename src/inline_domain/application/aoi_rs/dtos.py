"""AOI_RS application data-transfer objects."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class AoiRsQueryConfig(BaseModel):
    """Strongly typed AOI_RS query for the report's fixed date window."""

    start_date: str = Field(..., description="开始日期, 格式 YYYY-MM-DD")
    end_date: str = Field(..., description="结束日期, 格式 YYYY-MM-DD")
    prod_code: str = Field(..., description="产品代码")
    factory: Optional[str] = Field(None, description="厂别 (ARRAY/OLED/TP)")
    step_id: Optional[str] = Field(None, description="站点ID")
    rs_code: Optional[str] = Field(None, description="RS Code")

