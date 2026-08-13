"""Application-owned AOI TT query contracts."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class AoiTtQueryConfig(BaseModel):
    start_date: str = Field(..., description="Inclusive start date in YYYY-MM-DD format")
    end_date: str = Field(..., description="Inclusive end date in YYYY-MM-DD format")
    prod_code: str = Field(..., description="Product code")
    factory: Optional[str] = Field(None, description="ARRAY, OLED, or TP")
    step_id: Optional[str] = Field(None, description="Measurement station")
    tt_name: Optional[str] = Field(None, description="AOI TT parameter")
