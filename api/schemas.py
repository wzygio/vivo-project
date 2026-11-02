"""Explicit public contract. Internal columns cannot survive serialization."""
from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator


class AnomalyQuery(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    product: str = Field(min_length=1, max_length=64, pattern=r'^[A-Za-z0-9_-]+$')
    sub_domain: Literal['SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time']
    event_type: Literal['OOS', 'OOC'] = 'OOS'
    factory: str | None = Field(default=None, min_length=1, max_length=64)
    entity_type: Literal['sheet', 'lot'] | None = None
    entity_id: str | None = Field(default=None, min_length=1, max_length=128)
    limit: int = Field(default=100, ge=1, le=500)
    cursor: str | None = Field(default=None, max_length=2048)

    @model_validator(mode='after')
    def supported_event(self) -> AnomalyQuery:
        if self.sub_domain in ('Yield', 'Q-Time') and self.event_type != 'OOS':
            raise ValueError('This source has no OOC publication')
        return self


class AnomalyRecord(BaseModel):
    model_config = ConfigDict(extra='forbid', populate_by_name=True, frozen=True)
    sub_domain: Literal['SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time'] = Field(alias='分类')
    step_id: str | None = Field(alias='发生站点', min_length=1, max_length=128)
    factory: str = Field(alias='部门', min_length=1, max_length=64)
    layer: str | None = Field(alias='层别', min_length=1, max_length=256)
    parameter_name: str = Field(alias='监控参数', min_length=1, max_length=512)
    event_type: Literal['OOS', 'OOC'] = Field(alias='异常事件')
    entity_type: Literal['sheet', 'lot'] = Field(alias='对象类型')
    entity_id: str = Field(alias='对象编号', min_length=1, max_length=128)
    product: str = Field(alias='产品型号', min_length=1, max_length=64)
    occurred_at: str = Field(alias='发生时间', min_length=1, max_length=64)
    from_step_id: str | None = Field(default=None, alias='起始站点', min_length=1, max_length=128)
    boundary: str | None = Field(default=None, alias='命中边界', max_length=64)

    @model_validator(mode='after')
    def required_station(self) -> AnomalyRecord:
        if self.sub_domain == 'Yield':
            if self.step_id is not None or self.layer is not None:
                raise ValueError('Yield location must be unknown')
        elif not self.step_id or not self.layer:
            raise ValueError('Source station and layer are required')
        return self


class ResultMeta(BaseModel):
    status: Literal['ok', 'alert']
    returned_count: int
    has_more: bool
    next_cursor: str | None
    period_start: str
    period_end_exclusive: str
    timezone: str = 'Asia/Shanghai'
    read_at: str
    limitations: list[str] = Field(default_factory=list)


class AnomalyResponse(BaseModel):
    data: list[AnomalyRecord]
    meta: ResultMeta
