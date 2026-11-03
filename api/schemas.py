"""Explicit public contract. Internal columns cannot survive serialization."""
from __future__ import annotations

from typing import Literal
from datetime import date
import re
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator


class AnomalyQuery(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    product: str | None = Field(default=None, min_length=1, max_length=64, pattern=r'^[A-Za-z0-9_-]+$')
    sub_domain: Literal['SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time'] | None = None
    event_type: Literal['OOS', 'OOC', 'CPK', 'CPM'] = 'OOS'
    factory: str | None = Field(default=None, min_length=1, max_length=64)
    step_id: str | None = Field(default=None, min_length=1, max_length=128)
    layer: str | None = Field(default=None, min_length=1, max_length=256)
    parameter_name: str | None = Field(default=None, min_length=1, max_length=512)
    start_date: date | None = None
    end_date: date | None = None
    entity_type: Literal['sheet', 'lot', 'capability'] | None = None
    entity_id: str | None = Field(default=None, min_length=1, max_length=128)
    limit: int | None = Field(default=None, ge=1, le=500)
    cursor: str | None = Field(default=None, min_length=1, max_length=2048)

    @field_validator('start_date', 'end_date', mode='before')
    @classmethod
    def calendar_date(cls, value: object) -> object:
        if value is not None and not isinstance(value, date):
            if not isinstance(value, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
                raise ValueError('Use YYYY-MM-DD calendar dates')
        return value

    @model_validator(mode='after')
    def supported_event(self) -> AnomalyQuery:
        if self.event_type in ('CPK', 'CPM') and self.sub_domain not in (None, 'SPC'):
            raise ValueError('Capability events belong to SPC')
        if self.sub_domain in ('Yield', 'Q-Time') and self.event_type != 'OOS':
            raise ValueError('This source has no OOC publication')
        if self.cursor and self.limit is None:
            raise ValueError('Cursor requires an explicit page size')
        return self


class _PublicRecordBase(BaseModel):
    model_config = ConfigDict(extra='forbid', populate_by_name=True, frozen=True)
    sub_domain: Literal['SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time'] = Field(alias='分类')
    step_id: str | None = Field(alias='发生站点', min_length=1, max_length=128)
    factory: str = Field(alias='部门', min_length=1, max_length=64)
    layer: str | None = Field(alias='层别', min_length=1, max_length=256)
    parameter_name: str = Field(alias='监控参数', min_length=1, max_length=512)
    entity_id: str = Field(alias='对象编号', min_length=1, max_length=128)
    product: str = Field(alias='产品型号', min_length=1, max_length=64)
    occurred_at: str = Field(alias='发生时间', min_length=1, max_length=64)
    boundary: str | None = Field(default=None, alias='命中边界', max_length=64)

    @model_validator(mode='after')
    def required_station(self) -> _PublicRecordBase:
        if self.sub_domain == 'Yield':
            if self.step_id is not None or self.layer is not None:
                raise ValueError('Yield location must be unknown')
        elif not self.step_id or not self.layer:
            raise ValueError('Source station and layer are required')
        return self


class AnomalyRecord(_PublicRecordBase):
    event_type: Literal['OOS', 'OOC'] = Field(alias='异常事件')
    entity_type: Literal['sheet', 'lot'] = Field(alias='对象类型')


class CapabilityRecord(_PublicRecordBase):
    """A complete weekly parameter aggregate, never an invented Sheet/Lot."""

    event_type: Literal['CPK', 'CPM'] = Field(alias='异常事件')
    entity_type: Literal['capability'] = Field(alias='对象类型')
    metric_value: float = Field(alias='指标值', allow_inf_nan=False)
    lower_bound: float = Field(alias='达标下限', allow_inf_nan=False)
    period_type: Literal['week'] = Field(alias='统计周期类型')
    period_label: str = Field(alias='统计周期', pattern=r'^[0-9]{4}-W[0-9]{2}$')
    period_start_date: date = Field(alias='统计开始日期')
    period_end_date: date = Field(alias='统计截止日期')

    @model_validator(mode='after')
    def complete_week(self) -> CapabilityRecord:
        if self.sub_domain != 'SPC' or not re.fullmatch(r'capability:[0-9a-f]{64}', self.entity_id):
            raise ValueError('Capability identity must belong to SPC')
        if (self.period_start_date.weekday() != 0 or self.period_end_date.weekday() != 6
                or (self.period_end_date - self.period_start_date).days != 6
                or self.occurred_at != self.period_end_date.isoformat()):
            raise ValueError('Capability dates must identify one complete ISO week')
        year, week, _ = self.period_start_date.isocalendar()
        if self.period_label != f'{year}-W{week:02d}' or self.metric_value >= self.lower_bound:
            raise ValueError('Invalid capability alert period or threshold')
        return self


PublicRecord = AnomalyRecord | CapabilityRecord
PUBLIC_RECORD_ADAPTER = TypeAdapter(PublicRecord)


class SourceFailure(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    product: str
    sub_domain: Literal['SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time']
    event_type: Literal['OOS', 'OOC', 'CPK', 'CPM']
    code: Literal['result_unavailable'] = 'result_unavailable'


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
    complete: bool = True
    failed_sources: list[SourceFailure] = Field(default_factory=list)


class AnomalyResponse(BaseModel):
    data: list[PublicRecord]
    meta: ResultMeta
