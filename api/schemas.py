"""Explicit public contract. Internal columns cannot survive serialization."""
from __future__ import annotations

from typing import Literal
from datetime import date
import re
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator


def normalize_sheet_ids(value: object) -> list[str]:
    """Preserve textual Sheet identity while canonicalizing an entire membership."""
    if not isinstance(value, list) or not value:
        raise ValueError('Capability Sheet membership is required')
    members: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item.strip() or len(item.strip()) > 128:
            raise ValueError('Invalid capability Sheet identity')
        members.add(item.strip())
    return sorted(members)


def public_record_sort_key(record: dict) -> tuple[str, ...]:
    """Use visible business keys; capability ordering is independent of members."""
    identity = '' if record.get('对象类型') == 'capability' else record.get('对象编号')
    return tuple(str(value or '') for value in (
        record.get('发生时间'), record.get('产品型号'), record.get('分类'), identity,
        record.get('发生站点'), record.get('监控参数'), record.get('命中边界'),
        record.get('对象类型'), record.get('部门'), record.get('异常事件'),
        record.get('统计周期类型'), record.get('统计周期'),
    ))


class AnomalyQuery(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    product: str | None = Field(default=None, min_length=1, max_length=64, pattern=r'^[A-Za-z0-9_-]+$')
    sub_domain: Literal['SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time'] | None = None
    event_type: Literal['OOS', 'OOC', 'CPK', 'CPM'] | None = Field(
        default=None, description='省略时查询各分类支持的全部事件；填写时只查询该事件。')
    factory: str | None = Field(default=None, min_length=1, max_length=64)
    step_id: str | None = Field(default=None, min_length=1, max_length=128)
    layer: str | None = Field(default=None, min_length=1, max_length=256)
    parameter_name: str | None = Field(default=None, min_length=1, max_length=512)
    start_date: date | None = None
    end_date: date | None = None
    entity_type: Literal['sheet', 'lot', 'capability'] | None = None
    entity_id: str | None = Field(
        default=None, min_length=1, max_length=128,
        description='Sheet/Lot 编号；CPK/CPM 按 Sheet ID 成员匹配，返回该项目的完整编号列表。',
    )
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
        if self.sub_domain in ('Yield', 'Q-Time') and self.event_type not in (None, 'OOS'):
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
    product: str = Field(alias='产品型号', min_length=1, max_length=64)
    occurred_at: str = Field(alias='发生时间', min_length=1, max_length=64)
    boundary: str = Field(default='spec', alias='命中边界', min_length=1, max_length=64,
                          description='已明确的边界名称；LOWER、NULL 和未明确值统一为 spec。')

    @field_validator('boundary', mode='before')
    @classmethod
    def unspecified_boundary(cls, value: object) -> object:
        if value is None:
            return 'spec'
        if isinstance(value, str):
            text = value.strip()
            return 'spec' if text.lower() in {'', 'null', 'lower', 'spec'} else text
        return value

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
    entity_id: str = Field(alias='对象编号', min_length=1, max_length=128)


class CapabilityRecord(_PublicRecordBase):
    """A complete weekly parameter aggregate, never an invented Sheet/Lot."""

    event_type: Literal['CPK', 'CPM'] = Field(alias='异常事件')
    entity_type: Literal['capability'] = Field(alias='对象类型')
    entity_id: list[str] = Field(
        alias='对象编号', min_length=1,
        description='同产品、厂别、站点、参数及统计周内全部 Sheet ID，去重后按文本升序排列。',
    )
    metric_value: float = Field(alias='指标值', allow_inf_nan=False)
    lower_bound: float = Field(alias='达标下限', allow_inf_nan=False)
    period_type: Literal['week'] = Field(alias='统计周期类型')
    period_label: str = Field(alias='统计周期', pattern=r'^[0-9]{4}-W[0-9]{2}$')
    period_start_date: date = Field(alias='统计开始日期')
    period_end_date: date = Field(alias='统计截止日期')

    @field_validator('entity_id', mode='before')
    @classmethod
    def complete_sheet_membership(cls, value: object) -> list[str]:
        return normalize_sheet_ids(value)

    @model_validator(mode='after')
    def complete_week(self) -> CapabilityRecord:
        if self.sub_domain != 'SPC':
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
