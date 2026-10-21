"""Validated, immutable settings for daily chamber point decoration."""

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ChamberDecorationSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra='forbid')

    enabled: bool = Field(default=False, strict=True)
    mode: Literal['random', 'remove'] = 'remove'
    start_date: date = date(2026, 10, 12)
    end_date: date | Literal['today'] = 'today'
    threshold_seconds: float = Field(default=300, gt=0, allow_inf_nan=False, strict=True)
    minimum_seconds: float = Field(default=200, ge=0, allow_inf_nan=False, strict=True)
    point_count_threshold: int = Field(default=50, ge=0, strict=True)
    group_by: tuple[Literal['prod_code', 'line', 'chamber'], ...] = ('line', 'chamber')
    random_seed: int = Field(default=0, strict=True)

    @model_validator(mode='after')
    def validate_window_and_groups(self) -> 'ChamberDecorationSettings':
        if self.mode == 'random' and self.minimum_seconds >= self.threshold_seconds:
            raise ValueError('chamber_point_decoration.minimum_seconds must be below threshold_seconds')
        if self.end_date != 'today' and self.start_date > self.end_date:
            raise ValueError('chamber_point_decoration.start_date must not exceed end_date')
        if len(set(self.group_by)) != len(self.group_by):
            raise ValueError('chamber_point_decoration.group_by must not contain duplicates')
        return self

    def window_end(self, as_of: date) -> date:
        return as_of if self.end_date == 'today' else min(as_of, self.end_date)
