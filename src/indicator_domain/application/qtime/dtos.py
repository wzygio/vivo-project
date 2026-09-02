"""Validated inputs for Q-Time report use cases."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


Shop = Literal["ARRAY", "OLED", "TP"]


@dataclass(frozen=True, slots=True)
class QTimeStepOption:
    """A selectable station path with its original endpoint codes."""

    step_desc: str
    f_step: str
    t_step: str

    @property
    def label(self) -> str:
        return f"{self.f_step} → {self.t_step}｜{self.step_desc}"


class QTimeQuery(BaseModel):
    """A half-open Q-Time detail query."""

    model_config = ConfigDict(frozen=True)

    start_time: datetime
    end_time: datetime
    shop: Shop
    step_descriptions: tuple[str, ...] = Field(min_length=1)
    products: tuple[str, ...] = ()

    @field_validator("step_descriptions", mode="before")
    @classmethod
    def normalize_step_descriptions(cls, value: object) -> tuple[str, ...]:
        if value is None:
            return ()
        values = (value,) if isinstance(value, str) else value
        normalized = (str(description).strip() for description in values)
        return tuple(dict.fromkeys(item for item in normalized if item))

    @field_validator("products", mode="before")
    @classmethod
    def normalize_products(cls, value: object) -> tuple[str, ...]:
        if value is None:
            return ()
        normalized = (str(product).strip() for product in value)
        return tuple(dict.fromkeys(product for product in normalized if product))

    @model_validator(mode="after")
    def validate_time_window(self) -> "QTimeQuery":
        if self.end_time <= self.start_time:
            raise ValueError("结束时间必须晚于开始时间")
        return self
