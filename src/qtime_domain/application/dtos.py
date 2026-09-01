"""Validated inputs for Q-Time report use cases."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


Shop = Literal["ARRAY", "OLED", "TP"]


class QTimeQuery(BaseModel):
    """A half-open Q-Time detail query."""

    model_config = ConfigDict(frozen=True)

    start_time: datetime
    end_time: datetime
    shop: Shop
    step_desc: str = Field(min_length=1)
    products: tuple[str, ...] = ()

    @field_validator("step_desc", mode="before")
    @classmethod
    def normalize_step_description(cls, value: object) -> str:
        return str(value).strip()

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
