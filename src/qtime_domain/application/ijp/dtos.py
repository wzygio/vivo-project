"""Validated inputs for the IJP overflow report use cases."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_FILTER_FIELDS = (
    "product_names",
    "product_codes",
    "sub_prod_types",
    "lines",
    "equipments",
    "glass_ids",
    "codes",
    "panel_locations",
    "picis",
    "cycles",
)


class IjpQuery(BaseModel):
    """A closed-interval IJP overflow query; empty filters mean no filtering."""

    model_config = ConfigDict(frozen=True)

    start_time: datetime
    end_time: datetime
    product_names: tuple[str, ...] = ()
    product_codes: tuple[str, ...] = ()
    sub_prod_types: tuple[str, ...] = ()
    lines: tuple[str, ...] = ()
    equipments: tuple[str, ...] = ()
    glass_ids: tuple[str, ...] = ()
    codes: tuple[str, ...] = ()
    panel_locations: tuple[str, ...] = ()
    picis: tuple[str, ...] = ()
    cycles: tuple[str, ...] = ()
    target: float | None = None
    detail_limit: int = Field(default=5000, gt=0)

    @field_validator(*_FILTER_FIELDS, mode="before")
    @classmethod
    def normalize_filters(cls, value: object) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            items = value.replace("，", ",").replace("\n", ",").split(",")
        else:
            items = list(value)  # type: ignore[arg-type]
        normalized = (str(item).strip() for item in items if item is not None)
        return tuple(dict.fromkeys(item for item in normalized if item))

    @model_validator(mode="after")
    def validate_time_window(self) -> "IjpQuery":
        if self.end_time < self.start_time:
            raise ValueError("结束时间不能早于开始时间")
        return self
