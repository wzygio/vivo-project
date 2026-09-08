"""Validated IJP report configuration."""

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.indicator_domain.core.ijp.printer_summary import PRINTER_CODES


class IjpSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    codes: tuple[str, ...] = PRINTER_CODES
    c3dm1_minimum: float = Field(default=0.9, ge=0.9, le=1.0)

    @field_validator("codes")
    @classmethod
    def validate_codes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if not values or len(set(values)) != len(values):
            raise ValueError("IJP codes 必须非空且不能重复")
        if not set(values) <= set(PRINTER_CODES):
            raise ValueError("IJP codes 仅支持 C3DM0 至 C3DM5")
        return tuple(code for code in PRINTER_CODES if code in values)
