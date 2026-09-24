"""Chamber residence report use case and its single outbound contract."""

from datetime import date
from math import isfinite
from typing import Protocol
from uuid import uuid4

from src.indicator_domain.core.qtime.chamber import build_chamber_report


class ChamberQTimeSource(Protocol):
    def read(self, *, as_of: date) -> dict: ...

    def cache_signature(self) -> tuple: ...


class ChamberQTimeService:
    def __init__(
        self, source: ChamberQTimeSource, *, enabled_products: tuple[str, ...],
        cache_namespace: str | None = None,
        target_seconds: float | None = None,
    ) -> None:
        self._source = source
        self.enabled_products = tuple(enabled_products)
        self._target_seconds = float(target_seconds) if target_seconds is not None else None
        if self._target_seconds is not None and (
            not isfinite(self._target_seconds) or self._target_seconds <= 0
        ):
            raise ValueError('Chamber target_seconds must be finite and positive')
        # Explicit adapters are isolated by default; the production composition
        # opts into a stable namespace and includes engine/file identities below.
        self._cache_namespace = cache_namespace or uuid4().hex

    def cache_signature(self) -> tuple:
        return (self._cache_namespace, self.enabled_products, self._target_seconds, self._source.cache_signature())

    def load(self, *, as_of: date) -> dict:
        source = self._source.read(as_of=as_of)
        targets = source['targets']
        if self._target_seconds is not None:
            targets = targets.assign(target_seconds=self._target_seconds)
        report = build_chamber_report(
            source['measurements'], source['products'], targets, self.enabled_products,
        )
        return {**report, 'invalid_rows': source['invalid_rows']}
