"""Request-local reuse over native, bounded Streamlit caches.

Values live only for one report operation. A new request still receives the
private deserialized copy supplied by cache_data; nothing mutable is shared
between users or retained after the operation exits.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from inspect import signature
import json
from pathlib import Path
from typing import Any, Iterator


_report_values: ContextVar[dict | None] = ContextVar("yield_report_values", default=None)
_GENERATION_PREFIX = 'yield-source-generation:'


class YieldSourceGenerationChanged(RuntimeError):
    """The caller's captured report no longer matches the source being read."""


def bind_source_generation(source_signature: str, health: dict) -> str:
    """Derived caches follow actual source health; raw reads keep their root key."""
    return _GENERATION_PREFIX + json.dumps({'source': source_signature, 'health': health},
                                            sort_keys=True, ensure_ascii=False)


def _source_key(value: str) -> str:
    if value.startswith(_GENERATION_PREFIX):
        return json.loads(value[len(_GENERATION_PREFIX):])['source']
    return value


@contextmanager
def report_cache_scope() -> Iterator[None]:
    """Nested report operations reuse the caller's private memo."""
    if _report_values.get() is not None:
        yield
        return
    token = _report_values.set({})
    try:
        yield
    finally:
        _report_values.reset(token)


def _key_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _key_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_key_value(item) for item in value]
    return value


def _reuse_report_value(cached, *, source_only: bool = False):
    """Skip repeat deserialization within an operation; explicit ports bypass it."""
    parameters = signature(cached)

    @wraps(cached)
    def invoke(*args, **kwargs):
        bound = parameters.bind(*args, **kwargs)
        bound.apply_defaults()
        expected = None
        if source_only:
            requested = bound.arguments['snapshot_signature']
            if requested.startswith(_GENERATION_PREFIX):
                expected = json.loads(requested[len(_GENERATION_PREFIX):])['health']
            bound.arguments['snapshot_signature'] = _source_key(bound.arguments['snapshot_signature'])
        memo = _report_values.get()
        if memo is None:
            result = cached(*bound.args, **bound.kwargs)
        elif bound.arguments.get("_data_port") is not None:
            result = cached(*bound.args, **bound.kwargs)
        else:
            values = {name: _key_value(value) for name, value in bound.arguments.items()
                      if not name.startswith("_")}
            key = (cached.__module__, cached.__qualname__,
                   json.dumps(values, sort_keys=True, ensure_ascii=False, default=str),
                   id(bound.arguments.get("_db_manager")))
            if key not in memo:
                memo[key] = cached(*bound.args, **bound.kwargs)
            result = memo[key]
        if expected is not None:
            actual = result.attrs.get('data_health', {}) if hasattr(result, 'attrs') else result
            if actual != expected:
                raise YieldSourceGenerationChanged('YIELD_REPORT_GENERATION_CHANGED')
        return result

    def clear(*args, **kwargs):
        memo = _report_values.get()
        if memo is not None:
            # A cache clear must also discard this operation's retained copy.
            for key in list(memo):
                if key[:2] == (cached.__module__, cached.__qualname__):
                    del memo[key]
        if not args and not kwargs:
            return cached.clear()
        bound = parameters.bind(*args, **kwargs)
        bound.apply_defaults()
        if source_only:
            bound.arguments['snapshot_signature'] = _source_key(bound.arguments['snapshot_signature'])
        return cached.clear(*bound.args, **bound.kwargs)

    invoke.clear = clear
    return invoke


def reuse_report_value(cached):
    return _reuse_report_value(cached)


def reuse_source_value(cached):
    return _reuse_report_value(cached, source_only=True)


def report_operation(function):
    """Run one application/presentation operation with scoped input reuse."""
    @wraps(function)
    def invoke(*args, **kwargs):
        with report_cache_scope():
            return function(*args, **kwargs)
    return invoke
