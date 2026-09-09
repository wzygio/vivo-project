"""Keep explicit outbound adapters outside shared Streamlit data caches."""

from functools import wraps
from inspect import signature


def cache_default_port(cached):
    """Preserve production caching; explicit ``_data_port`` calls are isolated.

    Apply outside ``st.cache_data``. Keep its clear hook and wrapped metadata
    discoverable so existing page refresh registration continues to work.
    """
    original = cached.__wrapped__
    parameters = signature(original)

    @wraps(cached)
    def dispatch(*args, **kwargs):
        bound = parameters.bind(*args, **kwargs)
        if bound.arguments.get("_data_port") is not None:
            return original(*args, **kwargs)
        return cached(*args, **kwargs)

    dispatch.clear = cached.clear
    return dispatch
