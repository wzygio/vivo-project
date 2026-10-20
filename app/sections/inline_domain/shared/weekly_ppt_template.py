"""Cache native template bytes by resource identity for all Inline weekly exports."""
from pathlib import Path

import streamlit as st

from src.inline_domain.infrastructure.shared.ppt_template import read_ppt_template
from src.shared_kernel.config import ConfigLoader


@st.cache_data(show_spinner=False, max_entries=2, ttl=ConfigLoader.get_cache_ttl_seconds())
def _cached_template(path: str, mtime_ns: int, size: int) -> bytes:
    return read_ppt_template(Path(path), temp_root=ConfigLoader.get_project_root() / "output" / "tmp")


def load_weekly_ppt_template(resource_key: str) -> bytes:
    path = ConfigLoader.get_domain_resource_path("inline_domain", resource_key)
    stat = path.stat()
    return _cached_template(str(path), stat.st_mtime_ns, stat.st_size)
