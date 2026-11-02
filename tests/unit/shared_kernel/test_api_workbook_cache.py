"""Bounded read-only workbook cache behavior through its callable interface."""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import os

import pandas as pd
import pytest


def test_same_workbook_version_is_parsed_once_and_returns_independent_copies(tmp_path):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache

    path = tmp_path / "result.xlsx"
    path.write_bytes(b"fixture")
    original = path.read_bytes()
    calls = []

    def read(source: Path):
        calls.append(source)
        return {"P1": pd.DataFrame({"sheet_id": ["0000123"]})}

    reader = BoundedWorkbookCache(reader=read)
    stat = path.stat()
    first = reader(str(path), stat.st_mtime_ns, stat.st_size)
    first["P1"].loc[0, "sheet_id"] = "changed"
    second = reader(str(path), stat.st_mtime_ns, stat.st_size)

    assert second["P1"]["sheet_id"].tolist() == ["0000123"]
    assert calls == [path.resolve()]
    assert path.read_bytes() == original


def _call(cache, path):
    stat = path.stat()
    return cache(str(path), stat.st_mtime_ns, stat.st_size)


def test_default_reader_preserves_text_identifiers_and_source_bytes(tmp_path):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache

    path = tmp_path / "published.xlsx"
    pd.DataFrame({"sheet_id": ["0000123"], "step_id": ["00100"]}).to_excel(path, index=False)
    original = path.read_bytes()
    value = _call(BoundedWorkbookCache(), path)
    assert value["Sheet1"].to_dict("records") == [{"sheet_id": "0000123", "step_id": "00100"}]
    assert path.read_bytes() == original


def test_changed_source_rejects_stale_request_and_parses_new_version(tmp_path):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache, WorkbookVersionError

    path = tmp_path / "published.xlsx"
    path.write_bytes(b"first")
    old = path.stat()
    calls = []

    def parse(source):
        calls.append(source.read_bytes())
        return {"P1": pd.DataFrame({"value": [source.read_bytes().decode()]})}

    cache = BoundedWorkbookCache(reader=parse)
    assert _call(cache, path)["P1"]["value"].tolist() == ["first"]
    path.write_bytes(b"second version")
    with pytest.raises(WorkbookVersionError):
        cache(str(path), old.st_mtime_ns, old.st_size)
    assert _call(cache, path)["P1"]["value"].tolist() == ["second version"]
    assert calls == [b"first", b"second version"]


def test_mutation_during_parse_is_not_cached(tmp_path):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache, WorkbookVersionError

    path = tmp_path / "published.xlsx"
    path.write_bytes(b"first")
    calls = []

    def parse(source):
        calls.append(source.read_bytes())
        if len(calls) == 1:
            source.write_bytes(b"second version")
        return {"P1": pd.DataFrame({"value": ["parsed"]})}

    cache = BoundedWorkbookCache(reader=parse)
    with pytest.raises(WorkbookVersionError):
        _call(cache, path)
    assert _call(cache, path)["P1"]["value"].tolist() == ["parsed"]
    assert calls == [b"first", b"second version"]


def test_missing_or_failed_new_version_never_falls_back_to_cached_result(tmp_path):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache

    path = tmp_path / "published.xlsx"
    path.write_bytes(b"good")
    original_stat = path.stat()
    calls = []

    def parse(source):
        calls.append(source.read_bytes())
        if source.read_bytes() == b"broken new version":
            raise ValueError("invalid fixture")
        return {"P1": pd.DataFrame({"value": ["good"]})}

    cache = BoundedWorkbookCache(reader=parse)
    _call(cache, path)
    path.write_bytes(b"broken new version")
    with pytest.raises(ValueError, match="invalid fixture"):
        _call(cache, path)
    path.unlink()
    with pytest.raises(FileNotFoundError):
        cache(str(path), original_stat.st_mtime_ns, original_stat.st_size)
    # A failed parse did not remove the earlier valid cache entry.
    path.write_bytes(b"good")
    os.utime(path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    assert _call(cache, path)["P1"]["value"].tolist() == ["good"]
    assert calls == [b"good", b"broken new version"]


def test_file_size_limit_prevents_parser_work(tmp_path):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache, WorkbookLimitError

    path = tmp_path / "large.xlsx"
    path.write_bytes(b"too large")
    calls = []
    cache = BoundedWorkbookCache(max_file_bytes=2, reader=lambda source: calls.append(source))
    with pytest.raises(WorkbookLimitError):
        _call(cache, path)
    assert calls == []


@pytest.mark.parametrize("limit", ["rows", "bytes"])
def test_parsed_workbook_limits_reject_instead_of_partially_caching(tmp_path, limit):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache, WorkbookLimitError

    path = tmp_path / "result.xlsx"
    path.write_bytes(b"fixture")
    calls = []

    def parse(source):
        calls.append(source)
        return {
            "P1": pd.DataFrame({"value": ["x" * 100, "y" * 100]}),
            "P2": pd.DataFrame({"value": ["z" * 100, "w" * 100]}),
        }

    settings = {"max_rows": 3} if limit == "rows" else {"max_bytes": 100}
    cache = BoundedWorkbookCache(reader=parse, **settings)
    for _ in range(2):
        with pytest.raises(WorkbookLimitError):
            _call(cache, path)
    assert len(calls) == 2


def test_entry_limit_evicts_least_recently_used_file(tmp_path):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache

    paths = [tmp_path / f"{index}.xlsx" for index in range(3)]
    for path in paths:
        path.write_bytes(b"fixture")
    calls = []

    def parse(source):
        calls.append(source.name)
        return {"P1": pd.DataFrame({"value": [source.name]})}

    cache = BoundedWorkbookCache(max_entries=2, reader=parse)
    for index in [0, 1, 0, 2, 1]:
        _call(cache, paths[index])
    assert calls == ["0.xlsx", "1.xlsx", "2.xlsx", "1.xlsx"]
    cache.clear()
    _call(cache, paths[1])
    assert calls[-2:] == ["1.xlsx", "1.xlsx"]


def test_byte_budget_evicts_prior_file_even_below_entry_limit(tmp_path):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache

    paths = [tmp_path / f"{index}.xlsx" for index in range(2)]
    for path in paths:
        path.write_bytes(b"fixture")
    calls = []

    def parse(source):
        calls.append(source.name)
        return {"P1": pd.DataFrame({"value": ["x" * 512]})}

    cache = BoundedWorkbookCache(max_bytes=1000, reader=parse)
    for index in [0, 1, 0]:
        _call(cache, paths[index])
    assert calls == ["0.xlsx", "1.xlsx", "0.xlsx"]


def test_simultaneous_cold_requests_share_one_parse(tmp_path):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache

    path = tmp_path / "result.xlsx"
    path.write_bytes(b"fixture")
    calls = []

    def parse(source):
        calls.append(source)
        return {"P1": pd.DataFrame({"sheet_id": ["0000123"]})}

    cache = BoundedWorkbookCache(reader=parse)
    with ThreadPoolExecutor(max_workers=8) as pool:
        values = list(pool.map(lambda _: _call(cache, path), range(8)))
    assert len(calls) == 1
    assert all(value["P1"]["sheet_id"].tolist() == ["0000123"] for value in values)
    assert len({id(value["P1"]) for value in values}) == 8


def test_standard_parse_failure_uses_existing_protected_workbook_adapter(tmp_path, monkeypatch):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache
    from src.shared_kernel.utils import excel_tools

    path = tmp_path / "protected.xlsx"
    path.write_bytes(b"protected fixture")
    calls = []

    def encrypted(*args, **kwargs):
        raise ValueError("encrypted fixture")

    def read_protected(source):
        calls.append(source)
        return {"P1": pd.DataFrame({"sheet_id": ["0000123"]})}

    monkeypatch.setattr(pd, "read_excel", encrypted)
    monkeypatch.setattr(excel_tools, "read_workbook_sheets", read_protected)
    assert _call(BoundedWorkbookCache(), path)["P1"]["sheet_id"].tolist() == ["0000123"]
    assert calls == [path.resolve()]


@pytest.mark.parametrize("settings", [
    {"max_entries": 0}, {"max_bytes": -1}, {"max_rows": True}, {"max_file_bytes": 1.5},
])
def test_invalid_cache_limits_are_rejected(settings):
    from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache

    with pytest.raises(ValueError):
        BoundedWorkbookCache(**settings)
