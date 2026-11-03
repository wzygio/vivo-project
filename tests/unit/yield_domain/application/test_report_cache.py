from concurrent.futures import ThreadPoolExecutor

from src.yield_domain.application.report_cache import report_cache_scope, reuse_report_value


def test_report_memo_lives_only_inside_one_operation_and_binds_arguments():
    calls = []

    def cached(product, revision="v1", _data_port=None):
        calls.append(product)
        return {"values": []}

    cached.clear = lambda: None
    read = reuse_report_value(cached)
    with report_cache_scope():
        first = read("P")
        assert read(product="P", revision="v1") is first
        with report_cache_scope():
            assert read("P") is first
        assert read("Q") is not first
        assert read("P", revision="v2") is not first
    with report_cache_scope():
        assert read("P") is not first
    assert calls == ["P", "Q", "P", "P"]


def test_explicit_adapters_and_failed_calls_are_never_memoized():
    calls = []

    def cached(product, _data_port=None):
        calls.append(product)
        if product == "fail":
            raise ValueError("failure")
        return object()

    cached.clear = lambda: None
    read = reuse_report_value(cached)
    with report_cache_scope():
        port = object()
        assert read("P", _data_port=port) is not read("P", _data_port=port)
        for _ in range(2):
            try:
                read("fail")
            except ValueError:
                pass
    assert calls == ["P", "P", "fail", "fail"]


def test_concurrent_requests_get_independent_mutable_results():
    def cached(product):
        return {"values": []}

    cached.clear = lambda: None
    read = reuse_report_value(cached)

    def operation(number):
        with report_cache_scope():
            result = read("P")
            result["values"].append(number)
            return read("P")["values"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(operation, [1, 2])) == [[1], [2]]


def test_cache_clear_discards_the_operation_local_copy():
    def cached(product):
        return object()

    cached.clear = lambda *args, **kwargs: None
    read = reuse_report_value(cached)
    with report_cache_scope():
        first = read('P')
        read.clear('P')
        assert read('P') is not first
