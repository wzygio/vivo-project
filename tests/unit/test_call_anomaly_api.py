"""Minimal caller checks without a listener or production credentials."""
from io import BytesIO
import json
from pathlib import Path
import runpy
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import HTTPRedirectHandler

import pytest

from tools import call_anomaly_api as client


KEY = "synthetic-minimal-caller-key"


@pytest.fixture(autouse=True)
def isolate_credentials(monkeypatch):
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.delenv("VIVO_API_KEY", raising=False)


@pytest.mark.parametrize("complete", [True, False])
def test_params_headers_and_unmodified_results(monkeypatch, complete):
    payload = {"data": [{"对象编号": ["00001", "00002"]}], "meta": {
        "complete": complete, "failed_sources": [] if complete else [{"code": "result_unavailable"}]}}
    params = {"sub_domain": "AOI_TT", "step_id": "00100", "parameter_name": "中文 空格"}
    before = dict(params)

    def open_request(request, timeout):
        assert timeout == 120 and request.method == "GET" and request.data is None
        assert request.get_header("X-api-key") == KEY
        assert parse_qs(urlsplit(request.full_url).query) == {k: [v] for k, v in params.items()}
        assert KEY not in request.full_url
        redirected = HTTPRedirectHandler().redirect_request(
            request, None, 302, "Found", {}, "http://redirect-fixture/next")
        assert redirected.get_header("X-api-key") is None
        return BytesIO(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(client, "urlopen", open_request)
    assert client.main(params=params, api_key=KEY) == payload
    assert params == before


@pytest.mark.parametrize("failure", [
    HTTPError("http://fixture", 401, "Unauthorized", {}, BytesIO()),
    TimeoutError("fixture timeout"),
])
def test_request_errors_propagate_without_wrapper(monkeypatch, failure):
    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(client, "urlopen", fail)
    with pytest.raises(type(failure)):
        client.main(api_key=KEY)


@pytest.mark.parametrize("from_environment", [True, False])
def test_direct_run_uses_default_scope_and_does_not_print_key(monkeypatch, capsys, from_environment):
    if from_environment:
        monkeypatch.setenv("API_KEY", KEY)

    def prompt(message):
        assert not from_environment
        return KEY

    payload = {"data": [], "meta": {"complete": True, "failed_sources": []}}
    requests = []

    def open_request(request, timeout):
        requests.append(request)
        assert request.get_header("X-api-key") == KEY
        assert urlsplit(request.full_url).query == ""
        return BytesIO(json.dumps(payload).encode())

    monkeypatch.setattr("getpass.getpass", prompt)
    monkeypatch.setattr("urllib.request.urlopen", open_request)
    runpy.run_path(str(Path(client.__file__)), run_name="__main__")
    output = capsys.readouterr().out
    assert len(requests) == 1 and json.loads(output) == payload
    assert KEY not in output
