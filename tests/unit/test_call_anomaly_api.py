"""Direct caller behavior with synthetic credentials and mocked HTTP only."""
from io import BytesIO
import json
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import HTTPRedirectHandler

import pytest

from tools import call_anomaly_api as client


FAKE_KEY = "offline-direct-caller-key"


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.delenv("VIVO_API_KEY", raising=False)


def response(*, complete=True, rows=None):
    return {"data": rows or [], "meta": {
        "complete": complete, "has_more": False,
        "failed_sources": [] if complete else [
            {"product": "M626", "sub_domain": "Yield", "event_type": "OOS", "code": "result_unavailable"}],
    }}


@pytest.mark.parametrize("as_json", [False, True])
def test_standardized_fields_override_arguments_without_sending_unrelated_fields(monkeypatch, as_json):
    std = {"product": "M678", "event_type": "", "step_id": "00100", "parameter_name": "中文 空格", "limit": 1}
    payload = response(rows=[{"对象编号": ["0001", "0002"]}])
    requests = []

    def open_request(request, timeout):
        requests.append(request)
        assert timeout == 120 and request.method == "GET" and request.data is None
        assert request.get_header("X-api-key") == FAKE_KEY
        redirected = HTTPRedirectHandler().redirect_request(
            request, None, 302, "Found", {}, "http://redirect-fixture/next")
        assert redirected.get_header("X-api-key") is None
        return BytesIO(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(client, "urlopen", open_request)
    result = client.main(std_result=json.dumps(std) if as_json else std,
                         api_key=FAKE_KEY, product="M626", event_type="CPK", sub_domain="SPC")
    assert result["success"] is True and result["data"] == payload["data"]
    assert parse_qs(urlsplit(requests[0].full_url).query) == {
        "product": ["M678"], "sub_domain": ["SPC"], "event_type": ["CPK"],
        "step_id": ["00100"], "parameter_name": ["中文 空格"],
    }
    assert FAKE_KEY not in requests[0].full_url


@pytest.mark.parametrize("complete", [True, False])
def test_default_scope_empty_results_and_partial_sources(monkeypatch, complete):
    monkeypatch.setenv("API_KEY", FAKE_KEY)
    payload = response(complete=complete)

    def open_request(request, timeout):
        assert urlsplit(request.full_url).query == ""
        assert request.get_header("X-api-key") == FAKE_KEY
        return BytesIO(json.dumps(payload).encode())

    monkeypatch.setattr(client, "urlopen", open_request)
    result = client.main()
    assert result["success"] is True and result["data"] == []
    assert result["meta"] == payload["meta"]


@pytest.mark.parametrize("std", ["invalid", "[]", [], 123])
def test_invalid_standardized_input_does_not_send_query(monkeypatch, std):
    monkeypatch.setattr(client, "urlopen", lambda *a, **k: pytest.fail("Unexpected request"))
    assert client.main(std_result=std, api_key=FAKE_KEY)["success"] is False


@pytest.mark.parametrize("key", [None, "bad\nkey", "密钥", 123])
def test_invalid_credentials_do_not_send_query(monkeypatch, key):
    monkeypatch.setattr(client, "urlopen", lambda *a, **k: pytest.fail("Unexpected request"))
    assert client.main(api_key=key)["success"] is False


@pytest.mark.parametrize("failure", [
    HTTPError("http://fixture", 401, FAKE_KEY, {}, BytesIO(FAKE_KEY.encode())),
    HTTPError("http://fixture", 503, FAKE_KEY, {}, BytesIO(FAKE_KEY.encode())),
    URLError(FAKE_KEY), TimeoutError(FAKE_KEY),
])
def test_network_errors_return_safe_failures(monkeypatch, failure):
    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(client, "urlopen", fail)
    result = client.main(api_key=FAKE_KEY)
    assert result["success"] is False and result["data"] == []
    assert FAKE_KEY not in json.dumps(result)


@pytest.mark.parametrize("payload", [
    b"invalid-json", b"[]", b'{}',
    json.dumps({"data": [], "meta": {"complete": True, "failed_sources": [], "has_more": True}}).encode(),
    json.dumps({"data": [], "meta": {"complete": False, "failed_sources": [], "has_more": False}}).encode(),
])
def test_invalid_or_unfinished_responses_are_not_successful(monkeypatch, payload):
    monkeypatch.setattr(client, "urlopen", lambda *a, **k: BytesIO(payload))
    result = client.main(api_key=FAKE_KEY)
    assert result["success"] is False and result["data"] == []
