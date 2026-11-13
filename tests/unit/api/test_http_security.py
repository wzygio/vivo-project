"""Authorization and recovery through the approved, in-process HTTP seam."""

from datetime import datetime
from hashlib import sha256
import json
from types import SimpleNamespace
import time
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pytest

from api.main import create_app
from api.settings import ApiClient, ApiSettings

ENDPOINT = "/api/v1/sheet-anomalies"
KEY = "synthetic-security-test-key-not-a-real-credential"
OTHER_KEY = "synthetic-other-system-key-not-a-real-credential"
PARAMS = {"product": "P1", "sub_domain": "SPC", "event_type": "OOS"}
HEADERS = {"X-API-Key": KEY}
NOW = datetime(2026, 11, 2, 10, tzinfo=ZoneInfo("Asia/Shanghai"))


class SyntheticService:
    def __init__(self) -> None:
        self.calls = []
        self.failure = None

    def read(self, query, factories, reference_date):
        self.calls.append((query, factories, reference_date))
        if self.failure:
            raise self.failure
        return [
            {"分类": query.sub_domain, "发生站点": "00100", "部门": factories[0], "层别": "PI2",
             "监控参数": "THK", "异常事件": query.event_type, "对象类型": "sheet", "对象编号": identifier,
             "产品型号": query.product, "发生时间": "2026-10-28T09:30:00+08:00"}
            for identifier in ("0001", "0002", "0003")
        ]


def principal(key=KEY, **changes) -> ApiClient:
    return ApiClient(**{
        "client_id": "system-one", "key_hash": sha256(key.encode()).hexdigest(),
        "products": ("P1",), "factories": ("ARRAY",), "sub_domains": ("SPC",), **changes,
    })


@pytest.fixture
def environment():
    service = SyntheticService()
    state = SimpleNamespace(clients=[principal()], enabled=["P1", "P2"])
    app = create_app(service=service, clients_provider=lambda: tuple(state.clients),
                     enabled_products=lambda: state.enabled, clock=lambda: NOW)
    return SimpleNamespace(app=app, service=service, state=state)


def test_malformed_factory_scope_environment_fails_closed(monkeypatch) -> None:
    malformed = [{"client_id": "fixture", "key_sha256": sha256(KEY.encode()).hexdigest(),
                  "products": ["P1"], "factories": "ARRAY", "sub_domains": ["SPC"]}]
    monkeypatch.setenv("VIVO_API_CLIENTS", json.dumps(malformed))
    service = SyntheticService()
    app = create_app(service=service, enabled_products=lambda: ["P1"], clock=lambda: NOW)
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(ENDPOINT, params=PARAMS, headers=HEADERS)
    assert response.status_code == 503
    assert service.calls == []


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": ""}, {"X-API-Key": "   "},
                                     {"X-API-Key": "wrong-synthetic-key"}, {"X-API-Key": "x" * 1025}])
def test_invalid_credentials_are_rejected_before_reading_sources(environment, headers) -> None:
    with TestClient(environment.app) as client:
        response = client.get(ENDPOINT, params=PARAMS, headers=headers)
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "invalid_credentials"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-request-id"] == response.json()["request_id"]
    assert environment.service.calls == []


@pytest.mark.parametrize("changes", [{"product": "P2"}, {"factory": "OLED"}, {"sub_domain": "CTQ"}])
def test_explicit_unauthorized_scope_is_forbidden_before_source_access(environment, changes) -> None:
    with TestClient(environment.app) as client:
        response = client.get(ENDPOINT, params={**PARAMS, **changes}, headers=HEADERS)
    assert response.status_code == 403
    assert environment.service.calls == []


def test_disabled_product_remains_forbidden_even_when_client_has_permission(environment) -> None:
    environment.state.enabled = []
    with TestClient(environment.app) as client:
        response = client.get(ENDPOINT, params=PARAMS, headers=HEADERS)
    assert response.status_code == 403
    assert environment.service.calls == []


def test_omitted_factory_means_only_the_principal_authorized_subset(environment) -> None:
    environment.state.clients = [principal(factories=("ARRAY", "OLED"))]
    with TestClient(environment.app) as client:
        response = client.get(ENDPOINT, params=PARAMS, headers=HEADERS)
    assert response.status_code == 200
    assert environment.service.calls[0][1] == ("ARRAY", "OLED")


@pytest.mark.parametrize("changes,private_value", [
    ({"admin": "PRIVATE_OVERRIDE"}, "PRIVATE_OVERRIDE"),
    ({"raw": "PRIVATE_RAW"}, "PRIVATE_RAW"),
    ({"path": "PRIVATE_WORKBOOK"}, "PRIVATE_WORKBOOK"),
    ({"product": "../../PRIVATE_PATH"}, "PRIVATE_PATH"),
    ({"sub_domain": "PRIVATE_SOURCE"}, "PRIVATE_SOURCE"),
    ({"event_type": "PRIVATE_EVENT"}, "PRIVATE_EVENT"),
    ({"limit": 0}, "0"),
    ({"limit": 501}, "501"),
    ({"limit": "PRIVATE_LIMIT"}, "PRIVATE_LIMIT"),
    ({"cursor": "x" * 2049}, "x" * 30),
])
def test_invalid_query_is_safe_and_does_not_echo_raw_values(environment, changes, private_value) -> None:
    with TestClient(environment.app) as client:
        response = client.get(ENDPOINT, params={**PARAMS, **changes}, headers=HEADERS)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    if len(private_value) > 3:
        assert private_value not in response.text
    assert environment.service.calls == []


def test_yield_cannot_request_a_nonexistent_ooc_publication(environment) -> None:
    environment.state.clients = [principal(sub_domains=("SPC", "Yield"))]
    with TestClient(environment.app) as client:
        response = client.get(ENDPOINT, params={**PARAMS, "sub_domain": "Yield", "event_type": "OOC"}, headers=HEADERS)
    assert response.status_code == 422
    assert environment.service.calls == []


def test_openapi_requires_authentication_and_uses_chinese_public_aliases(environment) -> None:
    with TestClient(environment.app) as client:
        assert client.get("/openapi.json").status_code == 401
        response = client.get("/openapi.json", headers=HEADERS)
    assert response.status_code == 200
    document = response.json()
    properties = document["components"]["schemas"]["AnomalyRecord"]["properties"]
    assert {"分类", "发生站点", "部门", "层别", "监控参数", "异常事件", "对象类型", "对象编号"} <= set(properties)
    assert "flag" not in properties and "key_hash" not in properties
    operation = document["paths"][ENDPOINT]["get"]
    assert operation["security"]
    assert document["components"]["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"
    assert environment.service.calls == []


def test_health_endpoints_do_not_construct_database_or_read_sources(environment, monkeypatch) -> None:
    import pandas as pd
    from src.shared_kernel.infrastructure import db_handler

    def forbidden(*args, **kwargs):
        raise AssertionError("Health checks must not access databases")
    monkeypatch.setattr(db_handler, "DatabaseManager", forbidden)
    monkeypatch.setattr(pd, "read_sql", forbidden)
    with TestClient(environment.app) as client:
        assert client.get("/health/live").json() == {"status": "ok"}
        ready = client.get("/health/ready")
    assert ready.status_code == 200 and ready.json() == {"status": "ok"}
    assert environment.service.calls == []


@pytest.mark.parametrize("raw", [
    "not-json", "{}", "[123]",
    json.dumps([{"client_id": 123, "key_sha256": sha256(KEY.encode()).hexdigest(), "products": ["P1"], "factories": ["ARRAY"]}]),
    json.dumps([{"client_id": "fixture", "key_sha256": sha256(KEY.encode()).hexdigest(), "products": "P1", "factories": ["ARRAY"]}]),
    json.dumps([{"client_id": "fixture", "key_sha256": sha256(KEY.encode()).hexdigest(), "products": ["P1"], "factories": {"ARRAY": True}}]),
    json.dumps([{"client_id": "fixture", "key_sha256": "invalid-hash", "products": ["P1"], "factories": ["ARRAY"]}]),
])
def test_malformed_client_environment_is_unavailable_without_source_access(monkeypatch, raw) -> None:
    monkeypatch.setenv("VIVO_API_CLIENTS", raw)
    service = SyntheticService()
    app = create_app(service=service, enabled_products=lambda: ["P1"], clock=lambda: NOW)
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(ENDPOINT, params=PARAMS, headers=HEADERS)
        ready = client.get("/health/ready")
    assert response.status_code == 503
    assert ready.status_code == 503
    assert KEY not in response.text and "key_sha256" not in response.text
    assert service.calls == []


def test_credential_revocation_takes_effect_on_the_next_request(environment) -> None:
    with TestClient(environment.app) as client:
        assert client.get(ENDPOINT, params=PARAMS, headers=HEADERS).status_code == 200
        environment.state.clients = []
        response = client.get(ENDPOINT, params=PARAMS, headers=HEADERS)
    assert response.status_code == 401
    assert len(environment.service.calls) == 1


@pytest.mark.parametrize("changes", [{"limit": 2}, {"entity_id": "0001"}, {"factory": "ARRAY"}])
def test_cursor_cannot_be_reused_with_changed_query_or_page_size(environment, changes) -> None:
    params = {**PARAMS, "limit": 1}
    with TestClient(environment.app) as client:
        first = client.get(ENDPOINT, params=params, headers=HEADERS)
        cursor = first.json()["meta"]["next_cursor"]
        response = client.get(ENDPOINT, params={**params, **changes, "cursor": cursor}, headers=HEADERS)
    assert response.status_code == 410


def test_cursor_is_invalidated_when_authorization_changes(environment) -> None:
    params = {**PARAMS, "limit": 1}
    with TestClient(environment.app) as client:
        cursor = client.get(ENDPOINT, params=params, headers=HEADERS).json()["meta"]["next_cursor"]
        environment.state.clients = [principal(factories=("ARRAY", "OLED"))]
        response = client.get(ENDPOINT, params={**params, "cursor": cursor}, headers=HEADERS)
    assert response.status_code == 410


def test_cursor_is_not_transferable_to_another_authorized_identity(environment) -> None:
    environment.state.clients.append(principal(OTHER_KEY, client_id="system-two"))
    params = {**PARAMS, "limit": 1}
    with TestClient(environment.app) as client:
        cursor = client.get(ENDPOINT, params=params, headers=HEADERS).json()["meta"]["next_cursor"]
        response = client.get(ENDPOINT, params={**params, "cursor": cursor}, headers={"X-API-Key": OTHER_KEY})
    assert response.status_code == 410


def test_tampered_cursor_is_rejected_without_echo(environment) -> None:
    with TestClient(environment.app) as client:
        response = client.get(ENDPOINT, params={**PARAMS, "limit": 1, "cursor": "PRIVATE_BAD_CURSOR"}, headers=HEADERS)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_cursor"
    assert "PRIVATE_BAD_CURSOR" not in response.text


def test_expired_public_snapshot_requires_restart_from_page_one(environment) -> None:
    app = create_app(service=environment.service, clients_provider=lambda: tuple(environment.state.clients),
                     enabled_products=lambda: ["P1"], settings=ApiSettings(snapshot_ttl_seconds=.01), clock=lambda: NOW)
    params = {**PARAMS, "limit": 1}
    with TestClient(app) as client:
        cursor = client.get(ENDPOINT, params=params, headers=HEADERS).json()["meta"]["next_cursor"]
        time.sleep(.04)
        response = client.get(ENDPOINT, params={**params, "cursor": cursor}, headers=HEADERS)
    assert response.status_code == 410


def test_evicted_public_snapshot_requires_restart_from_page_one(environment) -> None:
    app = create_app(service=environment.service, clients_provider=lambda: tuple(environment.state.clients),
                     enabled_products=lambda: ["P1"], settings=ApiSettings(max_snapshots=1), clock=lambda: NOW)
    params = {**PARAMS, "limit": 1}
    with TestClient(app) as client:
        cursor = client.get(ENDPOINT, params=params, headers=HEADERS).json()["meta"]["next_cursor"]
        assert client.get(ENDPOINT, params=params, headers=HEADERS).status_code == 200
        response = client.get(ENDPOINT, params={**params, "cursor": cursor}, headers=HEADERS)
    assert response.status_code == 410


def test_server_restart_does_not_accept_previous_process_cursor(environment) -> None:
    params = {**PARAMS, "limit": 1}
    with TestClient(environment.app) as client:
        cursor = client.get(ENDPOINT, params=params, headers=HEADERS).json()["meta"]["next_cursor"]
    restarted = create_app(service=environment.service, clients_provider=lambda: tuple(environment.state.clients),
                           enabled_products=lambda: ["P1"], clock=lambda: NOW)
    with TestClient(restarted) as client:
        response = client.get(ENDPOINT, params={**params, "cursor": cursor}, headers=HEADERS)
    assert response.status_code in (410, 422)


@pytest.mark.parametrize("settings", [
    {"read_timeout_seconds": float("nan")},
    {"read_timeout_seconds": float("inf")},
    {"snapshot_ttl_seconds": float("inf")},
    {"max_snapshot_bytes": float("inf")},
    {"requests_per_minute": 1.5},
    {"max_snapshots": True},
])
def test_nonfinite_or_invalid_limit_configuration_is_rejected(settings) -> None:
    with pytest.raises(ValueError):
        create_app(service=SyntheticService(), clients_provider=lambda: (principal(),),
                   enabled_products=lambda: ["P1"], settings=ApiSettings(**settings))


def test_unexpected_error_returns_safe_json_with_request_identifier(environment) -> None:
    environment.service.failure = RuntimeError("PRIVATE_DRIVER_PASSWORD_AND_PATH")
    with TestClient(environment.app, raise_server_exceptions=False) as client:
        response = client.get(ENDPOINT, params=PARAMS, headers=HEADERS)
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal_error"
    assert response.json()["request_id"] == response.headers["x-request-id"]
    assert "PRIVATE_DRIVER_PASSWORD_AND_PATH" not in response.text
    assert response.headers["cache-control"] == "no-store"


def test_default_per_client_limit_enforces_sixty_requests_per_minute(environment) -> None:
    with TestClient(environment.app) as client:
        for _ in range(60):
            assert client.get(ENDPOINT, params=PARAMS, headers=HEADERS).status_code == 200
        response = client.get(ENDPOINT, params=PARAMS, headers=HEADERS)
    assert response.status_code == 429
    assert int(response.headers["retry-after"]) > 0
    assert len(environment.service.calls) == 60
