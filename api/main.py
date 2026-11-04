"""HTTP entry point; constructing the app does not query sources or open a DB."""
from datetime import datetime, time, timedelta
from contextlib import asynccontextmanager
import hashlib
import hmac
import logging
from collections.abc import AsyncIterator, Awaitable
from typing import Annotated, Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Query, Request, Security
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from starlette.responses import Response

from api.schemas import AnomalyQuery, AnomalyResponse, ResultMeta
from api.settings import ApiClient, ApiSettings, load_clients
from api.pagination import CursorError, PublicSnapshots, SnapshotExpired, SnapshotLimit
from api.limits import BoundedReads, RateLimit, ReadBusy, ReadTimeout
from api.query import AllSourcesUnavailable, ForbiddenQuery, InvalidQuery, resolve_query


def create_app(*, service: Any = None, clients_provider: Callable = load_clients,
               enabled_products: Callable | None = None, clock: Callable | None = None,
               settings: ApiSettings | None = None) -> FastAPI:
    if enabled_products is None:
        from src.shared_kernel.config import ConfigLoader
        enabled_products = lambda: ConfigLoader.get_enabled_products(strict=True)
    if service is None:
        from api.composition import build_service
        service = build_service()
    settings = settings or ApiSettings()
    snapshots = PublicSnapshots(settings)
    reads = BoundedReads(settings.max_concurrent_reads, settings.read_timeout_seconds)
    client_limit = RateLimit(settings.requests_per_minute)
    network_limit = RateLimit(settings.requests_per_minute * 5)
    clock = clock or (lambda: datetime.now(ZoneInfo('Asia/Shanghai')))
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        reads.close()

    app = FastAPI(title='异常明细与能力不达项 API', version='1.4.0',
                  description='省略 event_type 时查询各分类支持的全部事件。CPK/CPM 对象编号包含对应完整 ISO 周的全部 Sheet ID。未明确命中边界统一为 spec。',
                  docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    key_header = APIKeyHeader(name='X-API-Key', auto_error=False)

    @app.middleware('http')
    async def public_headers(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request.state.request_id = uuid4().hex
        host = request.client.host if request.client else 'unknown'
        if not network_limit.allow(host):
            response = JSONResponse(status_code=429, headers={'Retry-After': '60'}, content={'error': {'code': 'rate_limit_exceeded', 'message': '请求过于频繁，请稍后重试。'}})
        elif len(str(request.url)) > 4096:
            response = JSONResponse(status_code=422, content={'error': {'code': 'invalid_request', 'message': '查询参数不符合接口约定。'}})
        else:
            response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Request-ID'] = request.state.request_id
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, headers=exc.headers,
                            content={'error': {'code': str(exc.detail), 'message': '请求未能完成，请检查请求或稍后重试。'},
                                     'request_id': request.state.request_id})

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={'error': {'code': 'invalid_request', 'message': '查询参数不符合接口约定。'},
                                                     'request_id': request.state.request_id})

    @app.exception_handler(AllSourcesUnavailable)
    async def no_sources(request: Request, exc: AllSourcesUnavailable) -> JSONResponse:
        return JSONResponse(status_code=503, content={
            'error': {'code': 'result_unavailable', 'message': '所选来源暂时无法提供有效结果。',
                      'failed_sources': [failure.model_dump() for failure in exc.failures]},
            'request_id': request.state.request_id,
        })

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        logging.getLogger(__name__).error('API_UNEXPECTED_FAILURE request_id=%s cause=%s', request.state.request_id, type(exc).__name__)
        return JSONResponse(status_code=500, headers={'Cache-Control': 'no-store', 'X-Request-ID': request.state.request_id},
                            content={'error': {'code': 'internal_error', 'message': '服务暂时无法完成请求。'}, 'request_id': request.state.request_id})

    def authenticate(key: Annotated[str | None, Security(key_header)]) -> ApiClient:
        if not key or len(key) > 1024:
            raise HTTPException(401, 'invalid_credentials')
        try:
            clients = clients_provider()
        except ValueError:
            raise HTTPException(503, 'service_unavailable') from None
        digest = hashlib.sha256(key.encode()).hexdigest()
        principal = next((item for item in clients if hmac.compare_digest(item.key_hash, digest)), None)
        if principal is None:
            raise HTTPException(401, 'invalid_credentials')
        if not client_limit.allow(principal.client_id):
            raise HTTPException(429, 'rate_limit_exceeded', headers={'Retry-After': '60'})
        return principal

    @app.get('/health/live')
    def health() -> dict[str, str]:
        return {'status': 'ok'}

    @app.get('/health/ready', include_in_schema=False)
    def ready() -> dict[str, str]:
        try:
            if not clients_provider() or not enabled_products():
                raise ValueError('Configuration unavailable')
        except (ValueError, OSError, TypeError):
            raise HTTPException(503, 'service_unavailable') from None
        return {'status': 'ok'}

    @app.get('/api/v1/sheet-anomalies', response_model=AnomalyResponse)
    async def anomalies(query: Annotated[AnomalyQuery, Query()],
                  principal: Annotated[ApiClient, Security(authenticate)]) -> AnomalyResponse:
        try:
            enabled = enabled_products()
        except (ValueError, OSError):
            raise HTTPException(503, 'service_unavailable') from None
        now = clock().astimezone(ZoneInfo('Asia/Shanghai'))
        try:
            plan = resolve_query(query, principal, enabled, now.date())
        except ForbiddenQuery:
            raise HTTPException(403, 'forbidden') from None
        except InvalidQuery:
            raise HTTPException(422, 'invalid_request') from None
        start = datetime.combine(plan.start_date, time.min, ZoneInfo('Asia/Shanghai'))
        end = datetime.combine(plan.end_date + timedelta(days=1), time.min, ZoneInfo('Asia/Shanghai'))
        try:
            result = await reads.run(plan.read, service, now.date(), settings.max_snapshot_bytes)
            public = result.records
        except ReadBusy:
            raise HTTPException(429, 'reader_busy', headers={'Retry-After': '5'}) from None
        except ReadTimeout:
            raise HTTPException(503, 'result_unavailable') from None
        except (ValueError, KeyError, OSError, TypeError) as exc:
            logging.getLogger(__name__).warning('API_RESULT_UNAVAILABLE cause=%s', type(exc).__name__)
            raise HTTPException(503, 'result_unavailable') from None
        try:
            page, next_cursor = snapshots.page(public, query, principal, plan.snapshot_context(result.failed_sources))
        except CursorError:
            raise HTTPException(422, 'invalid_cursor') from None
        except SnapshotExpired:
            raise HTTPException(410, 'snapshot_expired') from None
        except SnapshotLimit:
            raise HTTPException(503, 'result_unavailable') from None
        return AnomalyResponse(data=page, meta=ResultMeta(
            status='alert' if public else 'ok', returned_count=len(page),
            has_more=next_cursor is not None, next_cursor=next_cursor, period_start=start.isoformat(),
            period_end_exclusive=end.isoformat(), read_at=now.isoformat(),
            complete=not result.failed_sources, failed_sources=result.failed_sources,
        ))

    @app.get('/openapi.json', include_in_schema=False)
    def schema(principal: Annotated[ApiClient, Security(authenticate)]) -> dict[str, Any]:
        return app.openapi()

    return app
