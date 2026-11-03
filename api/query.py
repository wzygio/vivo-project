"""Resolve authorized selections and read them within one bounded worker call."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import json
import logging
from typing import Any

from api.schemas import AnomalyQuery, PUBLIC_RECORD_ADAPTER, SourceFailure
from api.settings import ApiClient
from api.pagination import SnapshotLimit
from src.shared_kernel.infrastructure.api_workbook_cache import WorkbookVersionError


class ForbiddenQuery(ValueError):
    pass


class InvalidQuery(ValueError):
    pass


class AllSourcesUnavailable(RuntimeError):
    def __init__(self, failures: list[SourceFailure]) -> None:
        super().__init__('No selected publication available')
        self.failures = failures


@dataclass(frozen=True)
class QueryResult:
    records: list[dict]
    failed_sources: list[SourceFailure]


@dataclass(frozen=True)
class QueryPlan:
    queries: tuple[AnomalyQuery, ...]
    factories: tuple[str, ...]
    start_date: date
    end_date: date

    def read(self, service: Any, reference_date: date, max_public_bytes: int) -> QueryResult:
        public = []
        failures = []
        public_bytes = 2  # JSON array brackets; same serialization as snapshots.
        for query in self.queries:
            try:
                try:
                    rows = service.read(query, self.factories, reference_date)
                except WorkbookVersionError:
                    # Retry the complete source once, including current public
                    # policy. Never reuse a prefix or bypass workbook versions.
                    # The outer bounded-read deadline still covers both calls.
                    rows = service.read(query, self.factories, reference_date)
                # Validate the entire source before adding any rows. A failed
                # source must not leave a prefix in an apparently partial result.
                projected = [PUBLIC_RECORD_ADAPTER.validate_python(row).model_dump(mode='json', by_alias=True) for row in rows]
            except (ValueError, KeyError, OSError, TypeError) as exc:
                logging.getLogger(__name__).warning('API_SOURCE_UNAVAILABLE product=%s sub_domain=%s cause=%s',
                                                   query.product, query.sub_domain, type(exc).__name__)
                failures.append(SourceFailure(product=query.product, sub_domain=query.sub_domain,
                                              event_type=query.event_type))
                continue
            if projected:
                public_bytes += len(json.dumps(projected, ensure_ascii=False, allow_nan=False).encode()) - 2
                if public:
                    public_bytes += 2
                if public_bytes > max_public_bytes:
                    raise SnapshotLimit('Public result too large')
            public.extend(projected)
        if failures and len(failures) == len(self.queries):
            raise AllSourcesUnavailable(failures)
        public.sort(key=lambda row: tuple(str(row.get(name) or '') for name in (
            '发生时间', '产品型号', '分类', '对象编号', '发生站点', '监控参数', '命中边界', '对象类型',
        )), reverse=True)
        return QueryResult(public, failures)

    def snapshot_context(self, failures: list[SourceFailure]) -> str:
        return json.dumps({
            'sources': [(q.product, q.sub_domain, q.event_type) for q in self.queries],
            'factories': self.factories, 'start': self.start_date.isoformat(), 'end': self.end_date.isoformat(),
            'failed_sources': [failure.model_dump() for failure in failures],
        }, sort_keys=True)


def resolve_query(query: AnomalyQuery, principal: ApiClient, enabled: list[str], reference_date: date) -> QueryPlan:
    if (query.product is not None and (query.product not in principal.products or query.product not in enabled)
            or query.sub_domain is not None and query.sub_domain not in principal.sub_domains
            or query.factory is not None and query.factory not in principal.factories):
        raise ForbiddenQuery('Unauthorized query scope')
    products = tuple(dict.fromkeys(p for p in enabled if p in principal.products))
    if not products:
        raise ForbiddenQuery('No enabled authorized products')
    if query.product:
        products = (query.product,)
    if query.event_type in ('CPK', 'CPM'):
        if 'SPC' not in principal.sub_domains:
            raise ForbiddenQuery('SPC capability scope is not authorized')
        domains = ('SPC',)
    else:
        domains = (query.sub_domain,) if query.sub_domain else principal.sub_domains
    if query.event_type == 'OOC':
        domains = tuple(d for d in domains if d not in ('Yield', 'Q-Time'))
    factories = (query.factory,) if query.factory else principal.factories
    this_monday = reference_date - timedelta(days=reference_date.weekday())
    start = query.start_date or this_monday - timedelta(days=7)
    end = query.end_date or this_monday - timedelta(days=1)
    if start > end or end == date.max:
        raise InvalidQuery('Invalid date range')
    queries = tuple(query.model_copy(update={'product': product, 'sub_domain': domain,
                                            'start_date': start, 'end_date': end})
                    for product in products for domain in dict.fromkeys(domains))
    return QueryPlan(queries, factories, start, end)
