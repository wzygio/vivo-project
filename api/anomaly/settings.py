"""Reloadable, fail-closed client authorization; credentials never enter YAML."""
from __future__ import annotations

from dataclasses import dataclass
import json
from math import isfinite
import os
import re

SUB_DOMAINS = ('SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time')


@dataclass(frozen=True)
class ApiClient:
    client_id: str
    key_hash: str
    products: tuple[str, ...]
    factories: tuple[str, ...]
    sub_domains: tuple[str, ...] = SUB_DOMAINS

    def __post_init__(self) -> None:
        if (not isinstance(self.client_id, str) or not self.client_id.strip()
                or not isinstance(self.key_hash, str) or not re.fullmatch(r'[0-9a-f]{64}', self.key_hash)):
            raise ValueError('Invalid API client configuration')
        for scope in (self.products, self.factories, self.sub_domains):
            if not isinstance(scope, tuple) or not scope or any(not isinstance(item, str) or not item.strip() for item in scope):
                raise ValueError('Empty API authorization scope')
        if set(self.sub_domains) - set(SUB_DOMAINS):
            raise ValueError('Unknown API authorization scope')


@dataclass(frozen=True)
class ApiSettings:
    requests_per_minute: int = 60
    max_concurrent_reads: int = 2
    # A default request can read 84 product/domain/event combinations, including
    # cold imports and protected files. Keep a finite total budget below the
    # portable caller's 120s wait, rather than applying a single-source budget.
    read_timeout_seconds: float = 90
    snapshot_ttl_seconds: float = 300
    max_snapshots: int = 64
    max_snapshot_bytes: int = 16 * 1024 * 1024

    def __post_init__(self) -> None:
        if any(isinstance(value, bool) or not isinstance(value, (int, float))
               or not isfinite(value) or value <= 0 for value in vars(self).values()):
            raise ValueError('API limits must be positive')
        for name in ('requests_per_minute', 'max_concurrent_reads', 'max_snapshots', 'max_snapshot_bytes'):
            if type(getattr(self, name)) is not int:
                raise ValueError('API counts must be positive integers')


def load_clients() -> tuple[ApiClient, ...]:
    """Read each request so revocation does not depend on a server restart."""
    try:
        raw = json.loads(os.environ.get('VIVO_API_CLIENTS', '[]'))
        if not isinstance(raw, list) or len(raw) > 256:
            raise ValueError('Invalid client list')
        for item in raw:
            if not isinstance(item, dict) or set(item) - {'client_id','key_sha256','products','factories','sub_domains'}:
                raise ValueError('Invalid API client schema')
            if any(not isinstance(item.get(name), list) for name in ('products','factories')):
                raise ValueError('Invalid API authorization scope')
            if 'sub_domains' in item and not isinstance(item['sub_domains'], list):
                raise ValueError('Invalid API authorization scope')
        clients = tuple(ApiClient(
            client_id=item['client_id'], key_hash=item['key_sha256'],
            products=tuple(item['products']), factories=tuple(item['factories']),
            sub_domains=tuple(item.get('sub_domains', SUB_DOMAINS)),
        ) for item in raw)
        if len({c.client_id for c in clients}) != len(clients) or len({c.key_hash for c in clients}) != len(clients):
            raise ValueError('Duplicate API client')
        return clients
    except (KeyError, TypeError, ValueError):
        raise ValueError('API client configuration unavailable') from None
