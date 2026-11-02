"""Bounded immutable public snapshots; process-local cursors are authenticated."""
from __future__ import annotations

from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import hmac
import json
import secrets
from threading import RLock
import time

from api.schemas import AnomalyQuery
from api.settings import ApiClient, ApiSettings


class CursorError(ValueError):
    pass


class SnapshotExpired(ValueError):
    pass


class SnapshotLimit(ValueError):
    pass


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class Snapshot:
    binding: str
    fingerprint: str
    payload: bytes
    expires_at: float


class PublicSnapshots:
    def __init__(self, settings: ApiSettings) -> None:
        self._settings = settings
        self._snapshots: OrderedDict[str, Snapshot] = OrderedDict()
        self._secret = secrets.token_bytes(32)
        self._lock = RLock()
        self._bytes = 0

    def page(self, records: list[dict], query: AnomalyQuery, principal: ApiClient,
             reference_week: str) -> tuple[list[dict], str | None]:
        binding = _digest((query.model_dump(mode='json', exclude={'cursor'}), vars(principal), reference_week))
        fingerprint = _digest(records)
        with self._lock:
            now = time.monotonic()
            for identifier, snapshot in list(self._snapshots.items()):
                if snapshot.expires_at <= now:
                    self._remove(identifier)
            if query.cursor:
                identifier, offset = self._decode(query.cursor)
                snapshot = self._snapshots.get(identifier)
                if snapshot is None or snapshot.binding != binding or snapshot.fingerprint != fingerprint:
                    raise SnapshotExpired('Public result changed or expired')
                rows = json.loads(snapshot.payload)
                if offset >= len(rows):
                    raise CursorError('Invalid cursor offset')
            else:
                payload = json.dumps(records, ensure_ascii=False, allow_nan=False).encode()
                if len(payload) > self._settings.max_snapshot_bytes:
                    raise SnapshotLimit('Public result too large')
                if query.limit is None or len(records) <= query.limit:
                    return records, None
                while self._snapshots and (
                    len(self._snapshots) >= self._settings.max_snapshots
                    or self._bytes + len(payload) > self._settings.max_snapshot_bytes
                ):
                    self._remove(next(iter(self._snapshots)))
                identifier, offset = secrets.token_hex(16), 0
                self._snapshots[identifier] = Snapshot(binding, fingerprint, payload, now+self._settings.snapshot_ttl_seconds)
                self._bytes += len(payload)
                rows = records
            if query.limit is None:
                raise CursorError('Cursor requires page size')
            end = offset + query.limit
            next_cursor = self._encode(identifier, end) if end < len(rows) else None
            return rows[offset:end], next_cursor

    def _remove(self, identifier: str) -> None:
        self._bytes -= len(self._snapshots.pop(identifier).payload)

    def _encode(self, identifier: str, offset: int) -> str:
        payload = json.dumps([identifier, offset], separators=(',', ':')).encode()
        signature = hmac.new(self._secret, payload, hashlib.sha256).digest()
        return urlsafe_b64encode(signature+payload).decode()

    def _decode(self, token: str) -> tuple[str, int]:
        try:
            decoded = urlsafe_b64decode(token.encode())
            signature, payload = decoded[:32], decoded[32:]
            if not hmac.compare_digest(signature, hmac.new(self._secret, payload, hashlib.sha256).digest()):
                raise ValueError('Invalid signature')
            identifier, offset = json.loads(payload)
            if not isinstance(identifier, str) or type(offset) is not int or offset <= 0:
                raise ValueError('Invalid position')
            return identifier, offset
        except (ValueError, TypeError, UnicodeError, json.JSONDecodeError):
            raise CursorError('Invalid cursor') from None
