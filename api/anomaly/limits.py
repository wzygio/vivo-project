"""Finite request accounting and finite synchronous file-reader work."""
from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, RLock
import time
from typing import Any, Callable


class ReadBusy(ValueError):
    pass


class ReadTimeout(ValueError):
    pass


class RateLimit:
    def __init__(self, limit: int, max_keys: int = 4096) -> None:
        self._limit, self._max_keys = limit, max_keys
        self._entries: dict[str, deque] = {}
        self._lock = RLock()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        with self._lock:
            for name, times in list(self._entries.items()):
                while times and times[0] <= now - 60:
                    times.popleft()
                if not times:
                    del self._entries[name]
            if key not in self._entries:
                if len(self._entries) >= self._max_keys:
                    return False
                self._entries[key] = deque()
            times = self._entries[key]
            if len(times) >= self._limit:
                return False
            times.append(now)
            return True


class BoundedReads:
    """Timeout returns promptly; a still-running reader retains its slot.

    This cannot forcibly cancel an Office call. It prevents an unbounded queue
    or replacement threads when a file reader is slow or stuck.
    """
    def __init__(self, workers: int, timeout: float) -> None:
        self._slots = BoundedSemaphore(workers)
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='alert-api-read')
        self._timeout = timeout

    async def run(self, function: Callable, *args: Any) -> Any:
        if not self._slots.acquire(blocking=False):
            raise ReadBusy('Reader capacity reached')
        try:
            future = self._executor.submit(function, *args)
        except RuntimeError:
            self._slots.release()
            raise ReadBusy('Reader unavailable') from None
        future.add_done_callback(lambda _: self._slots.release())
        try:
            return await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(future)), self._timeout)
        except TimeoutError:
            raise ReadTimeout('Reader timed out') from None

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)
