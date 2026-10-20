"""OS-backed publication locks, released automatically when a process exits."""
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import tempfile
import time
from typing import Iterator


@contextmanager
def publication_lock(path: Path, timeout_seconds: float = 30.0) -> Iterator[None]:
    identity = hashlib.sha256(str(path.resolve()).casefold().encode()).hexdigest()
    lock_path = Path(tempfile.gettempdir()) / f"vivo-publication-{identity}.lock"
    # Stable lock inode: do not unlink while another process may be waiting on it.
    with lock_path.open("a+b") as handle:
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"0")
            handle.flush()
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Publication lock timed out") from None
                time.sleep(0.05)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
