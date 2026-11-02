"""Windows readers can briefly prevent atomic state replacement."""
import json
import os
import threading
import time

import pytest

from tools import anomaly_api_service as host


def test_host_state_retries_transient_windows_sharing_violation(tmp_path, monkeypatch):
    path = tmp_path / 'state.json'
    host.save(path, {'status': 'old'})
    replace = host.os.replace
    attempts = []

    def sharing_violation(source, destination):
        attempts.append(destination)
        if len(attempts) == 1:
            raise PermissionError('transient shared read handle')
        return replace(source, destination)

    monkeypatch.setattr(host.os, 'replace', sharing_violation)
    host.save(path, {'status': 'running'})
    assert json.loads(path.read_text()) == {'status': 'running'}
    assert len(attempts) == 2
    assert not list(tmp_path.glob('*.tmp'))


@pytest.mark.skipif(os.name != 'nt', reason='Windows sharing semantics')
def test_real_windows_reader_handle_allows_state_save_after_release(tmp_path):
    import ctypes
    from ctypes import wintypes

    path = tmp_path / 'state.json'
    host.save(path, {'status': 'old'})
    create = ctypes.windll.kernel32.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                       wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = ctypes.windll.kernel32.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    handle = create(str(path), 0x80000000, 3, None, 3, 0x80, None)  # Read/write sharing, no delete sharing.
    assert handle != ctypes.c_void_p(-1).value

    def release_reader():
        time.sleep(.15)
        close(handle)

    reader = threading.Thread(target=release_reader)
    reader.start()
    try:
        host.save(path, {'status': 'running'})
    finally:
        reader.join(timeout=2)
    assert not reader.is_alive()
    assert json.loads(path.read_text()) == {'status': 'running'}
