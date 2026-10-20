"""Read standard or enterprise-protected PPT templates without modifying them."""
from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from zipfile import is_zipfile
from io import BytesIO

logger = logging.getLogger(__name__)
_OFFICE_LOCK = Lock()


def read_ppt_template(path: Path, *, temp_root: Path) -> bytes:
    """Return Open XML bytes. Only the owned document/session is closed.

    Office's enterprise hooks may protect SaveCopyAs output after close, so bytes
    are captured while the read-only source is open. Temporary files are scoped
    to this call and never become maintained resources.
    """
    content = path.read_bytes()
    if is_zipfile(BytesIO(content)):
        return content
    with _OFFICE_LOCK:
        return _read_protected_template(path, temp_root)


def _read_protected_template(path: Path, temp_root: Path) -> bytes:
    import pythoncom
    import win32com.client

    # PowerPoint may reuse an existing process even for DispatchEx. In that case
    # never call Application.Quit, which would close the user's presentations.
    before = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command",
         "@(Get-Process -Name POWERPNT -ErrorAction SilentlyContinue).Id | ConvertTo-Json -Compress"],
        check=True, capture_output=True, text=True, timeout=15,
        creationflags=subprocess.CREATE_NO_WINDOW,
    ).stdout.strip()
    existing = json.loads(before) if before else []
    temp_root.mkdir(parents=True, exist_ok=True)
    pythoncom.CoInitialize()
    app = document = None
    try:
        with TemporaryDirectory(prefix="inline_ppt_template_", dir=temp_root) as directory:
            app = win32com.client.DispatchEx("PowerPoint.Application")
            document = app.Presentations.Open(str(path.resolve()), True, False, False)
            target = Path(directory) / "template.pptx"
            document.SaveCopyAs(str(target), 24)  # ppSaveAsOpenXMLPresentation
            content = target.read_bytes()
            if not is_zipfile(BytesIO(content)):
                raise ValueError("PowerPoint did not expose a readable Open XML template")
            return content
    finally:
        try:
            if document is not None:
                document.Close()
        finally:
            try:
                if app is not None and not existing and app.Presentations.Count == 0:
                    app.Quit()
            finally:
                pythoncom.CoUninitialize()
