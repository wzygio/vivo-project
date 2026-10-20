"""Template normalization closes owned documents and preserves user sessions."""
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest
import pythoncom
import win32com.client

from src.inline_domain.infrastructure.shared import ppt_template


def xml_package():
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        archive.writestr("test.xml", "<test/>")
    return output.getvalue()


def test_standard_template_requires_no_office(monkeypatch, tmp_path):
    source = tmp_path / "template.bin"
    content = xml_package()
    source.write_bytes(content)
    monkeypatch.setattr(win32com.client, "DispatchEx", lambda *_: pytest.fail("Office started"))
    assert ppt_template.read_ppt_template(source, temp_root=tmp_path / "work") == content
    assert not (tmp_path / "work").exists()


@pytest.mark.parametrize("existing", ["", "13104", "[13104,13105]"])
@pytest.mark.parametrize("outcome", ["success", "save_failure", "interrupt", "open_failure"])
def test_protected_template_cleanup_and_preserved_sessions(monkeypatch, tmp_path, existing, outcome):
    source = tmp_path / "protected.bin"
    source.write_bytes(b"\0" * 16)
    calls = []
    valid = xml_package()
    read_bytes = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda path: valid if path.name == "template.pptx" else read_bytes(path))
    monkeypatch.setattr(ppt_template.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=existing))
    monkeypatch.setattr(pythoncom, "CoInitialize", lambda: calls.append("init"))
    monkeypatch.setattr(pythoncom, "CoUninitialize", lambda: calls.append("uninit"))

    def save(path, kind):
        calls.append("save")
        assert kind == 24
        if outcome == "save_failure":
            raise OSError("synthetic save failure")
        if outcome == "interrupt":
            raise KeyboardInterrupt

    document = SimpleNamespace(SaveCopyAs=save, Close=lambda: calls.append("close"))

    def open_doc(path, read_only, untitled, with_window):
        assert (read_only, untitled, with_window) == (True, False, False)
        calls.append("open")
        if outcome == "open_failure":
            raise OSError("synthetic open failure")
        return document

    app = SimpleNamespace(Presentations=SimpleNamespace(Open=open_doc, Count=0),
                          Quit=lambda: calls.append("quit"))
    monkeypatch.setattr(win32com.client, "DispatchEx", lambda *_: app)
    if outcome == "success":
        assert ppt_template.read_ppt_template(source, temp_root=tmp_path / "work") == valid
    else:
        with pytest.raises(KeyboardInterrupt if outcome == "interrupt" else OSError):
            ppt_template.read_ppt_template(source, temp_root=tmp_path / "work")
    assert ("close" in calls) == (outcome != "open_failure")
    assert ("quit" in calls) == (not existing)
    assert calls[-1] == "uninit"
    assert not list((tmp_path / "work").glob("inline_ppt_template_*"))
    assert source.read_bytes() == b"\0" * 16
