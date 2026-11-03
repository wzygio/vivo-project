"""Native cache identity covers changed files and independent configured paths."""

from types import SimpleNamespace

from app.sections.indicator_domain.point_test.dashboard import fetch_report_payload


def test_full_path_version_and_manual_refresh_invalidate_cache(monkeypatch):
    calls = []

    def build(path):
        def read():
            calls.append(str(path))
            return {"value": len(calls)}
        return SimpleNamespace(read_report=read)

    monkeypatch.setattr("app.sections.indicator_domain.point_test.dashboard.build_point_test_service", build)
    fetch_report_payload.clear()
    try:
        assert fetch_report_payload("one.xlsx", (1, 100)) == {"value": 1}
        assert fetch_report_payload("one.xlsx", (1, 100)) == {"value": 1}
        assert fetch_report_payload("one.xlsx", (2, 100)) == {"value": 2}
        assert fetch_report_payload("other.xlsx", (2, 100)) == {"value": 3}
        assert fetch_report_payload("other.xlsx", (2, 101)) == {"value": 4}
        fetch_report_payload.clear()
        assert fetch_report_payload("other.xlsx", (2, 101)) == {"value": 5}
    finally:
        fetch_report_payload.clear()
