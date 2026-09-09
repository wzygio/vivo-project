import pytest
from streamlit.testing.v1 import AppTest


@pytest.mark.parametrize("status,kind", [("stale", "warning"), ("unknown", "warning"), ("unavailable", "error"), ("fresh", "caption")])
def test_yield_health_is_visible_before_results(status, kind):
    app = AppTest.from_string(
        "from app.sections.yield_domain.data_health import render_yield_data_health\n"
        f"render_yield_data_health({{'status': '{status}', 'refreshed_at': '2026-09-01', 'source_start': '2026-06-01', 'source_end': '2026-09-01'}})"
    ).run()
    assert not app.exception
    assert len(app.get(kind)) == 1
