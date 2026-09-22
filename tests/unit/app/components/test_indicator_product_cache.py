from collections import Counter
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace

from app.components import indicator_cache, page_header


def test_refresh_only_recomputes_selected_indicator_and_product(tmp_path: Path):
    calls = Counter()

    @lru_cache
    def compute(indicator, product, signature):
        calls[indicator, product] += 1
        return calls[indicator, product]

    def render():
        for indicator in ("yield_lot_oos", "spc_cpk_trend"):
            for product in ("M626", "M673"):
                signature = indicator_cache.build_indicator_product_cache_signature(
                    "v1", product, (indicator,), revision_dir=tmp_path,
                )
                compute(indicator, product, signature)

    render()
    indicator_cache.bump_indicator_product_revision("yield_lot_oos", "m626", revision_dir=tmp_path)
    render()
    assert calls == {("yield_lot_oos", "M626"): 2, ("yield_lot_oos", "M673"): 1,
                     ("spc_cpk_trend", "M626"): 1, ("spc_cpk_trend", "M673"): 1}


def test_scoped_hard_reset_reloads_code_without_clearing_other_product_caches(monkeypatch):
    bumped = []
    configs = []
    reloads = []
    session_state = {
        "code_update_pending": True,
        "view_model_cache": object(),
        "matrix_detail_example": object(),
        "unrelated_filter": "keep",
    }
    monkeypatch.setattr(page_header, "st", SimpleNamespace(
        session_state=session_state, toast=lambda *a, **k: None,
    ))
    monkeypatch.setattr(page_header, "bump_indicator_product_revision", lambda i, p: bumped.append((i, p)))
    monkeypatch.setattr(page_header.SessionManager, "load_and_set_config", configs.append)
    import app.utils.reloader as reloader
    monkeypatch.setattr(reloader, "deep_reload_modules", lambda: reloads.append(True))
    class Cached:
        def clear(self):
            raise AssertionError("global clear")
    page_header.perform_hard_reset([Cached()], "M626", ("yield_lot_oos",))
    assert bumped == [("yield_lot_oos", "M626")]
    assert configs == ["M626"]
    assert reloads == [True]
    assert session_state == {"unrelated_filter": "keep"}


def test_every_single_product_header_declares_indicator_scope():
    import ast
    pages = Path("app/pages")
    found = []
    for path in pages.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for call in ast.walk(tree):
            if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name) or call.func.id != "render_page_header":
                continue
            kwargs = {kw.arg: kw.value for kw in call.keywords}
            if isinstance(kwargs.get("show_product_filter"), ast.Constant) and kwargs["show_product_filter"].value is False:
                continue
            assert "product_cache_scope" in kwargs, path.name
            assert "product_cache_indicators" in kwargs, path.name
            source = path.read_text(encoding="utf-8-sig")
            assert "build_product_cache_signature" not in source, path.name
            assert "get_product_cache_revision" not in source, path.name
            found.append(path.name)
    assert len(found) == 7
