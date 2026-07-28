from pathlib import Path

from app.components import page_header


class _CachedFunctionStub:
    def __init__(self) -> None:
        self.clear_count = 0

    def clear(self) -> None:
        self.clear_count += 1


def test_product_cache_revision_changes_only_the_selected_product(tmp_path: Path) -> None:
    m626_before = page_header.build_product_cache_signature(
        "report-v1",
        "M626",
        revision_dir=tmp_path,
    )
    m673_before = page_header.build_product_cache_signature(
        "report-v1",
        "M673",
        revision_dir=tmp_path,
    )

    page_header.bump_product_cache_revision("M626", revision_dir=tmp_path)

    m626_after = page_header.build_product_cache_signature(
        "report-v1",
        "M626",
        revision_dir=tmp_path,
    )
    m673_after = page_header.build_product_cache_signature(
        "report-v1",
        "M673",
        revision_dir=tmp_path,
    )

    assert m626_after != m626_before
    assert m673_after == m673_before


def test_product_scoped_invalidation_does_not_clear_whole_function_cache(
    monkeypatch,
) -> None:
    cached_function = _CachedFunctionStub()
    bumped_products: list[str] = []
    monkeypatch.setattr(
        page_header,
        "bump_product_cache_revision",
        lambda product_code: bumped_products.append(product_code),
    )

    scope = page_header.invalidate_page_cache(
        cached_funcs=[cached_function],
        product_code="M673",
    )

    assert scope == "product"
    assert bumped_products == ["M673"]
    assert cached_function.clear_count == 0


def test_unscoped_invalidation_preserves_legacy_function_clear_behavior() -> None:
    cached_function = _CachedFunctionStub()

    scope = page_header.invalidate_page_cache(cached_funcs=[cached_function])

    assert scope == "global"
    assert cached_function.clear_count == 1
