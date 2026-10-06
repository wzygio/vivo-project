"""Product selector labels; widget values remain the original product codes."""

from collections.abc import Callable

from src.shared_kernel.config import ConfigLoader


def get_product_label_formatter() -> Callable[[str], str]:
    """Load annotations once per widget render, without caching display config."""
    annotations = ConfigLoader.get_product_annotations()

    def format_label(product_code: str) -> str:
        annotation = annotations.get(product_code)
        return f"{product_code}（{annotation}）" if annotation else product_code

    return format_label
