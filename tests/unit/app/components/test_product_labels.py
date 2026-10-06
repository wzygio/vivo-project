"""Product annotations affect widget labels without changing selected codes."""

from pathlib import Path

import pytest
import yaml
from streamlit.testing.v1 import AppTest

from src.shared_kernel.config import ConfigLoader


def _write_registry(root: Path, registry: dict) -> None:
    config_dir = root / "config"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "global.yaml").write_text(
        yaml.safe_dump({"product_registry": registry}), encoding="utf-8",
    )


def test_annotations_do_not_change_enabled_products(monkeypatch, tmp_path):
    from app.components.product_labels import get_product_label_formatter

    _write_registry(tmp_path, {
        "enabled_products": ["M678", "M626", "Z553"],
        "product_annotations": {
            "M678": "CPD2455", "M626": "CPD2515", "Z611": "CPD2611",
            "Z553": "  ", "Z576": None,
        },
    })
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)

    formatter = get_product_label_formatter()

    assert ConfigLoader.get_enabled_products() == ["M678", "M626", "Z553"]
    assert formatter("M678") == "M678（CPD2455）"
    assert formatter("M626") == "M626（CPD2515）"
    assert formatter("Z553") == "Z553"
    assert formatter("Z576") == "Z576"
    assert formatter("UNKNOWN") == "UNKNOWN"


@pytest.mark.parametrize("registry", [{}, {"product_annotations": {}},
                                      {"product_annotations": None}])
def test_missing_annotations_keep_original_label(monkeypatch, tmp_path, registry):
    from app.components.product_labels import get_product_label_formatter

    _write_registry(tmp_path, registry)
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)

    assert get_product_label_formatter()("M678") == "M678"


def test_widgets_keep_raw_codes_across_annotation_changes(monkeypatch, tmp_path):
    registry = {
        "enabled_products": ["M678", "M626", "Z553"],
        "product_annotations": {"M678": "CPD2455", "M626": "CPD2515"},
    }
    _write_registry(tmp_path, registry)
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    app = AppTest.from_string('''
import streamlit as st
from app.components.product_labels import get_product_label_formatter
from src.shared_kernel.config import ConfigLoader
options = ConfigLoader.get_enabled_products()
formatter = get_product_label_formatter()
st.selectbox('产品型号', options, format_func=formatter, key='single')
st.multiselect('产品型号', options, format_func=formatter, key='multiple')
''').run()

    assert not app.exception
    labels = ["M678（CPD2455）", "M626（CPD2515）", "Z553"]
    assert app.selectbox[0].options == labels
    assert app.multiselect[0].options == labels
    app.selectbox[0].set_value("M626")
    app.multiselect[0].set_value(["M678", "Z553"]).run()
    assert not app.exception
    assert app.session_state["single"] == "M626"
    assert app.session_state["multiple"] == ["M678", "Z553"]

    registry["product_annotations"] = {"M626": "NEW"}
    _write_registry(tmp_path, registry)
    app.run()
    assert not app.exception
    assert app.selectbox[0].options == ["M678", "M626（NEW）", "Z553"]
    assert app.session_state["single"] == "M626"
    assert app.session_state["multiple"] == ["M678", "Z553"]
