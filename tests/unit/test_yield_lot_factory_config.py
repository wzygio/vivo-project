"""Global YAML owns the Lot group/factory policy; no baked-in fallback."""
from pathlib import Path

import pytest
import yaml

from src.shared_kernel.config import ConfigLoader


def write_config(tmp_path: Path, monkeypatch, mapping) -> None:
    config_dir = tmp_path / 'config'
    config_dir.mkdir(exist_ok=True)
    (config_dir / 'global.yaml').write_text(yaml.safe_dump({
        'data_source': {'defect_group_factory_mapping': mapping},
    }), encoding='utf-8')
    monkeypatch.setattr(ConfigLoader, 'get_project_root', staticmethod(lambda: tmp_path))


def test_lot_factory_mapping_uses_current_global_configuration(tmp_path, monkeypatch):
    write_config(tmp_path, monkeypatch, {'Array_Line': 'ARRAY'})
    assert ConfigLoader.get_yield_lot_factory_mapping() == {'Array_Line': 'ARRAY'}
    write_config(tmp_path, monkeypatch, {'Array_Line': 'OLED'})
    assert ConfigLoader.get_yield_lot_factory_mapping() == {'Array_Line': 'OLED'}


@pytest.mark.parametrize('mapping', [None, {}, [], {'Array_Line': ''}, {'Array_Line': 1}, {'': 'ARRAY'}])
def test_invalid_mapping_is_not_replaced_by_a_hardcoded_default(tmp_path, monkeypatch, mapping):
    write_config(tmp_path, monkeypatch, mapping)
    with pytest.raises(ValueError):
        ConfigLoader.get_yield_lot_factory_mapping()
