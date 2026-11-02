"""Ownership audit for maintained inputs, collections and known companions."""
from pathlib import Path
import re


def unregistered_resources(root: Path, files: set[Path], collections: set[Path]) -> set[Path]:
    registered = {path.resolve() for path in files}
    directories = {path.resolve() for path in collections}
    unknown = set()
    for entry in (root / 'resources').rglob('*'):
        if not entry.is_file():
            continue
        path = entry.resolve()
        if path in registered or any(path.is_relative_to(directory) for directory in directories):
            continue
        if path.name.startswith('~$'):
            continue  # Office ownership marker, never a maintained input.
        if re.fullmatch(r'[0-9A-Fa-f]{8}', path.name) and path.stat().st_size == 0:
            continue  # Empty transient placeholders; nonempty files still need ownership.
        if any(path.parent == main.parent and (
            path.name == main.name + '.bak'
            or path.name.startswith(main.name + '.before-') and path.name.endswith('.bak')
            or path in {main.with_suffix('.sig.json'), main.with_name(main.name + '.sig.json'),
                        main.with_name(main.name + '.policy')}
        ) for main in registered):
            continue
        unknown.add(path)
    return unknown
