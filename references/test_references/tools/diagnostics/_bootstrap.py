"""让独立诊断脚本在任意工作目录均可导入项目模块。"""

from __future__ import annotations

import sys
from pathlib import Path


def configure_project_imports(script_file: str | Path) -> Path:
    """返回仓库根目录，并注册仓库根目录和 ``src`` 目录。"""
    script_path = Path(script_file).resolve()
    for candidate in (script_path.parent, *script_path.parents):
        if (candidate / "pyproject.toml").is_file() and (candidate / "src").is_dir():
            for import_path in (candidate, candidate / "src"):
                import_path_text = str(import_path)
                if import_path_text not in sys.path:
                    sys.path.insert(0, import_path_text)
            return candidate

    raise RuntimeError(f"无法从诊断脚本定位项目根目录：{script_path}")
