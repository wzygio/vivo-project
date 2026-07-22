# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.inline_domain.application.spc.indicator_improvement_service import IndicatorImprovementService
from src.shared_kernel.output_paths import OutputLayout


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run PNL indicator improvement analysis.")
    parser.add_argument(
        "--source-dir",
        default="resources/project_files",
        help="Directory containing PNL indicator workbook versions.",
    )
    parser.add_argument(
        "--output-dir",
        default="output/reports/indicator-improvement",
        help="Directory for all generated tables, config, and chart images.",
    )
    parser.add_argument(
        "--decrypted-dir",
        default="output/decrypted_files/indicator-improvement",
        help="Directory for normalized source workbooks.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    project_root = _find_project_root(Path.cwd())
    OutputLayout.from_project_root(project_root).ensure()
    source_dir = (project_root / args.source_dir).resolve()
    output_dir = (project_root / args.output_dir).resolve()
    decrypted_dir = (project_root / args.decrypted_dir).resolve()

    service = IndicatorImprovementService(
        source_dir=source_dir,
        output_dir=output_dir,
        normalized_workbook_dir=decrypted_dir,
    )
    result = service.run()
    payload = {
        "output_dir": str(result.output_dir),
        "workbook_path": str(result.workbook_path),
        "task1_image_path": str(result.task1_image_path),
        "task2_image_path": str(result.task2_image_path),
        "task1_summary": result.task1_summary.to_dict(orient="records"),
        "task2_summary": result.task2_summary.to_dict(orient="records"),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


def _find_project_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "pyproject.toml").exists() and (candidate / "src").exists():
            return candidate
    return current


if __name__ == "__main__":
    raise SystemExit(main())
