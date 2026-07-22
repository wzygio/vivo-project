from pathlib import Path

from src.inline_domain.application.spc.indicator_improvement_cli import build_parser
from src.shared_kernel.output_paths import OutputLayout


def test_output_layout_exposes_canonical_artifact_directories(tmp_path: Path) -> None:
    layout = OutputLayout.from_project_root(tmp_path)

    assert layout.root == tmp_path / "output"
    assert layout.reports == layout.root / "reports"
    assert layout.downloads == layout.root / "downloads"
    assert layout.decrypted_files == layout.root / "decrypted_files"
    assert layout.rpa_downloads == layout.root / "rpa_downloads"
    assert layout.screenshots == layout.root / "screenshots"
    assert layout.test_results == layout.root / "test-results"
    assert layout.logs == layout.root / "logs"
    assert layout.tmp == layout.root / "tmp"


def test_output_layout_creates_every_canonical_directory(tmp_path: Path) -> None:
    layout = OutputLayout.from_project_root(tmp_path).ensure()

    assert all(path.is_dir() for path in layout.directories())


def test_indicator_improvement_cli_defaults_to_reports_directory() -> None:
    args = build_parser().parse_args([])

    assert args.output_dir == "output/reports/indicator-improvement"
    assert args.decrypted_dir == "output/decrypted_files/indicator-improvement"
